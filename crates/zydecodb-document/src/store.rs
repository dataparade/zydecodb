//! Document write path and index maintenance.
//!
//! Every document write is one atomic [`Engine::write_batch`]: the body op plus
//! all index put/delete ops succeed or fail together (one WAL record, one CRC),
//! so a crash can never leave indexes disagreeing with the body.

use crate::catalog::{Catalog, CollectionMeta, IndexMeta};
use crate::error::{DocError, DocResult};
use crate::{encoding, keys};
use serde_json::Value;
use std::collections::BTreeSet;
use zydecodb_engine::engine::{BatchOp, Engine};
use zydecodb_engine::keys::MAX_BATCH_KEYS;

/// `value_kind` for a raw/JSON body (first byte of the stored value).
pub const VK_RAW: u8 = 0x00;
pub const VK_ZDOC: u8 = 0x01;

/// Build the set of index keys this document occupies across all of the
/// collection's indexes.
fn index_keys_for(
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    doc: &Value,
) -> Vec<Vec<u8>> {
    coll.indexes
        .iter()
        .map(|idx| {
            let vals: Vec<Value> = idx
                .fields
                .iter()
                .map(|f| encoding::extract_path(doc, f))
                .collect();
            let enc = encoding::encode_fields_with_directions(&vals, &idx.ascending());
            keys::index_key(prefix, coll.id, idx.id, &enc, doc_id)
        })
        .collect()
}

/// Like [`index_keys_for`] but walks a ZDoc [`ValueView`] — no full JSON tree.
fn index_keys_for_view(
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    view: &crate::binary::ValueView<'_>,
) -> Vec<Vec<u8>> {
    coll.indexes
        .iter()
        .map(|idx| {
            let enc = encoding::encode_fields_from_view_with_directions(
                view,
                &idx.fields,
                &idx.ascending(),
            );
            keys::index_key(prefix, coll.id, idx.id, &enc, doc_id)
        })
        .collect()
}

/// Drop the leading `value_kind` byte, yielding the raw body payload.
pub fn strip_value_kind(stored: &[u8]) -> &[u8] {
    stored.get(1..).unwrap_or(&[])
}

pub fn stored_to_json_vec(stored: &[u8]) -> Vec<u8> {
    if stored.is_empty() {
        return Vec::new();
    }
    let kind = stored[0];
    let payload = strip_value_kind(stored);
    if kind == VK_ZDOC {
        let Ok(val) = crate::binary::ValueView::new(payload).to_value() else {
            return Vec::new();
        };
        serde_json::to_vec(&val).unwrap_or_default()
    } else {
        payload.to_vec()
    }
}

/// Current opaque revision (`InternalKey.seq`) for a document, if it exists.
pub fn doc_revision(
    engine: &Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
) -> DocResult<Option<u64>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let dk = keys::doc_key(prefix, coll.id, doc_id);
    Ok(engine.get_with_seq(&dk)?.map(|(_, rev)| rev))
}

/// Compare-and-swap gate: require `expected` to equal the current document
/// revision. Missing/tombstoned/expired documents and mismatched revisions all
/// return [`DocError::StaleRevision`]. Must be called under the engine write lock.
pub fn check_if_match(
    engine: &Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    expected: u64,
) -> DocResult<()> {
    match doc_revision(engine, catalog, prefix, collection, doc_id)? {
        Some(rev) if rev == expected => Ok(()),
        _ => Err(DocError::StaleRevision),
    }
}

/// Reject a write that would place two different documents at the same value of
/// a unique index. The server holds the engine mutex across the whole write, so
/// this check-then-write is race-free against other writers (no TOCTOU).
fn enforce_unique_enc(
    engine: &mut Engine,
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    encoded_fields: impl Fn(&IndexMeta) -> Vec<u8>,
) -> DocResult<()> {
    if !coll.indexes.iter().any(|i| i.unique) {
        return Ok(());
    }
    let snap = engine.snapshot_owned();
    for idx in coll.indexes.iter().filter(|i| i.unique) {
        let enc = encoded_fields(idx);
        // Range covering every index entry whose encoded fields equal `enc`. The
        // order-preserving field encoding is prefix-free, so only `doc_id`
        // suffixes follow `enc` inside this range.
        let mut lo = keys::index_prefix(prefix, coll.id, idx.id);
        lo.extend_from_slice(&enc);
        let hi = keys::prefix_upper_bound(&lo);
        let rows = snap.scan(lo, hi)?;
        for item in rows {
            let (_key, existing_doc_id) = item?;
            if existing_doc_id.as_slice() != doc_id {
                return Err(DocError::DuplicateKey(format!(
                    "unique index '{}' on {:?}",
                    idx.name, idx.fields
                )));
            }
        }
    }
    Ok(())
}

fn enforce_unique(
    engine: &mut Engine,
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    new_doc: &Value,
) -> DocResult<()> {
    enforce_unique_enc(engine, coll, prefix, doc_id, |idx| {
        let vals: Vec<Value> = idx
            .fields
            .iter()
            .map(|f| encoding::extract_path(new_doc, f))
            .collect();
        encoding::encode_fields_with_directions(&vals, &idx.ascending())
    })
}

fn enforce_unique_view(
    engine: &mut Engine,
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    view: &crate::binary::ValueView<'_>,
) -> DocResult<()> {
    enforce_unique_enc(engine, coll, prefix, doc_id, |idx| {
        encoding::encode_fields_from_view_with_directions(view, &idx.fields, &idx.ascending())
    })
}

/// Build (but do not write) the batch that upserts `json` for `doc_id`: the body
/// put plus the index-key diff against the prior version. Enforces unique-index
/// constraints against the committed state. Returned ops never exceed
/// `MAX_BATCH_KEYS`.
///
/// `old_doc`, when `Some`, is the already-decoded prior body — callers that have
/// just read it (e.g. Update) pass it so this path skips a second LSM get +
/// decode. `None` means look up the prior body from the engine (DocPut /
/// replace / insert).
pub fn upsert_ops(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
) -> DocResult<Vec<BatchOp>> {
    upsert_ops_with_old(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, expires_at, None,
    )
}

/// Like [`upsert_ops`], with an optional pre-loaded prior document.
pub fn upsert_ops_with_old(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
    old_doc: Option<&Value>,
) -> DocResult<Vec<BatchOp>> {
    upsert_ops_with_old_inner(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, expires_at, old_doc, true,
    )
}

/// Like [`upsert_ops_with_old`], but skips unique-index enforcement.
///
/// Used by bounded transactions after they have already validated uniqueness
/// across the whole staged set (including ownership transfers).
pub fn upsert_ops_without_unique(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
    old_doc: Option<&Value>,
) -> DocResult<Vec<BatchOp>> {
    upsert_ops_with_old_inner(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, expires_at, old_doc, false,
    )
}

/// Like [`upsert_ops`], with an optional pre-loaded prior document.
#[allow(clippy::too_many_arguments)]
fn upsert_ops_with_old_inner(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
    old_doc: Option<&Value>,
    check_unique: bool,
) -> DocResult<Vec<BatchOp>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;

    let doc_key = keys::doc_key(prefix, coll.id, doc_id);

    // ZDoc path: index/TTL/unique use ValueView path extraction — never build a
    // full serde_json tree for the new body under the engine lock.
    let (expires_at, old_keys, new_keys) = if is_zdoc {
        let view = crate::binary::ValueView::new(payload);
        if check_unique {
            enforce_unique_view(engine, coll, prefix, doc_id, &view)?;
        }
        let expires_at = if let Some(ttl) = coll.ttl_index() {
            derive_ttl_expires_at_view(&view, ttl)
        } else {
            expires_at
        };
        let old_keys: BTreeSet<Vec<u8>> = if let Some(old) = old_doc {
            index_keys_for(coll, prefix, doc_id, old)
                .into_iter()
                .collect()
        } else {
            old_index_keys(engine, coll, prefix, doc_id, &doc_key)?
        };
        let new_keys: BTreeSet<Vec<u8>> = index_keys_for_view(coll, prefix, doc_id, &view)
            .into_iter()
            .collect();
        (expires_at, old_keys, new_keys)
    } else {
        let new_doc: Value =
            serde_json::from_slice(payload).map_err(|e| DocError::InvalidJson(e.to_string()))?;
        if check_unique {
            enforce_unique(engine, coll, prefix, doc_id, &new_doc)?;
        }
        let expires_at = if let Some(ttl) = coll.ttl_index() {
            derive_ttl_expires_at(&new_doc, ttl)
        } else {
            expires_at
        };
        let old_keys: BTreeSet<Vec<u8>> = if let Some(old) = old_doc {
            index_keys_for(coll, prefix, doc_id, old)
                .into_iter()
                .collect()
        } else {
            old_index_keys(engine, coll, prefix, doc_id, &doc_key)?
        };
        let new_keys: BTreeSet<Vec<u8>> = index_keys_for(coll, prefix, doc_id, &new_doc)
            .into_iter()
            .collect();
        (expires_at, old_keys, new_keys)
    };

    let mut ops: Vec<BatchOp> = Vec::with_capacity(1 + old_keys.len() + new_keys.len());
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(if is_zdoc { VK_ZDOC } else { VK_RAW });
    value.extend_from_slice(payload);
    ops.push(BatchOp::Put {
        key: doc_key,
        value,
        expires_at,
    });
    // Stale entries (old - new) are removed; fresh entries (new - old) are
    // added. Unchanged keys (intersection) are rewritten so index `expires_at`
    // stays aligned with the body when only expiry (or TTL derivation) changes.
    // Doc keys ('d') never collide with index keys ('i').
    for k in old_keys.difference(&new_keys) {
        ops.push(BatchOp::Del { key: k.clone() });
    }
    for k in new_keys.iter() {
        ops.push(BatchOp::Put {
            key: k.clone(),
            value: doc_id.to_vec(),
            // Share the body's expiry so index keys do not outlive the document.
            expires_at,
        });
    }

    if ops.len() > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(ops.len()));
    }
    Ok(ops)
}

/// Prior index footprint for `doc_id`. ZDoc bodies use view extraction (no
/// full-tree materialization); JSON bodies decode as usual.
fn old_index_keys(
    engine: &mut Engine,
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    doc_key: &[u8],
) -> DocResult<BTreeSet<Vec<u8>>> {
    Ok(match engine.get(doc_key)? {
        Some(stored) if !stored.is_empty() => {
            let old_kind = stored[0];
            let old_payload = &stored[1..];
            if old_kind == VK_ZDOC {
                let view = crate::binary::ValueView::new(old_payload);
                index_keys_for_view(coll, prefix, doc_id, &view)
                    .into_iter()
                    .collect()
            } else {
                match serde_json::from_slice::<Value>(old_payload) {
                    Ok(old) => index_keys_for(coll, prefix, doc_id, &old)
                        .into_iter()
                        .collect(),
                    Err(_) => BTreeSet::new(),
                }
            }
        }
        _ => BTreeSet::new(),
    })
}

/// Insert or replace a document (no TTL). See [`upsert_with_expiry`].
pub fn upsert(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
) -> DocResult<u64> {
    upsert_with_expiry(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, 0,
    )
}

/// Insert or replace a document with an optional absolute `expires_at` (unix
/// millis; `0` = never). Diffs index entries against the prior version.
pub fn upsert_with_expiry(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
) -> DocResult<u64> {
    let ops = upsert_ops(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, expires_at,
    )?;
    Ok(engine.write_batch(ops)?)
}

/// Build (but do not write) the batch that deletes `doc_id` and all of its index
/// entries. Empty if the document does not exist — or, when `filter` is given,
/// if the CURRENT body no longer matches it (see [`crate::update::apply_to_ids`]
/// for why the re-check under the lock is required).
pub fn delete_ops(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    filter: Option<&crate::filter::Filter>,
) -> DocResult<Vec<BatchOp>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let doc_key = keys::doc_key(prefix, coll.id, doc_id);
    let stored = match engine.get(&doc_key)? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    if let Some(f) = filter {
        if !crate::query::check_filter(&stored, f, doc_id) {
            return Ok(Vec::new());
        }
    }

    let mut ops: Vec<BatchOp> = vec![BatchOp::Del { key: doc_key }];
    let payload = strip_value_kind(&stored);
    if stored[0] == VK_ZDOC {
        let view = crate::binary::ValueView::new(payload);
        for k in index_keys_for_view(coll, prefix, doc_id, &view) {
            ops.push(BatchOp::Del { key: k });
        }
    } else if let Ok(old) = serde_json::from_slice::<Value>(payload) {
        for k in index_keys_for(coll, prefix, doc_id, &old) {
            ops.push(BatchOp::Del { key: k });
        }
    }
    if ops.len() > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(ops.len()));
    }
    Ok(ops)
}

/// Delete a document and all of its index entries atomically. Returns whether
/// the document existed.
pub fn delete(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
) -> DocResult<bool> {
    let ops = delete_ops(engine, catalog, prefix, collection, doc_id, None)?;
    if ops.is_empty() {
        return Ok(false);
    }
    engine.write_batch(ops)?;
    Ok(true)
}

/// Delete many documents. When the combined op count fits in one atomic
/// `write_batch` the whole set is removed atomically (and isolated from
/// concurrent readers); otherwise it falls back to one batch per document.
/// Returns the number of documents that existed and were removed.
///
/// `filter`, when given, is re-verified per document under the engine lock so
/// filtered deletes are per-document compare-and-swap (candidates stale since
/// snapshot selection are skipped and not counted).
pub fn delete_ids(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    ids: &[Vec<u8>],
    filter: Option<&crate::filter::Filter>,
) -> DocResult<u64> {
    let mut per_doc: Vec<Vec<BatchOp>> = Vec::with_capacity(ids.len());
    let mut deleted: u64 = 0;
    for id in ids {
        let ops = delete_ops(engine, catalog, prefix, collection, id, filter)?;
        if !ops.is_empty() {
            deleted += 1;
            per_doc.push(ops);
        }
    }
    commit_batches(engine, per_doc)?;
    Ok(deleted)
}

/// Submit pre-built per-document op groups: one atomic `write_batch` when the
/// total fits, otherwise one batch per group. Deletes can never violate a
/// unique constraint, and combined keys never collide (distinct doc ids), so the
/// merged batch is always safe.
pub(crate) fn commit_batches(engine: &mut Engine, per_doc: Vec<Vec<BatchOp>>) -> DocResult<()> {
    let total: usize = per_doc.iter().map(|o| o.len()).sum();
    if total == 0 {
        return Ok(());
    }

    let mut all = Vec::with_capacity(std::cmp::min(total, MAX_BATCH_KEYS));
    for mut ops in per_doc {
        // If adding this doc's ops would exceed the chunk limit, flush what we have
        if all.len() + ops.len() > MAX_BATCH_KEYS && !all.is_empty() {
            engine.write_batch(std::mem::take(&mut all))?;
        }

        // A single doc's ops should never exceed MAX_BATCH_KEYS in practice (unless
        // there are hundreds of indexes), but if it somehow does, we write it alone
        // and it'll get caught by the engine's internal check if it's strictly > MAX_BATCH_KEYS.
        if ops.len() > MAX_BATCH_KEYS {
            engine.write_batch(ops)?;
            continue;
        }

        all.append(&mut ops);
    }

    if !all.is_empty() {
        engine.write_batch(all)?;
    }
    Ok(())
}

/// Encoded unique-index field values claimed by `doc` (one entry per unique index).
/// Returns `(index_name, collection_id, index_id, encoded_fields)`.
#[allow(clippy::type_complexity)]
pub fn unique_encodings_for_doc(
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc: &Value,
) -> DocResult<Vec<(String, u32, u32, Vec<u8>)>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let mut out = Vec::new();
    for idx in coll.indexes.iter().filter(|i| i.unique) {
        let vals: Vec<Value> = idx
            .fields
            .iter()
            .map(|f| encoding::extract_path(doc, f))
            .collect();
        let enc = encoding::encode_fields_with_directions(&vals, &idx.ascending());
        out.push((idx.name.clone(), coll.id, idx.id, enc));
    }
    Ok(out)
}

/// Scan a unique index for the current committed owner of `encoded_fields`.
/// Returns `Some(doc_id)` when an entry exists.
pub fn unique_owner(
    engine: &Engine,
    prefix: &[u8],
    collection_id: u32,
    index_id: u32,
    encoded_fields: &[u8],
) -> DocResult<Option<Vec<u8>>> {
    let snap = engine.snapshot_owned();
    let mut lo = keys::index_prefix(prefix, collection_id, index_id);
    lo.extend_from_slice(encoded_fields);
    let hi = keys::prefix_upper_bound(&lo);
    let mut rows = snap.scan(lo, hi)?;
    if let Some(item) = rows.next() {
        let (_key, existing_doc_id) = item?;
        return Ok(Some(existing_doc_id));
    }
    Ok(None)
}

/// Decode the current committed JSON body for a document, if present.
pub fn current_json_body(
    engine: &Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
) -> DocResult<Option<Value>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let dk = keys::doc_key(prefix, coll.id, doc_id);
    match engine.get(&dk)? {
        Some(stored) if !stored.is_empty() => {
            let payload = strip_value_kind(&stored);
            if stored[0] == VK_ZDOC {
                Ok(Some(crate::binary::ValueView::new(payload).to_value()?))
            } else {
                Ok(Some(
                    serde_json::from_slice(payload)
                        .map_err(|e| DocError::InvalidJson(e.to_string()))?,
                ))
            }
        }
        _ => Ok(None),
    }
}

/// Define an index on a collection and backfill it over existing documents.
///
/// Ordering is deliberate for crash safety: the index entries are written
/// FIRST (in chunked batches), and the catalog is committed LAST. A crash
/// before the catalog commit leaves orphan index keys that no committed catalog
/// references, so they are invisible and harmless; the DDL is simply retried.
/// Queries only ever use indexes present in the committed catalog.
///
/// `catalog` is the live shared catalog; on success it is replaced with the new
/// version. The caller must hold the catalog write lock and engine lock.
pub fn define_index(
    engine: &mut Engine,
    catalog: &mut Catalog,
    prefix: &[u8],
    collection: &str,
    index_name: &str,
    fields: Vec<String>,
    unique: bool,
    expire_after_seconds: Option<u64>,
) -> DocResult<()> {
    define_index_directed(
        engine,
        catalog,
        prefix,
        collection,
        index_name,
        fields,
        Vec::new(),
        unique,
        expire_after_seconds,
    )
}

/// Like [`define_index`] with per-field ascending flags (`directions` empty = all ASC).
pub fn define_index_directed(
    engine: &mut Engine,
    catalog: &mut Catalog,
    prefix: &[u8],
    collection: &str,
    index_name: &str,
    fields: Vec<String>,
    directions: Vec<bool>,
    unique: bool,
    expire_after_seconds: Option<u64>,
) -> DocResult<()> {
    // Work on a copy so the live catalog is mutated only after the backfill and
    // persist both succeed.
    let mut working = catalog.clone();
    let meta = working.add_index_directed(
        prefix,
        collection,
        index_name,
        fields,
        directions,
        unique,
        expire_after_seconds,
    )?;
    let collection_id = working
        .collection(prefix, collection)
        .expect("collection ensured by add_index")
        .id;

    backfill_index(engine, prefix, collection_id, &meta)?;
    working.persist(engine)?;
    *catalog = working;
    Ok(())
}

/// Derive absolute `expires_at` (unix millis) from a TTL index + document.
/// Missing/non-numeric field → `0` (never expires until the field is present).
pub fn derive_ttl_expires_at(doc: &Value, idx: &crate::catalog::IndexMeta) -> u64 {
    let Some(secs) = idx.expire_after_seconds else {
        return 0;
    };
    let Some(field) = idx.fields.first() else {
        return 0;
    };
    let v = encoding::extract_path(doc, field);
    let field_ms = match v {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        _ => None,
    };
    match field_ms {
        Some(ms) => ms.saturating_add(secs.saturating_mul(1000)),
        None => 0,
    }
}

/// Like [`derive_ttl_expires_at`] for a ZDoc [`ValueView`].
pub fn derive_ttl_expires_at_view(
    view: &crate::binary::ValueView<'_>,
    idx: &crate::catalog::IndexMeta,
) -> u64 {
    let Some(secs) = idx.expire_after_seconds else {
        return 0;
    };
    let Some(field) = idx.fields.first() else {
        return 0;
    };
    let Some(v) = view.get_path(field) else {
        return 0;
    };
    let field_ms = v
        .as_f64()
        .map(|f| f as u64)
        .or_else(|| v.as_i64().map(|i| i as u64));
    match field_ms {
        Some(ms) => ms.saturating_add(secs.saturating_mul(1000)),
        None => 0,
    }
}

/// Scan every existing document in a collection and write the new index's
/// entries in chunks that respect `MAX_BATCH_KEYS`.
fn backfill_index(
    engine: &mut Engine,
    prefix: &[u8],
    collection_id: u32,
    idx: &IndexMeta,
) -> DocResult<()> {
    let dprefix = keys::doc_prefix(prefix, collection_id);
    let dhi = keys::prefix_upper_bound(&dprefix);
    let prefix_len = prefix.len();

    // An owned snapshot does not borrow the engine, so we can write index
    // entries back through `&mut engine` while iterating the doc range. The
    // snapshot's fixed seq ceiling means our own writes are never re-scanned.
    let snap = engine.snapshot_owned();
    let mut pending: Vec<BatchOp> = Vec::new();
    let mut rows = snap.scan(dprefix.clone(), dhi)?;
    for item in rows.by_ref() {
        let (doc_key, stored) = item?;
        let doc_id = keys::doc_id_from_doc_key(prefix_len, &doc_key);
        let dirs = idx.ascending();
        let (enc, expires_at) = if stored[0] == VK_ZDOC {
            let view = crate::binary::ValueView::new(strip_value_kind(&stored));
            (
                encoding::encode_fields_from_view_with_directions(&view, &idx.fields, &dirs),
                derive_ttl_expires_at_view(&view, idx),
            )
        } else {
            let doc: Value = match serde_json::from_slice(strip_value_kind(&stored)) {
                Ok(d) => d,
                Err(_) => continue, // skip unparseable bodies
            };
            let vals: Vec<Value> = idx
                .fields
                .iter()
                .map(|f| encoding::extract_path(&doc, f))
                .collect();
            (
                encoding::encode_fields_with_directions(&vals, &dirs),
                derive_ttl_expires_at(&doc, idx),
            )
        };
        let ikey = keys::index_key(prefix, collection_id, idx.id, &enc, &doc_id);
        pending.push(BatchOp::Put {
            key: ikey,
            value: doc_id.clone(),
            expires_at,
        });
        // When creating a TTL index, stamp body expiry so existing docs become
        // invisible under lazy expiry without waiting for a later rewrite.
        if idx.expire_after_seconds.is_some() && expires_at != 0 {
            pending.push(BatchOp::Put {
                key: doc_key.clone(),
                value: stored.clone(),
                expires_at,
            });
        }
        if pending.len() >= MAX_BATCH_KEYS {
            let chunk = std::mem::take(&mut pending);
            engine.write_batch(chunk)?;
        }
    }
    drop(rows);
    drop(snap);
    if !pending.is_empty() {
        engine.write_batch(pending)?;
    }
    Ok(())
}
