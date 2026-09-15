//! Document write path and index maintenance.
//!
//! Every document write is one atomic [`Engine::write_batch`]: the body op plus
//! all index put/delete ops succeed or fail together (one WAL record, one CRC),
//! so a crash can never leave indexes disagreeing with the body.

use crate::catalog::{Catalog, CollectionMeta, CounterDelta, IndexMeta};
use crate::error::{DocError, DocResult};
use crate::{encoding, keys};
use serde_json::Value;
use std::collections::BTreeSet;
use zydecodb_engine::engine::{BatchOp, Engine};
use zydecodb_engine::keys::MAX_BATCH_KEYS;

/// `value_kind` for a raw/JSON body (first byte of the stored value).
pub const VK_RAW: u8 = 0x00;
pub const VK_ZDOC: u8 = 0x01;

/// Ops for one document write plus the catalog counter delta it implies.
/// `doc_delta` is +1 for an insert, 0 for a replace, -1 for a delete; every
/// index entry count moves by the same delta (one entry per doc per index).
#[derive(Debug)]
pub struct WriteOps {
    pub ops: Vec<BatchOp>,
    pub collection_id: u32,
    pub doc_delta: i64,
}

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

/// The `value_kind` byte of a stored body. An empty body cannot have been
/// written by this layer (every write pushes the kind byte first), so it is
/// reported as corruption rather than indexed.
pub fn value_kind(stored: &[u8]) -> DocResult<u8> {
    stored
        .first()
        .copied()
        .ok_or_else(|| DocError::Corrupt("empty stored document body".into()))
}

/// Decode a stored body to JSON bytes. A body that cannot be decoded (empty,
/// or a ZDoc payload whose structure does not fit its bytes) is reported as
/// [`DocError::Corrupt`] instead of being handed back as an empty document.
pub fn stored_to_json_vec(stored: &[u8]) -> DocResult<Vec<u8>> {
    let kind = value_kind(stored)?;
    let payload = strip_value_kind(stored);
    if kind == VK_ZDOC {
        let val = crate::binary::ValueView::new(payload).to_value()?;
        serde_json::to_vec(&val).map_err(|e| DocError::Corrupt(e.to_string()))
    } else {
        Ok(payload.to_vec())
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
) -> DocResult<WriteOps> {
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
) -> DocResult<WriteOps> {
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
) -> DocResult<WriteOps> {
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
) -> DocResult<WriteOps> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;

    let doc_key = keys::doc_key(prefix, coll.id, doc_id);

    // ZDoc path: index/TTL/unique use ValueView path extraction — never build a
    // full serde_json tree for the new body under the engine lock.
    let (expires_at, old_keys, new_keys, existed) = if is_zdoc {
        let view = crate::binary::ValueView::new(payload);
        if check_unique {
            enforce_unique_view(engine, coll, prefix, doc_id, &view)?;
        }
        let expires_at = if let Some(ttl) = coll.ttl_index() {
            derive_ttl_expires_at_view(&view, ttl)
        } else {
            expires_at
        };
        let (old_keys, existed) = if let Some(old) = old_doc {
            (
                index_keys_for(coll, prefix, doc_id, old)
                    .into_iter()
                    .collect::<BTreeSet<Vec<u8>>>(),
                true,
            )
        } else {
            old_index_keys(engine, coll, prefix, doc_id, &doc_key)?
        };
        let new_keys: BTreeSet<Vec<u8>> = index_keys_for_view(coll, prefix, doc_id, &view)
            .into_iter()
            .collect();
        (expires_at, old_keys, new_keys, existed)
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
        let (old_keys, existed) = if let Some(old) = old_doc {
            (
                index_keys_for(coll, prefix, doc_id, old)
                    .into_iter()
                    .collect::<BTreeSet<Vec<u8>>>(),
                true,
            )
        } else {
            old_index_keys(engine, coll, prefix, doc_id, &doc_key)?
        };
        let new_keys: BTreeSet<Vec<u8>> = index_keys_for(coll, prefix, doc_id, &new_doc)
            .into_iter()
            .collect();
        (expires_at, old_keys, new_keys, existed)
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

    // One batch slot is reserved for this collection's counter record (it
    // commits in the same WAL record as the document write).
    if ops.len() + 1 > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(ops.len()));
    }
    Ok(WriteOps {
        ops,
        collection_id: coll.id,
        doc_delta: if existed { 0 } else { 1 },
    })
}

/// Prior index footprint for `doc_id`, plus whether the document existed at
/// all. ZDoc bodies use view extraction (no full-tree materialization); JSON
/// bodies decode as usual.
///
/// Existence is checked with [`Engine::get_including_expired`]: an
/// expired-but-unswept document still occupies a counter slot (the sweep
/// decrements when it tombstones), so re-upserting over it is a replace,
/// not an insert.
fn old_index_keys(
    engine: &mut Engine,
    coll: &CollectionMeta,
    prefix: &[u8],
    doc_id: &[u8],
    doc_key: &[u8],
) -> DocResult<(BTreeSet<Vec<u8>>, bool)> {
    Ok(match engine.get_including_expired(doc_key)? {
        Some(stored) if !stored.is_empty() => {
            let old_kind = stored[0];
            let old_payload = &stored[1..];
            let keys = if old_kind == VK_ZDOC {
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
            };
            (keys, true)
        }
        _ => (BTreeSet::new(), false),
    })
}

/// Insert or replace a document (no TTL). See [`upsert_with_expiry`].
pub fn upsert(
    engine: &mut Engine,
    catalog: &mut Catalog,
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
    catalog: &mut Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    payload: &[u8],
    is_zdoc: bool,
    expires_at: u64,
) -> DocResult<u64> {
    let w = upsert_ops(
        engine, catalog, prefix, collection, doc_id, payload, is_zdoc, expires_at,
    )?;
    commit_batches(engine, catalog, vec![w])
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
) -> DocResult<WriteOps> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let doc_key = keys::doc_key(prefix, coll.id, doc_id);
    let empty = || WriteOps {
        ops: Vec::new(),
        collection_id: coll.id,
        doc_delta: 0,
    };
    let stored = match engine.get(&doc_key)? {
        Some(v) => v,
        // Absent — or expired-but-unswept, in which case the sweep owns the
        // counter decrement.
        None => return Ok(empty()),
    };
    if let Some(f) = filter {
        if !crate::query::check_filter(&stored, f, doc_id) {
            return Ok(empty());
        }
    }

    let mut ops: Vec<BatchOp> = vec![BatchOp::Del { key: doc_key }];
    let payload = strip_value_kind(&stored);
    // A body that cannot be decoded (empty or garbage) has no derivable index
    // keys; the body row is still removed so an operator can clear it.
    match stored.first() {
        Some(&VK_ZDOC) => {
            let view = crate::binary::ValueView::new(payload);
            for k in index_keys_for_view(coll, prefix, doc_id, &view) {
                ops.push(BatchOp::Del { key: k });
            }
        }
        Some(_) => {
            if let Ok(old) = serde_json::from_slice::<Value>(payload) {
                for k in index_keys_for(coll, prefix, doc_id, &old) {
                    ops.push(BatchOp::Del { key: k });
                }
            }
        }
        None => {}
    }
    // One slot reserved for this collection's counter record.
    if ops.len() + 1 > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(ops.len()));
    }
    Ok(WriteOps {
        ops,
        collection_id: coll.id,
        doc_delta: -1,
    })
}

/// Delete a document and all of its index entries atomically. Returns whether
/// the document existed.
pub fn delete(
    engine: &mut Engine,
    catalog: &mut Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
) -> DocResult<bool> {
    let w = delete_ops(engine, catalog, prefix, collection, doc_id, None)?;
    if w.ops.is_empty() {
        return Ok(false);
    }
    commit_batches(engine, catalog, vec![w])?;
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
    catalog: &mut Catalog,
    prefix: &[u8],
    collection: &str,
    ids: &[Vec<u8>],
    filter: Option<&crate::filter::Filter>,
) -> DocResult<u64> {
    let mut per_doc: Vec<WriteOps> = Vec::with_capacity(ids.len());
    let mut deleted: u64 = 0;
    for id in ids {
        let w = delete_ops(engine, catalog, prefix, collection, id, filter)?;
        if !w.ops.is_empty() {
            deleted += 1;
            per_doc.push(w);
        }
    }
    commit_batches(engine, catalog, per_doc)?;
    Ok(deleted)
}

/// Commit one batch atomically with its counter records: the post-write
/// counters of every collection `deltas` touch are system puts inside the SAME
/// self-framed WAL record as the document ops, so a torn crash replays both or
/// neither. The in-memory catalog is updated only after the commit succeeds.
/// The caller must have reserved one batch slot per touched collection.
fn write_counted_chunk(
    engine: &mut Engine,
    catalog: &mut Catalog,
    ops: Vec<BatchOp>,
    deltas: &[CounterDelta],
) -> DocResult<u64> {
    let upd = catalog.counter_update(deltas);
    if ops.len() + upd.len() > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(ops.len()));
    }
    let seq = engine.write_batch_with_sys(ops, upd.sys_puts())?;
    catalog.apply_counter_update(&upd);
    Ok(seq)
}

/// Single-batch commit with counter deltas spanning multiple collections
/// (bounded transactions). The whole set must fit in one batch: `ops` plus
/// one counter record per collection with a non-zero delta. Returns the
/// committed seq.
pub fn commit_with_deltas(
    engine: &mut Engine,
    catalog: &mut Catalog,
    ops: Vec<BatchOp>,
    deltas: &[(u32, i64)],
) -> DocResult<u64> {
    if ops.is_empty() {
        return Ok(0);
    }
    let deltas: Vec<CounterDelta> = deltas
        .iter()
        .filter(|(_, delta)| *delta != 0)
        .map(|&(collection_id, delta)| CounterDelta::Doc {
            collection_id,
            delta,
        })
        .collect();
    write_counted_chunk(engine, catalog, ops, &deltas)
}

/// Submit pre-built per-document writes: one atomic batch when the total fits,
/// otherwise chunked. Each chunk carries the counter records of every
/// collection it touches, reflecting every delta up through that chunk, so a
/// crash between chunks leaves counters matching exactly the committed chunks.
/// Returns the last committed seq (0 when there was nothing to write).
pub(crate) fn commit_batches(
    engine: &mut Engine,
    catalog: &mut Catalog,
    per_doc: Vec<WriteOps>,
) -> DocResult<u64> {
    let total: usize = per_doc.iter().map(|w| w.ops.len()).sum();
    if total == 0 {
        return Ok(0);
    }

    let mut all: Vec<BatchOp> = Vec::with_capacity(std::cmp::min(total, MAX_BATCH_KEYS));
    let mut deltas: Vec<CounterDelta> = Vec::new();
    // Collections with a non-zero delta in the current chunk: each one costs
    // a batch slot for its counter record.
    let mut touched: BTreeSet<u32> = BTreeSet::new();
    let mut last_seq = 0u64;
    for mut w in per_doc {
        if w.ops.is_empty() {
            continue;
        }
        let counts = w.doc_delta != 0;
        let slots_after =
            touched.len() + usize::from(counts && !touched.contains(&w.collection_id));
        // Flush when this document's ops plus the chunk's counter records
        // would overflow the batch. A single document always fits an empty
        // chunk: its builders reserve one slot for its own counter record.
        if !all.is_empty() && all.len() + w.ops.len() + slots_after > MAX_BATCH_KEYS {
            last_seq = write_counted_chunk(engine, catalog, std::mem::take(&mut all), &deltas)?;
            deltas.clear();
            touched.clear();
        }
        if counts {
            touched.insert(w.collection_id);
            deltas.push(CounterDelta::Doc {
                collection_id: w.collection_id,
                delta: w.doc_delta,
            });
        }
        all.append(&mut w.ops);
    }

    if !all.is_empty() {
        last_seq = write_counted_chunk(engine, catalog, all, &deltas)?;
    }
    Ok(last_seq)
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
        Some(stored) => {
            let payload = strip_value_kind(&stored);
            if value_kind(&stored)? == VK_ZDOC {
                Ok(Some(crate::binary::ValueView::new(payload).to_value()?))
            } else {
                Ok(Some(
                    serde_json::from_slice(payload)
                        .map_err(|e| DocError::InvalidJson(e.to_string()))?,
                ))
            }
        }
        None => Ok(None),
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

    let entries = backfill_index(engine, prefix, collection_id, &meta)?;
    working.set_index_entry_count(meta.id, entries);
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

/// The new index entry a stored body contributes during backfill:
/// `(encoded_fields, expires_at)`. `None` skips a legacy JSON body that does
/// not parse; a ZDoc body is always indexable (its view is total).
fn backfill_entry(stored: &[u8], idx: &IndexMeta) -> DocResult<Option<(Vec<u8>, u64)>> {
    let dirs = idx.ascending();
    if value_kind(stored)? == VK_ZDOC {
        let view = crate::binary::ValueView::new(strip_value_kind(stored));
        return Ok(Some((
            encoding::encode_fields_from_view_with_directions(&view, &idx.fields, &dirs),
            derive_ttl_expires_at_view(&view, idx),
        )));
    }
    let doc: Value = match serde_json::from_slice(strip_value_kind(stored)) {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    let vals: Vec<Value> = idx
        .fields
        .iter()
        .map(|f| encoding::extract_path(&doc, f))
        .collect();
    Ok(Some((
        encoding::encode_fields_with_directions(&vals, &dirs),
        derive_ttl_expires_at(&doc, idx),
    )))
}

/// Scan every existing document in a collection and write the new index's
/// entries in chunks that respect `MAX_BATCH_KEYS`. Returns the number of
/// entries written (the index's initial `entry_count`).
///
/// For a unique index the written range is then scanned once in key order:
/// entries sort by `encoded_fields` and differ only in their `doc_id` suffix,
/// so two adjacent entries with equal encodings mean two documents share a
/// value. In that case every entry just written is removed again and
/// [`DocError::DuplicateKey`] is returned, leaving the store as it was. TTL
/// body stamps for a unique index are applied only after that check passes,
/// so a rejected DDL never changes document expiry.
fn backfill_index(
    engine: &mut Engine,
    prefix: &[u8],
    collection_id: u32,
    idx: &IndexMeta,
) -> DocResult<u64> {
    let dprefix = keys::doc_prefix(prefix, collection_id);
    let dhi = keys::prefix_upper_bound(&dprefix);
    let prefix_len = prefix.len();
    let is_ttl = idx.expire_after_seconds.is_some();
    let stamp_ttl_inline = is_ttl && !idx.unique;

    // An owned snapshot does not borrow the engine, so we can write index
    // entries back through `&mut engine` while iterating the doc range. The
    // snapshot's fixed seq ceiling means our own writes are never re-scanned.
    let snap = engine.snapshot_owned();
    let mut pending: Vec<BatchOp> = Vec::new();
    let mut entries: u64 = 0;
    let mut rows = snap.scan(dprefix.clone(), dhi.clone())?;
    for item in rows.by_ref() {
        let (doc_key, stored) = item?;
        let doc_id = keys::doc_id_from_doc_key(prefix_len, &doc_key);
        let Some((enc, expires_at)) = backfill_entry(&stored, idx)? else {
            continue;
        };
        let ikey = keys::index_key(prefix, collection_id, idx.id, &enc, &doc_id);
        pending.push(BatchOp::Put {
            key: ikey,
            value: doc_id,
            expires_at,
        });
        entries += 1;
        // When creating a TTL index, stamp body expiry so existing docs become
        // invisible under lazy expiry without waiting for a later rewrite.
        if stamp_ttl_inline && expires_at != 0 {
            pending.push(BatchOp::Put {
                key: doc_key,
                value: stored,
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

    if idx.unique {
        if unique_index_has_duplicate(engine, prefix, collection_id, idx.id)? {
            remove_index_range(engine, prefix, collection_id, idx.id)?;
            return Err(DocError::DuplicateKey(format!(
                "unique index '{}' on {:?}: existing documents share a value",
                idx.name, idx.fields
            )));
        }
        if is_ttl {
            let snap = engine.snapshot_owned();
            let mut pending: Vec<BatchOp> = Vec::new();
            let mut rows = snap.scan(dprefix, dhi)?;
            for item in rows.by_ref() {
                let (doc_key, stored) = item?;
                let Some((_, expires_at)) = backfill_entry(&stored, idx)? else {
                    continue;
                };
                if expires_at == 0 {
                    continue;
                }
                pending.push(BatchOp::Put {
                    key: doc_key,
                    value: stored,
                    expires_at,
                });
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
        }
    }
    Ok(entries)
}

/// True when two entries of the index share the same encoded field value.
/// Index keys are `header | encoded_fields | doc_id` with the entry value
/// equal to `doc_id`, so the encoding is `key[header .. key.len() - value.len()]`;
/// the encoding is prefix-free, so equal slices mean equal values. O(1)
/// memory: only the previous encoding is retained.
fn unique_index_has_duplicate(
    engine: &Engine,
    prefix: &[u8],
    collection_id: u32,
    index_id: u32,
) -> DocResult<bool> {
    let lo = keys::index_prefix(prefix, collection_id, index_id);
    let header = lo.len();
    let hi = keys::prefix_upper_bound(&lo);
    let snap = engine.snapshot_owned();
    let mut prev: Option<Vec<u8>> = None;
    for item in snap.scan(lo, hi)? {
        let (key, doc_id) = item?;
        let end = key.len().saturating_sub(doc_id.len());
        let enc = key.get(header..end).unwrap_or(&[]);
        match prev.as_mut() {
            Some(p) if p.as_slice() == enc => return Ok(true),
            Some(p) => {
                p.clear();
                p.extend_from_slice(enc);
            }
            None => prev = Some(enc.to_vec()),
        }
    }
    Ok(false)
}

/// Delete every key under one index's range in `MAX_BATCH_KEYS` chunks. Used
/// to undo a backfill whose unique check failed.
fn remove_index_range(
    engine: &mut Engine,
    prefix: &[u8],
    collection_id: u32,
    index_id: u32,
) -> DocResult<()> {
    let lo = keys::index_prefix(prefix, collection_id, index_id);
    let hi = keys::prefix_upper_bound(&lo);
    let snap = engine.snapshot_owned();
    let mut chunk: Vec<BatchOp> = Vec::new();
    let mut rows = snap.scan(lo, hi)?;
    for item in rows.by_ref() {
        let (key, _) = item?;
        chunk.push(BatchOp::Del { key });
        if chunk.len() >= MAX_BATCH_KEYS {
            engine.write_batch(std::mem::take(&mut chunk))?;
        }
    }
    drop(rows);
    drop(snap);
    if !chunk.is_empty() {
        engine.write_batch(chunk)?;
    }
    Ok(())
}

/// Durable TTL sweep with counter maintenance. Tombstones every expired
/// memtable entry AND applies the matching catalog counter deltas in the same
/// atomic batches, so a crash replays tombstones and counts together (the
/// engine's own `sweep_expired` is non-durable memtable hygiene and must not
/// be used on the document path — a replayed expired value would be swept and
/// counted twice).
///
/// Counter semantics: a document occupies its `doc_count` slot from upsert
/// until delete or sweep. Documents that expire after leaving the active
/// memtable (flushed, then dropped by compaction) are not observed by the
/// sweep, so counts can overstate live TTL'd documents — the conservative,
/// planner-safe direction. Raw-KV keys are tombstoned without any counter
/// change.
pub fn sweep_expired_with_counts(engine: &mut Engine, catalog: &mut Catalog) -> DocResult<usize> {
    let expired = engine.collect_expired();
    if expired.is_empty() {
        return Ok(0);
    }

    // Classify each expired key against the catalog's known collection
    // prefixes (snapshotted up front so the catalog can be mutated below).
    // Doc keys decrement doc_count; index keys decrement their index's
    // entry_count (they share the body's expiry and are swept alongside it).
    // Raw-KV keys are tombstoned with no counter change.
    let prefix_table: Vec<(Vec<u8>, u32)> = catalog
        .collections()
        .iter()
        .map(|c| (c.prefix.clone(), c.id))
        .collect();
    let classify = |key: &[u8]| -> SweepDelta {
        for (prefix, coll_id) in &prefix_table {
            let plen = prefix.len();
            if !key.starts_with(prefix) || key.len() < plen + 5 {
                continue;
            }
            let key_coll = u32::from_be_bytes(key[plen + 1..plen + 5].try_into().unwrap());
            if key_coll != *coll_id {
                continue;
            }
            if key[plen] == keys::REC_DOC {
                return SweepDelta::Doc(*coll_id);
            }
            if key[plen] == keys::REC_INDEX && key.len() >= plen + 9 {
                let index_id = u32::from_be_bytes(key[plen + 5..plen + 9].try_into().unwrap());
                return SweepDelta::Index(index_id);
            }
        }
        SweepDelta::Raw
    };

    let total = expired.len();
    // Each tombstone touches at most one collection's counter record, so
    // half a batch of tombstones can never overflow with its counters.
    let chunk_limit = MAX_BATCH_KEYS / 2;
    let mut chunk: Vec<BatchOp> = Vec::with_capacity(chunk_limit.min(total));
    let mut chunk_deltas: Vec<CounterDelta> = Vec::with_capacity(chunk_limit.min(total));
    for key in expired {
        // Doc keys move doc_count only; their index keys are swept (and
        // counted) alongside as separate `Index` deltas.
        match classify(&key) {
            SweepDelta::Doc(collection_id) => chunk_deltas.push(CounterDelta::DocOnly {
                collection_id,
                delta: -1,
            }),
            SweepDelta::Index(index_id) => chunk_deltas.push(CounterDelta::Index {
                index_id,
                delta: -1,
            }),
            SweepDelta::Raw => {}
        }
        chunk.push(BatchOp::Del { key });
        if chunk.len() >= chunk_limit {
            write_counted_chunk(engine, catalog, std::mem::take(&mut chunk), &chunk_deltas)?;
            chunk_deltas.clear();
        }
    }
    if !chunk.is_empty() {
        write_counted_chunk(engine, catalog, chunk, &chunk_deltas)?;
    }
    Ok(total)
}

/// Counter classification of one swept key.
enum SweepDelta {
    Doc(u32),
    Index(u32),
    Raw,
}
