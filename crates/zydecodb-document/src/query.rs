//! Read paths: get-by-id and index-range scans.
//!
//! Both operate on an owned [`SnapshotHandle`], which the server captures while
//! briefly holding the engine lock and then iterates with the lock released —
//! so a long scan never blocks concurrent writers.
//!
//! Pagination is cursor-based: the opaque cursor carries the snapshot sequence
//! ceiling (8-byte prefix) plus the resume position (the last index key, or an
//! offset for buffered/sorted scans). The next page resumes strictly after the
//! cursor position AND re-pins the same sequence ceiling via
//! `Engine::snapshot_at`, so pagination is repeatable-read: later pages never
//! observe writes newer than the first page (see `snapshot_at` for the exact
//! retention guarantee).

use crate::catalog::Catalog;
use crate::error::{DocError, DocResult};
use crate::filter::Filter;
use crate::planner::{self, AccessPath};
use crate::store::stored_to_json_vec;
use crate::{encoding, keys};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use zydecodb_engine::SnapshotHandle;

/// Default upper bound on documents buffered for an in-memory sort (collection
/// scan or descending/non-index-ordered sort). Beyond this the query is
/// rejected so a single request cannot exhaust server memory; add an index or
/// a tighter filter to stay under it. The server passes its configured
/// `[security] max_sort_buffer` instead of this constant.
pub const MAX_SORT_BUFFER: usize = 100_000;

/// A resolved index-range scan, built under lock (catalog + key math), then
/// executed lock-free against a snapshot.
#[derive(Debug, Clone)]
pub struct ScanSpec {
    /// Inclusive lower bound (full index key, possibly a cursor successor).
    pub lo: Vec<u8>,
    /// Exclusive upper bound (full index key, or the index prefix upper bound).
    pub hi: Vec<u8>,
    /// Max rows to return for this page.
    pub limit: usize,
    /// `prefix | 'd' | collection_id`, used to rebuild doc keys for body fetch.
    pub doc_prefix: Vec<u8>,
    /// Whether to fetch and return document bodies (vs doc ids only).
    pub include_bodies: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRow {
    pub doc_id: Vec<u8>,
    pub body: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct QueryPage {
    pub rows: Vec<QueryRow>,
    /// Opaque cursor for the next page, present only if more rows remain.
    pub next_cursor: Option<Vec<u8>>,
}

/// Point lookup by document id. Strips the `value_kind` byte from the body.
pub fn get_by_id(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
) -> DocResult<Option<Vec<u8>>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let dk = keys::doc_key(prefix, coll.id, doc_id);
    Ok(snap
        .get(&dk)?
        .map(|stored| stored_to_json_vec(&stored)))
}

/// Build a scan spec from the catalog and (optional) JSON-array bounds. Bounds
/// are JSON arrays of scalars matching the index field order, e.g. `[18]` or
/// `["alice", 7]`; absent bounds scan the whole index. A cursor (from a prior
/// page) overrides the lower bound.
#[allow(clippy::too_many_arguments)]
pub fn build_index_scan_spec(
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    index_name: &str,
    lo_json: Option<&[u8]>,
    hi_json: Option<&[u8]>,
    cursor: Option<&[u8]>,
    limit: usize,
    include_bodies: bool,
) -> DocResult<ScanSpec> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let idx = coll
        .indexes
        .iter()
        .find(|i| i.name == index_name)
        .ok_or_else(|| DocError::IndexNotFound(index_name.to_string()))?;

    let iprefix = keys::index_prefix(prefix, coll.id, idx.id);

    let mut lo = iprefix.clone();
    if let Some(j) = lo_json {
        lo.extend_from_slice(&encode_bound(j)?);
    }
    // A cursor resumes strictly after the last row of the previous page. The
    // leading seq prefix is consumed by the caller (to re-pin the snapshot); the
    // body is the last index key.
    if let Some(c) = cursor {
        let (_seq, body) = split_cursor_seq(c)?;
        lo = body.to_vec();
        lo.push(0x00);
    }

    let hi = match hi_json {
        Some(j) => {
            let mut h = iprefix.clone();
            h.extend_from_slice(&encode_bound(j)?);
            h
        }
        None => keys::prefix_upper_bound(&iprefix),
    };

    Ok(ScanSpec {
        lo,
        hi,
        limit: limit.max(1),
        doc_prefix: keys::doc_prefix(prefix, coll.id),
        include_bodies,
    })
}

fn encode_bound(json: &[u8]) -> DocResult<Vec<u8>> {
    let vals: Vec<Value> =
        serde_json::from_slice(json).map_err(|e| DocError::InvalidJson(e.to_string()))?;
    Ok(encoding::encode_fields(&vals))
}

/// Execute an index range scan against a snapshot, lock-free. Returns up to
/// `limit` rows and a cursor when more remain.
pub fn execute_index_scan(snap: &SnapshotHandle, spec: &ScanSpec) -> DocResult<QueryPage> {
    let mut rows: Vec<QueryRow> = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;
    let mut next_cursor: Option<Vec<u8>> = None;

    let iter = snap.scan(spec.lo.clone(), spec.hi.clone())?;
    for item in iter {
        let (ikey, doc_id) = item?; // index entry value IS the doc id
        if rows.len() == spec.limit {
            // We already have a full page and at least one more row exists, so
            // hand back a cursor pointing at the last returned row (prefixed
            // with the snapshot seq so the next page re-pins this read view).
            next_cursor = last_key
                .as_ref()
                .map(|k| with_cursor_seq(snap.seq_upper(), k));
            break;
        }
        let body = if spec.include_bodies {
            let mut dk = spec.doc_prefix.clone();
            dk.extend_from_slice(&doc_id);
            snap.get(&dk)?
                .map(|stored| stored_to_json_vec(&stored))
        } else {
            None
        };
        rows.push(QueryRow { doc_id, body });
        last_key = Some(ikey);
    }

    Ok(QueryPage { rows, next_cursor })
}

// ---------------------------------------------------------------------------
// Filter-driven find: planner + residual evaluation + sort/skip/limit/projection
// ---------------------------------------------------------------------------

/// Field projection: include only the listed paths (plus `_id`) or exclude
/// them. Paths may be dotted (`"address.city"`).
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Include(Vec<String>),
    Exclude(Vec<String>),
}

/// A fully-specified find request, executed lock-free against a snapshot.
#[derive(Debug, Clone)]
pub struct FindSpec {
    pub filter: Filter,
    /// Sort keys applied after filtering: `(dotted_path, ascending)`.
    pub sort: Vec<(String, bool)>,
    pub projection: Option<Projection>,
    pub skip: usize,
    /// Page size (rows returned per call); must be >= 1.
    pub limit: usize,
    /// Opaque cursor from a prior page.
    pub cursor: Option<Vec<u8>>,
}

const CUR_KEY: u8 = 0x01;
const CUR_OFFSET: u8 = 0x02;

enum Cursor {
    Key(Vec<u8>),
    Offset(usize),
}

/// Every cursor is prefixed with the snapshot sequence ceiling it was created
/// against, so the next page re-pins the same read view via `snapshot_at`
/// (repeatable-read pagination). Splits that prefix off the cursor body.
fn split_cursor_seq(c: &[u8]) -> DocResult<(u64, &[u8])> {
    if c.len() < 8 {
        return Err(DocError::Protocol("malformed cursor".into()));
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&c[..8]);
    Ok((u64::from_be_bytes(b), &c[8..]))
}

fn with_cursor_seq(seq: u64, body: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(8 + body.len());
    c.extend_from_slice(&seq.to_be_bytes());
    c.extend_from_slice(body);
    c
}

/// The snapshot sequence ceiling a prior page's cursor was pinned to, if the
/// cursor is well-formed. The server uses this to rebuild the same snapshot.
pub fn cursor_snapshot_seq(cursor: Option<&[u8]>) -> Option<u64> {
    cursor.and_then(|c| split_cursor_seq(c).ok().map(|(s, _)| s))
}

fn encode_key_cursor(seq: u64, k: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + k.len());
    body.push(CUR_KEY);
    body.extend_from_slice(k);
    with_cursor_seq(seq, &body)
}

fn encode_offset_cursor(seq: u64, off: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(9);
    body.push(CUR_OFFSET);
    body.extend_from_slice(&(off as u64).to_be_bytes());
    with_cursor_seq(seq, &body)
}

fn decode_cursor(c: &[u8]) -> DocResult<Cursor> {
    let (_seq, body) = split_cursor_seq(c)?;
    match body.first() {
        Some(&CUR_KEY) => Ok(Cursor::Key(body[1..].to_vec())),
        Some(&CUR_OFFSET) if body.len() == 9 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&body[1..]);
            Ok(Cursor::Offset(u64::from_be_bytes(b) as usize))
        }
        _ => Err(DocError::Protocol("malformed cursor".into())),
    }
}

/// Does the index's field order already satisfy the requested sort (so we can
/// stream by key instead of buffering)? True when there is no sort, or the sort
/// is a leading ascending prefix of the index fields.
fn sort_matches_index(sort: &[(String, bool)], fields: &[String]) -> bool {
    if sort.is_empty() {
        return true;
    }
    if sort.len() > fields.len() {
        return false;
    }
    sort.iter().zip(fields).all(|((p, asc), f)| *asc && p == f)
}

/// Parse a stored body and make `_id` a virtual always-present field equal to
/// the document key, so filters/projections on `_id` work even when the body
/// (raw-wire writes) did not include it. A body that already has `_id` keeps
/// its value (the driver convention).
pub(crate) fn check_filter(stored: &[u8], filter: &crate::filter::Filter, doc_id: &[u8]) -> bool {
    let kind = stored[0];
    let payload = crate::store::strip_value_kind(stored);
    let temp_zdoc;
    let view = if kind == crate::store::VK_ZDOC {
        crate::binary::ValueView::new(payload)
    } else {
        let val: serde_json::Value = serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null);
        temp_zdoc = crate::binary::ZDocBuilder::from_value(&val);
        crate::binary::ValueView::new(&temp_zdoc)
    };
    
    filter.matches(view, Some(doc_id))
}

fn parse_doc(stored: &[u8], doc_id: &[u8]) -> Option<serde_json::Value> {
    let kind = stored[0];
    let payload = crate::store::strip_value_kind(stored);
    let mut v = if kind == crate::store::VK_ZDOC {
        crate::binary::ValueView::new(payload).to_value()
    } else {
        serde_json::from_slice(payload).ok()?
    };
    
    if let serde_json::Value::Object(map) = &mut v {
        map.entry(crate::planner::ID_FIELD.to_string())
            .or_insert_with(|| serde_json::Value::String(String::from_utf8_lossy(doc_id).into_owned()));
    }
    Some(v)
}

fn make_row(doc_id: Vec<u8>, body: &Value, proj: &Option<Projection>) -> DocResult<QueryRow> {
    let shaped = apply_projection(body, proj);
    let bytes = serde_json::to_vec(&shaped).map_err(|e| DocError::InvalidJson(e.to_string()))?;
    Ok(QueryRow {
        doc_id,
        body: Some(bytes),
    })
}

/// Execute a filtered find. The planner narrows candidates; the FULL filter is
/// then re-evaluated against every materialized document (residual check), so
/// the result is correct regardless of which access path was chosen.
pub fn execute_find(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    spec: &FindSpec,
    max_sort_buffer: usize,
) -> DocResult<QueryPage> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let path = planner::plan(&spec.filter, prefix, coll);
    let doc_prefix = keys::doc_prefix(prefix, coll.id);
    let limit = spec.limit.max(1);

    match &path {
        AccessPath::ById(id) => {
            let dk = keys::doc_key(prefix, coll.id, id);
            let mut rows = Vec::new();
            if let Some(stored) = snap.get(&dk)? {
                if check_filter(&stored, &spec.filter, id) {
                    if spec.skip == 0 {
                        if let Some(v) = parse_doc(&stored, id) {
                            rows.push(make_row(id.clone(), &v, &spec.projection)?);
                        }
                    }
                }
            }
            Ok(QueryPage {
                rows,
                next_cursor: None,
            })
        }
        AccessPath::IndexScan { lo, hi, fields }
            if spec.skip == 0
                && sort_matches_index(&spec.sort, fields)
                && matches!(
                    spec.cursor.as_deref().map(decode_cursor),
                    None | Some(Ok(Cursor::Key(_)))
                ) =>
        {
            key_mode_page(snap, spec, &doc_prefix, lo, hi, limit)
        }
        _ => offset_mode_page(snap, spec, &path, &doc_prefix, limit, max_sort_buffer),
    }
}

/// Key-streaming pagination over an index range: resume strictly after the last
/// returned row's index key. Used when the scan order already satisfies the
/// sort and no `skip` is requested.
fn key_mode_page(
    snap: &SnapshotHandle,
    spec: &FindSpec,
    doc_prefix: &[u8],
    lo: &[u8],
    hi: &[u8],
    limit: usize,
) -> DocResult<QueryPage> {
    let start = match spec.cursor.as_deref() {
        Some(c) => match decode_cursor(c)? {
            Cursor::Key(mut k) => {
                k.push(0x00);
                k
            }
            Cursor::Offset(_) => return Err(DocError::Protocol("cursor mode changed".into())),
        },
        None => lo.to_vec(),
    };

    let mut rows = Vec::new();
    let mut next_cursor = None;
    let mut last_match_key: Option<Vec<u8>> = None;

    let iter = snap.scan(start, hi.to_vec())?;
    for item in iter {
        let (ikey, doc_id) = item?;
        let mut dk = doc_prefix.to_vec();
        dk.extend_from_slice(&doc_id);
        if let Some(stored) = snap.get(&dk)? {
            if check_filter(&stored, &spec.filter, &doc_id) {
                if rows.len() == limit {
                    next_cursor = last_match_key.map(|k| encode_key_cursor(snap.seq_upper(), &k));
                    break;
                }
                if let Some(v) = parse_doc(&stored, &doc_id) {
                    rows.push(make_row(doc_id, &v, &spec.projection)?);
                    last_match_key = Some(ikey);
                }
            }
        }
    }

    Ok(QueryPage { rows, next_cursor })
}

/// Offset-based pagination. Without a sort the candidates are streamed and the
/// offset is skipped in source order; with a sort all matches are buffered
/// (bounded by [`MAX_SORT_BUFFER`]), sorted, then sliced.
fn offset_mode_page(
    snap: &SnapshotHandle,
    spec: &FindSpec,
    path: &AccessPath,
    doc_prefix: &[u8],
    limit: usize,
    max_sort_buffer: usize,
) -> DocResult<QueryPage> {
    let offset = match spec.cursor.as_deref() {
        Some(c) => match decode_cursor(c)? {
            Cursor::Offset(o) => o,
            Cursor::Key(_) => return Err(DocError::Protocol("cursor mode changed".into())),
        },
        None => spec.skip,
    };

    if spec.sort.is_empty() {
        stream_offset_page(snap, spec, path, doc_prefix, offset, limit)
    } else {
        buffered_sort_page(snap, spec, path, doc_prefix, offset, limit, max_sort_buffer)
    }
}

/// Visit every candidate document for `path`, applying the residual `filter`,
/// and call `f(doc_id, body)` for each match (return `false` to stop early).
/// Bodies that fail to parse are skipped. `ById` is materialized as a single
/// point lookup so callers (count/distinct/find_ids) work for every path.
fn for_each_match<F: FnMut(Vec<u8>, &[u8]) -> DocResult<bool>>(
    snap: &SnapshotHandle,
    filter: &Filter,
    path: &AccessPath,
    doc_prefix: &[u8],
    prefix_len: usize,
    mut f: F,
) -> DocResult<()> {
    match path {
        AccessPath::ById(id) => {
            let mut dk = doc_prefix.to_vec();
            dk.extend_from_slice(id);
            if let Some(stored) = snap.get(&dk)? {
                if check_filter(&stored, filter, id) {
                    f(id.clone(), &stored)?;
                }
            }
        }
        AccessPath::IndexScan { lo, hi, .. } => {
            let iter = snap.scan(lo.clone(), hi.clone())?;
            for item in iter {
                let (_ikey, doc_id) = item?;
                let mut dk = doc_prefix.to_vec();
                dk.extend_from_slice(&doc_id);
                if let Some(stored) = snap.get(&dk)? {
                    if check_filter(&stored, filter, &doc_id) {
                        if !f(doc_id, &stored)? {
                            return Ok(());
                        }
                    }
                }
            }
        }
        AccessPath::CollectionScan => {
            let hi = keys::prefix_upper_bound(doc_prefix);
            let iter = snap.scan(doc_prefix.to_vec(), hi)?;
            for item in iter {
                let (doc_key, stored) = item?;
                let doc_id = keys::doc_id_from_doc_key(prefix_len, &doc_key);
                if check_filter(&stored, filter, &doc_id) {
                    if !f(doc_id, &stored)? {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

/// `(planned path, doc_prefix, prefix_len)` for a collection + filter.
fn plan_scan(
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    filter: &Filter,
) -> DocResult<(AccessPath, Vec<u8>, usize)> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let path = planner::plan(filter, prefix, coll);
    let doc_prefix = keys::doc_prefix(prefix, coll.id);
    Ok((path, doc_prefix, prefix.len()))
}

/// Collect the ids of all documents matching `filter` (bounded by
/// [`MAX_SORT_BUFFER`]); used to select candidates for `update_many`/
/// `delete_many` from a lock-free snapshot before writing.
pub fn find_ids(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    filter: &Filter,
    cap: usize,
) -> DocResult<Vec<Vec<u8>>> {
    let (path, doc_prefix, prefix_len) = plan_scan(catalog, prefix, collection, filter)?;
    let mut ids = Vec::new();
    let mut overflow = false;
    for_each_match(
        snap,
        filter,
        &path,
        &doc_prefix,
        prefix_len,
        |doc_id, _v| {
            if ids.len() >= cap {
                overflow = true;
                return Ok(false);
            }
            ids.push(doc_id);
            Ok(true)
        },
    )?;
    if overflow {
        return Err(DocError::BadFilter(format!(
            "matched more than {cap} documents; narrow the filter"
        )));
    }
    Ok(ids)
}

/// Id of the first document matching `filter` (in access-path order), if any.
/// Used by `update_one`/`delete_one`.
pub fn find_first_id(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    filter: &Filter,
) -> DocResult<Option<Vec<u8>>> {
    let (path, doc_prefix, prefix_len) = plan_scan(catalog, prefix, collection, filter)?;
    let mut first = None;
    for_each_match(
        snap,
        filter,
        &path,
        &doc_prefix,
        prefix_len,
        |doc_id, _v| {
            first = Some(doc_id);
            Ok(false)
        },
    )?;
    Ok(first)
}

/// Count documents matching `filter` without materializing result bodies.
pub fn count(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    filter: &Filter,
) -> DocResult<u64> {
    let (path, doc_prefix, prefix_len) = plan_scan(catalog, prefix, collection, filter)?;
    let mut n: u64 = 0;
    for_each_match(snap, filter, &path, &doc_prefix, prefix_len, |_id, _v| {
        n += 1;
        Ok(true)
    })?;
    Ok(n)
}

/// Distinct scalar values of `field` across documents matching `filter`,
/// returned in index (encoding) order.
pub fn distinct(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    field: &str,
    filter: &Filter,
) -> DocResult<Vec<Value>> {
    let (path, doc_prefix, prefix_len) = plan_scan(catalog, prefix, collection, filter)?;
    let mut seen: Vec<Value> = Vec::new();
    for_each_match(snap, filter, &path, &doc_prefix, prefix_len, |doc_id, stored| {
        if let Some(v) = parse_doc(stored, &doc_id) {
            let val = encoding::extract_path(&v, field);
            if seen
                .binary_search_by(|probe| encoding::cmp_values(probe, &val))
                .is_err()
            {
                let pos = seen
                    .binary_search_by(|probe| encoding::cmp_values(probe, &val))
                    .unwrap_or_else(|e| e);
                seen.insert(pos, val);
            }
        }
        Ok(true)
    })?;
    Ok(seen)
}

fn stream_offset_page(
    snap: &SnapshotHandle,
    spec: &FindSpec,
    path: &AccessPath,
    doc_prefix: &[u8],
    offset: usize,
    limit: usize,
) -> DocResult<QueryPage> {
    let prefix_len = doc_prefix.len().saturating_sub(1 + 4);
    let mut rows: Vec<QueryRow> = Vec::new();
    let mut seen = 0usize;
    let mut has_more = false;

    for_each_match(
        snap,
        &spec.filter,
        path,
        doc_prefix,
        prefix_len,
        |doc_id, stored| {
            if seen < offset {
                seen += 1;
                return Ok(true);
            }
            if rows.len() == limit {
                has_more = true;
                return Ok(false);
            }
            if let Some(v) = parse_doc(stored, &doc_id) {
                rows.push(make_row(doc_id, &v, &spec.projection)?);
            }
            Ok(true)
        },
    )?;

    let next_cursor = has_more.then(|| encode_offset_cursor(snap.seq_upper(), offset + limit));
    Ok(QueryPage { rows, next_cursor })
}

fn extract_sort_keys(stored: &[u8], sort: &[(String, bool)]) -> Vec<Vec<u8>> {
    let kind = stored[0];
    let payload = crate::store::strip_value_kind(stored);
    let temp_zdoc;
    let view = if kind == crate::store::VK_ZDOC {
        crate::binary::ValueView::new(payload)
    } else {
        let val: serde_json::Value = serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null);
        temp_zdoc = crate::binary::ZDocBuilder::from_value(&val);
        crate::binary::ValueView::new(&temp_zdoc)
    };

    let mut keys = Vec::with_capacity(sort.len());
    for (path, _) in sort {
        if let Some(v) = view.get_path(path) {
            let mut out = Vec::new();
            crate::encoding::encode_value(&v.to_value(), &mut out);
            keys.push(out);
        } else {
            keys.push(vec![0x00]); // TAG_NULL
        }
    }
    keys
}

fn buffered_sort_page(
    snap: &SnapshotHandle,
    spec: &FindSpec,
    path: &AccessPath,
    doc_prefix: &[u8],
    offset: usize,
    limit: usize,
    max_sort_buffer: usize,
) -> DocResult<QueryPage> {
    let prefix_len = doc_prefix.len().saturating_sub(1 + 4);
    let mut all: Vec<(Vec<u8>, Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
    let mut overflow = false;

    for_each_match(
        snap,
        &spec.filter,
        path,
        doc_prefix,
        prefix_len,
        |doc_id, stored| {
            if all.len() >= max_sort_buffer {
                overflow = true;
                return Ok(false);
            }
            let keys = extract_sort_keys(stored, &spec.sort);
            all.push((doc_id, stored.to_vec(), keys));
            Ok(true)
        },
    )?;

    if overflow {
        return Err(DocError::BadFilter(format!(
            "sorted result exceeds {max_sort_buffer} documents; add an index or a tighter filter"
        )));
    }

    let sort = spec.sort.clone();
    all.sort_by(|a, b| compare_sort_keys(&a.2, &b.2, &sort));

    let total = all.len();
    let end = offset.saturating_add(limit).min(total);
    let slice_start = offset.min(total);
    let mut rows = Vec::with_capacity(end - slice_start);
    for (doc_id, stored, _) in &all[slice_start..end] {
        if let Some(v) = parse_doc(stored, doc_id) {
            rows.push(make_row(doc_id.clone(), &v, &spec.projection)?);
        }
    }
    let next_cursor = (end < total).then(|| encode_offset_cursor(snap.seq_upper(), end));
    Ok(QueryPage { rows, next_cursor })
}

fn compare_sort_keys(a: &[Vec<u8>], b: &[Vec<u8>], sort: &[(String, bool)]) -> Ordering {
    for (i, (_, asc)) in sort.iter().enumerate() {
        let ord = a[i].cmp(&b[i]);
        let ord = if *asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// --- Projection ------------------------------------------------------------

fn apply_projection(body: &Value, proj: &Option<Projection>) -> Value {
    match proj {
        None => body.clone(),
        Some(Projection::Exclude(paths)) => {
            let mut out = body.clone();
            for p in paths {
                remove_path(&mut out, p);
            }
            out
        }
        Some(Projection::Include(paths)) => {
            let mut out = Value::Object(Map::new());
            // `_id` is included by default unless the caller used an exclude.
            copy_path(body, &mut out, planner::ID_FIELD);
            for p in paths {
                copy_path(body, &mut out, p);
            }
            out
        }
    }
}

fn remove_path(v: &mut Value, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = v;
    for seg in &segs[..segs.len() - 1] {
        match cur {
            Value::Object(m) => match m.get_mut(*seg) {
                Some(next) => cur = next,
                None => return,
            },
            _ => return,
        }
    }
    if let Value::Object(m) = cur {
        m.remove(segs[segs.len() - 1]);
    }
}

fn copy_path(src: &Value, dst: &mut Value, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut s = src;
    for seg in &segs {
        match s {
            Value::Object(m) => match m.get(*seg) {
                Some(next) => s = next,
                None => return,
            },
            _ => return,
        }
    }
    let mut d = dst;
    for seg in &segs[..segs.len() - 1] {
        let map = match d {
            Value::Object(m) => m,
            _ => return,
        };
        d = map
            .entry((*seg).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    if let Value::Object(m) = d {
        m.insert(segs[segs.len() - 1].to_string(), s.clone());
    }
}
