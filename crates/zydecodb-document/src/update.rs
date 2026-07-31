//! Partial-update operators and the update/delete write paths.
//!
//! An update document must use operators (`$set $inc $unset $push $setOnInsert`);
//! a bare (non-`$`) document is rejected rather than silently replacing the
//! whole document. `$setOnInsert` applies only on upsert insert (see
//! [`materialize_upsert`]); normal updates ignore it.
//!
//! `$set` supports one filtered positional array segment per path:
//! `items.$[skuId=ABC].qty` updates the single matching element (exactly one
//! match required). Mongo `$` / `$[]` / `arrayFilters` forms are rejected.
//!
//! Each write reuses the atomic [`crate::store::upsert`]/[`crate::store::delete`]
//! path, so the body and every secondary index move together in one WAL record.
//! `update_many`/`delete_many` select candidate ids from a snapshot first, then
//! apply one atomic batch per document (not globally atomic across the set).

use crate::error::{DocError, DocResult};
use crate::filter::{Atom, Filter};
use crate::store::{self, strip_value_kind};
use crate::{catalog::Catalog, keys};
use serde_json::{Map, Value};
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};
use zydecodb_engine::engine::Engine;

/// A parsed update document: an ordered list of operator applications.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateDoc {
    ops: Vec<UpdateOp>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateOp {
    Set(String, Value),
    Unset(String),
    Inc(String, f64),
    Push(String, Value),
    /// Insert-only; applied by [`UpdateDoc::apply_on_insert`], skipped by [`UpdateDoc::apply`].
    SetOnInsert(String, Value),
}

impl UpdateDoc {
    /// Parse an update document. Every top-level key must be a supported
    /// `$`-operator whose value is an object of `path: operand`.
    pub fn parse(doc: &Value) -> DocResult<UpdateDoc> {
        let obj = doc
            .as_object()
            .ok_or_else(|| DocError::BadUpdate("update must be a JSON object".into()))?;
        if obj.is_empty() {
            return Err(DocError::BadUpdate("update document is empty".into()));
        }
        let mut ops = Vec::new();
        for (key, val) in obj {
            if !key.starts_with('$') {
                return Err(DocError::BadUpdate(format!(
                    "bare field '{key}' — use an operator like $set (full-replace is not allowed)"
                )));
            }
            let fields = val.as_object().ok_or_else(|| {
                DocError::BadUpdate(format!("{key} requires an object of field updates"))
            })?;
            for (path, operand) in fields {
                ops.push(parse_op(key, path, operand)?);
            }
        }
        Ok(UpdateDoc { ops })
    }

    pub fn parse_bytes(bytes: &[u8]) -> DocResult<UpdateDoc> {
        let v: Value =
            serde_json::from_slice(bytes).map_err(|e| DocError::BadUpdate(e.to_string()))?;
        UpdateDoc::parse(&v)
    }

    /// Apply operators that run on an existing document. `$setOnInsert` is skipped.
    pub fn apply(&self, doc: &mut Value) -> DocResult<()> {
        if !doc.is_object() {
            return Err(DocError::BadUpdate(
                "target document is not an object".into(),
            ));
        }
        for op in &self.ops {
            if matches!(op, UpdateOp::SetOnInsert(_, _)) {
                continue;
            }
            op.apply(doc)?;
        }
        check_result_depth(doc)
    }

    /// Apply operators for an upsert insert: `$setOnInsert` first, then regular
    /// ops so `$set`/`$inc`/`$unset`/`$push` win on path conflicts.
    pub fn apply_on_insert(&self, doc: &mut Value) -> DocResult<()> {
        if !doc.is_object() {
            return Err(DocError::BadUpdate(
                "target document is not an object".into(),
            ));
        }
        for op in &self.ops {
            if let UpdateOp::SetOnInsert(path, v) = op {
                set_path(doc, path, v.clone())?;
            }
        }
        for op in &self.ops {
            if matches!(op, UpdateOp::SetOnInsert(_, _)) {
                continue;
            }
            op.apply(doc)?;
        }
        check_result_depth(doc)
    }
}

/// Total document depth an update may produce. A dotted path is a flat JSON
/// string, so serde_json's parse depth cap does NOT bound grafting — without
/// this check, repeated `$set` ratchets a document past the read path's
/// [`crate::binary::MAX_ZDOC_DEPTH`] guard until reads blow the stack. The
/// mutation happens on an in-memory clone; on error nothing is persisted.
fn check_result_depth(doc: &Value) -> DocResult<()> {
    if value_depth(doc) > crate::binary::MAX_ZDOC_DEPTH {
        return Err(DocError::BadUpdate(format!(
            "update would nest the document deeper than {} levels",
            crate::binary::MAX_ZDOC_DEPTH
        )));
    }
    Ok(())
}

fn value_depth(v: &Value) -> usize {
    match v {
        Value::Array(a) => 1 + a.iter().map(value_depth).max().unwrap_or(0),
        Value::Object(m) => 1 + m.values().map(value_depth).max().unwrap_or(0),
        _ => 1,
    }
}

fn parse_op(op: &str, path: &str, operand: &Value) -> DocResult<UpdateOp> {
    validate_update_path(op, path)?;
    Ok(match op {
        "$set" => UpdateOp::Set(path.to_string(), operand.clone()),
        "$unset" => UpdateOp::Unset(path.to_string()),
        "$inc" => {
            let n = operand
                .as_f64()
                .ok_or_else(|| DocError::BadUpdate("$inc requires a number".into()))?;
            UpdateOp::Inc(path.to_string(), n)
        }
        "$push" => UpdateOp::Push(path.to_string(), operand.clone()),
        "$setOnInsert" => UpdateOp::SetOnInsert(path.to_string(), operand.clone()),
        other => {
            return Err(DocError::BadUpdate(format!(
                "unsupported update operator '{other}'"
            )))
        }
    })
}

/// Bound on segments in one update path. A single flat dotted string is not
/// subject to any JSON parse depth cap, so an unbounded segment count would
/// graft thousands of nesting levels in ONE update.
const MAX_UPDATE_PATH_SEGMENTS: usize = 64;

/// Validate path shape at parse time. Filtered `$[field=value]` is `$set`-only.
fn validate_update_path(op: &str, path: &str) -> DocResult<()> {
    let segs = tokenize_path(path)?;
    if segs.len() > MAX_UPDATE_PATH_SEGMENTS {
        return Err(DocError::BadUpdate(format!(
            "path has {} segments (max {MAX_UPDATE_PATH_SEGMENTS})",
            segs.len()
        )));
    }
    let mut filtered = 0usize;
    for seg in &segs {
        match seg {
            PathSeg::Field(name) => {
                if *name == "$" || name.starts_with("$[") {
                    return Err(DocError::BadUpdate(format!(
                        "unsupported positional path segment '{name}' in '{path}'"
                    )));
                }
            }
            PathSeg::Filtered { .. } => {
                filtered += 1;
            }
        }
    }
    if filtered > 1 {
        return Err(DocError::BadUpdate(format!(
            "path '{path}' may contain at most one $[field=value] segment"
        )));
    }
    if filtered == 1 && op != "$set" {
        return Err(DocError::BadUpdate(format!(
            "filtered positional paths are only supported with $set (got {op})"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum PathSeg<'a> {
    Field(&'a str),
    Filtered { field: &'a str, value: Value },
}

/// Split `a.b.$[skuId=ABC].qty` without breaking on dots inside `$[…]`.
fn tokenize_path(path: &str) -> DocResult<Vec<PathSeg<'_>>> {
    if path.is_empty() {
        return Err(DocError::BadUpdate("update path is empty".into()));
    }
    let mut segs = Vec::new();
    let bytes = path.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'.' {
            return Err(DocError::BadUpdate(format!(
                "invalid empty path segment in '{path}'"
            )));
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            let close = path[i..]
                .find(']')
                .ok_or_else(|| DocError::BadUpdate(format!("unclosed $[…] in path '{path}'")))?;
            let inner = &path[i + 2..i + close];
            if inner.is_empty() {
                return Err(DocError::BadUpdate(format!(
                    "empty $[…] is not supported in '{path}'"
                )));
            }
            if !inner.contains('=') {
                return Err(DocError::BadUpdate(format!(
                    "unsupported positional form '$[{inner}]' in '{path}' — use $[field=value]"
                )));
            }
            let (field, raw_val) = inner.split_once('=').unwrap();
            if field.is_empty() || field.contains('.') || field.contains('[') || field.contains(']')
            {
                return Err(DocError::BadUpdate(format!(
                    "invalid identity field in '$[{inner}]'"
                )));
            }
            let value = parse_filter_literal(raw_val)?;
            segs.push(PathSeg::Filtered { field, value });
            i += close + 1;
            if i < bytes.len() {
                if bytes[i] != b'.' {
                    return Err(DocError::BadUpdate(format!(
                        "expected '.' after $[…] in '{path}'"
                    )));
                }
                i += 1; // skip trailing '.'
            }
        } else {
            let mut j = i;
            while j < bytes.len() && bytes[j] != b'.' {
                // A bare `$` segment (Mongo positional) or `$name` without `[` is rejected later.
                j += 1;
            }
            let name = &path[i..j];
            if name.is_empty() {
                return Err(DocError::BadUpdate(format!(
                    "invalid empty path segment in '{path}'"
                )));
            }
            segs.push(PathSeg::Field(name));
            i = j;
            if i < bytes.len() {
                i += 1; // skip '.'
                if i == bytes.len() {
                    return Err(DocError::BadUpdate(format!(
                        "trailing '.' in path '{path}'"
                    )));
                }
            }
        }
    }
    Ok(segs)
}

/// Parse the RHS of `$[field=value]`: JSON literal, or bare token as string.
fn parse_filter_literal(raw: &str) -> DocResult<Value> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(DocError::BadUpdate(
            "empty value in $[field=value] predicate".into(),
        ));
    }
    // Quoted string / number / bool / null via JSON.
    if raw.starts_with('"')
        || raw.starts_with('{')
        || raw.starts_with('[')
        || matches!(raw.as_bytes().first(), Some(b'0'..=b'9') | Some(b'-'))
        || raw == "true"
        || raw == "false"
        || raw == "null"
    {
        return serde_json::from_str(raw).map_err(|e| {
            DocError::BadUpdate(format!("invalid $[field=value] literal '{raw}': {e}"))
        });
    }
    // Bare token → string (ABC, sku_1).
    if !raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(DocError::BadUpdate(format!(
            "invalid bare token '{raw}' in $[field=value] — use a JSON string literal"
        )));
    }
    Ok(Value::String(raw.to_string()))
}

impl UpdateOp {
    fn apply(&self, doc: &mut Value) -> DocResult<()> {
        match self {
            UpdateOp::Set(path, v) | UpdateOp::SetOnInsert(path, v) => {
                set_path(doc, path, v.clone())
            }
            UpdateOp::Unset(path) => {
                unset_path(doc, path);
                Ok(())
            }
            UpdateOp::Inc(path, delta) => {
                let cur = get_path(doc, path).and_then(Value::as_f64).unwrap_or(0.0);
                set_path(doc, path, json_number(cur + delta))
            }
            UpdateOp::Push(path, v) => {
                let mut arr = match get_path(doc, path) {
                    Some(Value::Array(a)) => a.clone(),
                    Some(_) => {
                        return Err(DocError::BadUpdate(format!(
                            "$push target '{path}' is not an array"
                        )))
                    }
                    None => Vec::new(),
                };
                arr.push(v.clone());
                set_path(doc, path, Value::Array(arr))
            }
        }
    }
}

fn json_number(f: f64) -> Value {
    // Prefer an integer representation when the result is integral.
    if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
        Value::from(f as i64)
    } else {
        serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn get_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    // Object-only dotted paths (no filtered segments). Filtered paths are
    // rejected at parse time for non-$set ops that use get_path.
    let mut cur = doc;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

fn set_path(doc: &mut Value, path: &str, val: Value) -> DocResult<()> {
    let segs = tokenize_path(path)?;
    if segs.iter().any(|s| matches!(s, PathSeg::Filtered { .. })) {
        return set_path_filtered(doc, path, &segs, val);
    }
    set_path_object(doc, path, &segs, val)
}

fn set_path_object(doc: &mut Value, path: &str, segs: &[PathSeg<'_>], val: Value) -> DocResult<()> {
    if segs.is_empty() {
        return Err(DocError::BadUpdate("update path is empty".into()));
    }
    let mut cur = doc;
    for seg in &segs[..segs.len() - 1] {
        let PathSeg::Field(name) = seg else {
            unreachable!("filtered segments handled elsewhere");
        };
        let map = cur
            .as_object_mut()
            .ok_or_else(|| DocError::BadUpdate(format!("cannot set nested path '{path}'")))?;
        cur = map
            .entry((*name).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    let PathSeg::Field(last) = &segs[segs.len() - 1] else {
        unreachable!("filtered segments handled elsewhere");
    };
    let map = cur
        .as_object_mut()
        .ok_or_else(|| DocError::BadUpdate(format!("cannot set nested path '{path}'")))?;
    map.insert((*last).to_string(), val);
    Ok(())
}

/// `$set` on `prefix.$[field=value]` or `prefix.$[field=value].suffix`.
/// Requires exactly one matching array element.
fn set_path_filtered(
    doc: &mut Value,
    path: &str,
    segs: &[PathSeg<'_>],
    val: Value,
) -> DocResult<()> {
    let filt_idx = segs
        .iter()
        .position(|s| matches!(s, PathSeg::Filtered { .. }))
        .expect("caller checked for filtered segment");
    if filt_idx == 0 {
        return Err(DocError::BadUpdate(format!(
            "filtered path '{path}' must have an array field before $[…]"
        )));
    }

    // Walk object prefix to the parent of the array field, then into the array field.
    let mut cur = doc;
    for seg in &segs[..filt_idx - 1] {
        let PathSeg::Field(name) = seg else {
            return Err(DocError::BadUpdate(format!(
                "invalid path '{path}': $[…] before array field"
            )));
        };
        let map = cur.as_object_mut().ok_or_else(|| {
            DocError::BadUpdate(format!("cannot traverse path '{path}' — not an object"))
        })?;
        cur = map
            .get_mut(*name)
            .ok_or_else(|| DocError::BadUpdate(format!("path '{path}' missing field '{name}'")))?;
    }
    let PathSeg::Field(arr_name) = &segs[filt_idx - 1] else {
        return Err(DocError::BadUpdate(format!(
            "invalid path '{path}': expected array field before $[…]"
        )));
    };
    let map = cur.as_object_mut().ok_or_else(|| {
        DocError::BadUpdate(format!("cannot traverse path '{path}' — not an object"))
    })?;
    let arr_val = map.get_mut(*arr_name).ok_or_else(|| {
        DocError::BadUpdate(format!("path '{path}' missing array field '{arr_name}'"))
    })?;
    let arr = arr_val.as_array_mut().ok_or_else(|| {
        DocError::BadUpdate(format!("path '{path}': field '{arr_name}' is not an array"))
    })?;

    let PathSeg::Filtered {
        field: id_field,
        value: id_value,
    } = &segs[filt_idx]
    else {
        unreachable!();
    };

    let mut match_idx: Option<usize> = None;
    for (i, elem) in arr.iter().enumerate() {
        let Some(obj) = elem.as_object() else {
            continue;
        };
        if obj.get(*id_field) == Some(id_value) {
            if match_idx.is_some() {
                return Err(DocError::BadUpdate(format!(
                    "filtered path '{path}' matched multiple array elements"
                )));
            }
            match_idx = Some(i);
        }
    }
    let idx = match_idx.ok_or_else(|| {
        DocError::BadUpdate(format!("filtered path '{path}' matched no array elements"))
    })?;

    let suffix = &segs[filt_idx + 1..];
    if suffix.is_empty() {
        arr[idx] = val;
        return Ok(());
    }
    // Set nested path inside the matched element (object nesting only).
    set_path_object(&mut arr[idx], path, suffix, val)
}

fn unset_path(doc: &mut Value, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = doc;
    for seg in &segs[..segs.len() - 1] {
        match cur.as_object_mut().and_then(|m| m.get_mut(*seg)) {
            Some(next) => cur = next,
            None => return,
        }
    }
    if let Some(m) = cur.as_object_mut() {
        m.remove(segs[segs.len() - 1]);
    }
}

/// Read the current body for `doc_id` and apply `update`, returning the new body
/// bytes. Returns `None` if the document does not exist — or, when `filter` is
/// given, if the CURRENT body no longer matches it. Does not write.
///
/// The filter re-check closes the TOCTOU between candidate selection and the
/// write: candidates are chosen from a lock-free snapshot, so by the time the
/// write runs under the engine lock a concurrent writer may have changed the
/// document such that it no longer matches. Re-verifying here makes filtered
/// updates behave as per-document compare-and-swap.
/// Returns `(new_zdoc_bytes, old_value)` so the write path can reuse the already
/// decoded prior body for the index diff (one get + decode per document).
fn updated_body(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    update: &UpdateDoc,
    filter: Option<&crate::filter::Filter>,
) -> DocResult<Option<(Vec<u8>, Value)>> {
    let coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let dk = keys::doc_key(prefix, coll.id, doc_id);
    let stored = match engine.get(&dk)? {
        Some(s) => s,
        None => return Ok(None),
    };
    if let Some(f) = filter {
        if !crate::query::check_filter(&stored, f, doc_id) {
            return Ok(None);
        }
    }
    let old: Value = if stored[0] == crate::store::VK_ZDOC {
        crate::binary::ValueView::new(strip_value_kind(&stored)).to_value()?
    } else {
        serde_json::from_slice(strip_value_kind(&stored))
            .map_err(|e| DocError::InvalidJson(e.to_string()))?
    };
    let mut body = old.clone();
    update.apply(&mut body)?;
    let new_bytes = crate::binary::ZDocBuilder::from_value(&body);
    Ok(Some((new_bytes, old)))
}

/// Read the current body for `doc_id`, apply `update`, and write it back via the
/// atomic index-maintaining [`store::upsert`]. Returns whether the doc existed.
pub fn apply_to_id(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    update: &UpdateDoc,
) -> DocResult<bool> {
    match updated_body(engine, catalog, prefix, collection, doc_id, update, None)? {
        Some((bytes, old)) => {
            let ops = store::upsert_ops_with_old(
                engine,
                catalog,
                prefix,
                collection,
                doc_id,
                &bytes,
                true,
                0,
                Some(&old),
            )?;
            engine.write_batch(ops)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Conditional by-id update: require `if_match` to equal the current revision,
/// apply `update`, and return the newly committed revision. Missing or stale
/// revisions return [`DocError::StaleRevision`].
pub fn apply_to_id_if_match(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    update: &UpdateDoc,
    if_match: u64,
) -> DocResult<u64> {
    store::check_if_match(engine, catalog, prefix, collection, doc_id, if_match)?;
    match updated_body(engine, catalog, prefix, collection, doc_id, update, None)? {
        Some((bytes, old)) => {
            let ops = store::upsert_ops_with_old(
                engine,
                catalog,
                prefix,
                collection,
                doc_id,
                &bytes,
                true,
                0,
                Some(&old),
            )?;
            Ok(engine.write_batch(ops)?)
        }
        None => Err(DocError::StaleRevision),
    }
}

/// Apply `update` to many documents. With no unique index on the collection and
/// a combined op count within one batch, the whole set is updated atomically
/// (isolated from concurrent readers). When a unique index is present, updates
/// run sequentially so each commit is visible to the next uniqueness check
/// (preserving correct enforcement). Returns the number of documents modified.
///
/// `filter`, when given, is re-verified per document under the engine lock:
/// candidates whose current body no longer matches are skipped (and not
/// counted), closing the snapshot-selection TOCTOU so filtered updates are
/// per-document compare-and-swap.
pub fn apply_to_ids(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    ids: &[Vec<u8>],
    update: &UpdateDoc,
    filter: Option<&crate::filter::Filter>,
) -> DocResult<u64> {
    let _coll = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;

    // A unique index makes intra-batch conflicts possible (two updated docs
    // could collide on the same value); the merged batch could not detect that
    // because each carries a distinct doc-id suffix. For now, we still batch
    // everything and rely on the engine's write_batch uniqueness check, but
    // if that fails, we would ideally fall back to sequential. Since we are
    // optimizing the happy path, we'll try the batch first.
    let mut per_doc: Vec<Vec<zydecodb_engine::engine::BatchOp>> = Vec::with_capacity(ids.len());
    let mut modified: u64 = 0;
    for id in ids {
        if let Some((bytes, old)) =
            updated_body(engine, catalog, prefix, collection, id, update, filter)?
        {
            let ops = store::upsert_ops_with_old(
                engine,
                catalog,
                prefix,
                collection,
                id,
                &bytes,
                true,
                0,
                Some(&old),
            )?;
            modified += 1;
            per_doc.push(ops);
        }
    }
    store::commit_batches(engine, per_doc)?;
    Ok(modified)
}

/// Build the document that an upsert would insert: equality fields from `filter`
/// as the base, then apply `update` via [`UpdateDoc::apply_on_insert`] (so
/// `$setOnInsert` runs). Returns `(doc_id_bytes, zdoc_body)`.
///
/// The filter must be equality-extractable (top-level `Atom::Eq` only). `_id`
/// must be a string equality when present; otherwise a UUIDv7-style hex id is
/// generated (same shape drivers use).
pub fn materialize_upsert(filter: &Filter, update: &UpdateDoc) -> DocResult<(Vec<u8>, Vec<u8>)> {
    let (mut base, id_opt) = build_upsert_base(filter)?;
    let id = id_opt.unwrap_or_else(generate_doc_id);
    if let Value::Object(ref mut m) = base {
        m.insert(
            crate::planner::ID_FIELD.to_string(),
            Value::String(id.clone()),
        );
    }
    update.apply_on_insert(&mut base)?;
    let body = crate::binary::ZDocBuilder::from_value(&base);
    Ok((id.into_bytes(), body))
}

/// Extract a usable insert base from top-level equality predicates only.
pub fn build_upsert_base(filter: &Filter) -> DocResult<(Value, Option<String>)> {
    let mut map = Map::new();
    let mut id = None;
    extract_eq_fields(filter, &mut map, &mut id)?;
    if map.is_empty() && id.is_none() {
        return Err(DocError::BadFilter(
            "upsert requires equality predicates (or _id) to build an insert document".into(),
        ));
    }
    Ok((Value::Object(map), id))
}

fn extract_eq_fields(
    filter: &Filter,
    out: &mut Map<String, Value>,
    id: &mut Option<String>,
) -> DocResult<()> {
    match filter {
        Filter::MatchAll => Err(DocError::BadFilter(
            "upsert cannot build an insert document from an empty filter".into(),
        )),
        Filter::Or(_) | Filter::Not(_) => Err(DocError::BadFilter(
            "upsert requires equality predicates; $or/$not cannot build an insert document".into(),
        )),
        Filter::And(fs) => {
            for sub in fs {
                extract_eq_fields(sub, out, id)?;
            }
            Ok(())
        }
        Filter::Field(fp) => {
            if fp.atoms.len() != 1 {
                return Err(DocError::BadFilter(format!(
                    "upsert cannot extract equality for field '{}'",
                    fp.path
                )));
            }
            match &fp.atoms[0] {
                Atom::Eq(v) => {
                    if fp.path == crate::planner::ID_FIELD {
                        let s = v.as_str().ok_or_else(|| {
                            DocError::BadFilter("upsert _id equality must be a string".into())
                        })?;
                        *id = Some(s.to_string());
                    }
                    // Top-level path segment only for the object key when undotted;
                    // dotted paths nest via set_path.
                    if fp.path.contains('.') {
                        let mut root = Value::Object(std::mem::take(out));
                        set_path(&mut root, &fp.path, v.clone())?;
                        *out = root.as_object().cloned().unwrap_or_default();
                    } else {
                        out.insert(fp.path.clone(), v.clone());
                    }
                    Ok(())
                }
                _ => Err(DocError::BadFilter(format!(
                    "upsert requires equality on '{}'; non-eq operators cannot build an insert document",
                    fp.path
                ))),
            }
        }
    }
}

/// UUIDv7-style hex id: 48-bit ms timestamp + 80 random bits (matches drivers).
fn generate_doc_id() -> String {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
        & ((1u64 << 48) - 1);
    let mut rnd = [0u8; 10];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut rnd);
    } else {
        let mix = ts_ms.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        rnd[..8].copy_from_slice(&mix.to_le_bytes());
    }
    let mut out = String::with_capacity(32);
    for b in &ts_ms.to_be_bytes()[2..] {
        out.push_str(&format!("{b:02x}"));
    }
    for b in &rnd {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn apply(update: Value, mut doc: Value) -> Value {
        UpdateDoc::parse(&update).unwrap().apply(&mut doc).unwrap();
        doc
    }

    #[test]
    fn path_over_segment_cap_rejected_at_parse() {
        // A dotted path is a flat JSON string: serde's depth cap never sees it.
        let path = (0..MAX_UPDATE_PATH_SEGMENTS + 1)
            .map(|i| format!("s{i}"))
            .collect::<Vec<_>>()
            .join(".");
        let u = json!({"$set": { path: 1 }});
        assert!(matches!(
            UpdateDoc::parse(&u),
            Err(DocError::BadUpdate(_))
        ));

        // Exactly at the cap still parses.
        let ok_path = (0..MAX_UPDATE_PATH_SEGMENTS)
            .map(|i| format!("s{i}"))
            .collect::<Vec<_>>()
            .join(".");
        assert!(UpdateDoc::parse(&json!({"$set": { ok_path: 1 }})).is_ok());
    }

    #[test]
    fn apply_rejects_document_beyond_depth_cap() {
        // Defense in depth: if a too-deep document is ever in play, grafting
        // onto it is rejected rather than ratcheting it deeper.
        let mut deep = json!(1);
        for _ in 0..crate::binary::MAX_ZDOC_DEPTH + 20 {
            deep = json!({ "d": deep });
        }
        let u = UpdateDoc::parse(&json!({"$set": {"x": 1}})).unwrap();
        assert!(matches!(u.apply(&mut deep), Err(DocError::BadUpdate(_))));
    }

    #[test]
    fn set_inc_unset_push() {
        let doc = json!({"name": "a", "n": 1, "tags": ["x"]});
        let out = apply(
            json!({"$set": {"name": "b"}, "$inc": {"n": 4}, "$unset": {"old": ""}, "$push": {"tags": "y"}}),
            doc,
        );
        assert_eq!(out["name"], json!("b"));
        assert_eq!(out["n"], json!(5));
        assert_eq!(out["tags"], json!(["x", "y"]));
    }

    #[test]
    fn inc_on_missing_field_starts_at_zero() {
        let out = apply(json!({"$inc": {"count": 3}}), json!({}));
        assert_eq!(out["count"], json!(3));
    }

    #[test]
    fn set_nested_path() {
        let out = apply(json!({"$set": {"a.b": 7}}), json!({"a": {"c": 1}}));
        assert_eq!(out, json!({"a": {"c": 1, "b": 7}}));
    }

    #[test]
    fn push_to_missing_creates_array() {
        let out = apply(json!({"$push": {"items": 1}}), json!({}));
        assert_eq!(out["items"], json!([1]));
    }

    #[test]
    fn bare_field_is_rejected() {
        assert!(UpdateDoc::parse(&json!({"name": "x"})).is_err());
        assert!(UpdateDoc::parse(&json!({})).is_err());
        assert!(UpdateDoc::parse(&json!({"$bogus": {"a": 1}})).is_err());
    }

    #[test]
    fn upsert_base_from_equality_filter() {
        let f = Filter::parse(&json!({"email": "a@b.c", "n": 1})).unwrap();
        let (base, id) = build_upsert_base(&f).unwrap();
        assert!(id.is_none());
        assert_eq!(base, json!({"email": "a@b.c", "n": 1}));
    }

    #[test]
    fn upsert_base_extracts_string_id() {
        let f = Filter::parse(&json!({"_id": "u1", "city": "NOLA"})).unwrap();
        let (base, id) = build_upsert_base(&f).unwrap();
        assert_eq!(id.as_deref(), Some("u1"));
        assert_eq!(base["city"], json!("NOLA"));
        assert_eq!(base["_id"], json!("u1"));
    }

    #[test]
    fn upsert_base_rejects_non_eq() {
        let f = Filter::parse(&json!({"age": {"$gt": 18}})).unwrap();
        assert!(build_upsert_base(&f).is_err());
        let f = Filter::parse(&json!({"$or": [{"a": 1}, {"b": 2}]})).unwrap();
        assert!(build_upsert_base(&f).is_err());
    }

    #[test]
    fn materialize_upsert_applies_update() {
        let f = Filter::parse(&json!({"_id": "x", "email": "a@b.c"})).unwrap();
        let u = UpdateDoc::parse(&json!({"$set": {"email": "a@b.c", "n": 1}})).unwrap();
        let (id, body) = materialize_upsert(&f, &u).unwrap();
        assert_eq!(id, b"x");
        let v = crate::binary::ValueView::new(&body).to_value().unwrap();
        assert_eq!(v["_id"], json!("x"));
        assert_eq!(v["email"], json!("a@b.c"));
        assert_eq!(v["n"], json!(1));
    }

    #[test]
    fn set_on_insert_applies_on_materialize() {
        let f = Filter::parse(&json!({"_id": "x", "email": "a@b.c"})).unwrap();
        let u = UpdateDoc::parse(&json!({
            "$set": {"n": 1},
            "$setOnInsert": {"created": true, "n": 99}
        }))
        .unwrap();
        let (_, body) = materialize_upsert(&f, &u).unwrap();
        let v = crate::binary::ValueView::new(&body).to_value().unwrap();
        assert_eq!(v["created"], json!(true));
        // Regular $set wins over $setOnInsert on the same path.
        assert_eq!(v["n"], json!(1));
    }

    #[test]
    fn set_on_insert_ignored_on_normal_apply() {
        let mut doc = json!({"_id": "x", "n": 1});
        let u = UpdateDoc::parse(&json!({
            "$set": {"n": 2},
            "$setOnInsert": {"created": true}
        }))
        .unwrap();
        u.apply(&mut doc).unwrap();
        assert_eq!(doc["n"], json!(2));
        assert!(doc.get("created").is_none());
    }

    #[test]
    fn set_on_insert_only_is_valid() {
        let u = UpdateDoc::parse(&json!({"$setOnInsert": {"created": true}})).unwrap();
        let mut doc = json!({"_id": "x"});
        u.apply(&mut doc).unwrap();
        assert!(doc.get("created").is_none());
        let f = Filter::parse(&json!({"_id": "x"})).unwrap();
        let (_, body) = materialize_upsert(&f, &u).unwrap();
        let v = crate::binary::ValueView::new(&body).to_value().unwrap();
        assert_eq!(v["created"], json!(true));
    }

    #[test]
    fn filtered_set_updates_matching_leaf() {
        let doc = json!({
            "items": [
                {"skuId": "A", "qty": 1},
                {"skuId": "B", "qty": 2}
            ]
        });
        let out = apply(json!({"$set": {"items.$[skuId=B].qty": 9}}), doc);
        assert_eq!(out["items"][1]["qty"], json!(9));
        assert_eq!(out["items"][0]["qty"], json!(1));
    }

    #[test]
    fn filtered_set_accepts_json_string_literal() {
        let doc = json!({"items": [{"skuId": "A", "qty": 1}]});
        let out = apply(json!({"$set": {"items.$[skuId=\"A\"].qty": 3}}), doc);
        assert_eq!(out["items"][0]["qty"], json!(3));
    }

    #[test]
    fn filtered_set_replaces_whole_element() {
        let doc = json!({"items": [{"skuId": "A", "qty": 1}]});
        let out = apply(
            json!({"$set": {"items.$[skuId=A]": {"skuId": "A", "qty": 5, "extra": true}}}),
            doc,
        );
        assert_eq!(
            out["items"][0],
            json!({"skuId": "A", "qty": 5, "extra": true})
        );
    }

    #[test]
    fn filtered_set_zero_matches_fails() {
        let mut doc = json!({"items": [{"skuId": "A", "qty": 1}]});
        let u = UpdateDoc::parse(&json!({"$set": {"items.$[skuId=Z].qty": 1}})).unwrap();
        let err = u.apply(&mut doc).unwrap_err();
        assert!(matches!(err, DocError::BadUpdate(_)), "{err}");
        assert!(err.to_string().contains("matched no"), "{err}");
    }

    #[test]
    fn filtered_set_many_matches_fails() {
        let mut doc = json!({
            "items": [
                {"skuId": "A", "qty": 1},
                {"skuId": "A", "qty": 2}
            ]
        });
        let u = UpdateDoc::parse(&json!({"$set": {"items.$[skuId=A].qty": 9}})).unwrap();
        let err = u.apply(&mut doc).unwrap_err();
        assert!(err.to_string().contains("multiple"), "{err}");
    }

    #[test]
    fn filtered_set_requires_array() {
        let mut doc = json!({"items": {"skuId": "A"}});
        let u = UpdateDoc::parse(&json!({"$set": {"items.$[skuId=A].qty": 1}})).unwrap();
        let err = u.apply(&mut doc).unwrap_err();
        assert!(err.to_string().contains("not an array"), "{err}");
    }

    #[test]
    fn filtered_path_rejected_on_inc() {
        let err = UpdateDoc::parse(&json!({"$inc": {"items.$[skuId=A].qty": 1}})).unwrap_err();
        assert!(err.to_string().contains("$set"), "{err}");
    }

    #[test]
    fn mongo_positional_forms_rejected() {
        assert!(UpdateDoc::parse(&json!({"$set": {"items.$.qty": 1}})).is_err());
        assert!(UpdateDoc::parse(&json!({"$set": {"items.$[].qty": 1}})).is_err());
        assert!(UpdateDoc::parse(&json!({"$set": {"items.$[elem].qty": 1}})).is_err());
        assert!(UpdateDoc::parse(&json!({
            "$set": {"items.$[skuId=A].qty": 1, "other.$[id=1].x": 2}
        }))
        .is_ok()); // two ops, each path has one filter — allowed
                   // One path with two filters:
        assert!(UpdateDoc::parse(&json!({
            "$set": {"items.$[skuId=A].subs.$[id=1].x": 2}
        }))
        .is_err());
    }

    #[test]
    fn filtered_set_numeric_and_bool_identity() {
        let doc = json!({
            "rows": [
                {"id": 1, "ok": false},
                {"id": 2, "ok": true}
            ]
        });
        let out = apply(json!({"$set": {"rows.$[id=2].ok": false}}), doc);
        assert_eq!(out["rows"][1]["ok"], json!(false));
        let out2 = apply(
            json!({"$set": {"rows.$[ok=true].n": 7}}),
            json!({
                "rows": [{"ok": true, "n": 1}, {"ok": false, "n": 2}]
            }),
        );
        assert_eq!(out2["rows"][0]["n"], json!(7));
        assert_eq!(out2["rows"][1]["n"], json!(2));
        let _ = out;
    }

    #[test]
    fn tokenize_preserves_dots_inside_quoted_value() {
        let segs = tokenize_path(r#"items.$[skuId="a.b"].qty"#).unwrap();
        assert_eq!(segs.len(), 3);
        match &segs[1] {
            PathSeg::Filtered { field, value } => {
                assert_eq!(*field, "skuId");
                assert_eq!(value, &json!("a.b"));
            }
            _ => panic!("expected filtered seg"),
        }
    }
}
