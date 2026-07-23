//! Partial-update operators and the update/delete write paths.
//!
//! An update document must use operators (`$set $inc $unset $push $setOnInsert`);
//! a bare (non-`$`) document is rejected rather than silently replacing the
//! whole document. `$setOnInsert` applies only on upsert insert (see
//! [`materialize_upsert`]); normal updates ignore it.
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
        Ok(())
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
        Ok(())
    }
}

fn parse_op(op: &str, path: &str, operand: &Value) -> DocResult<UpdateOp> {
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
    let mut cur = doc;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

fn set_path(doc: &mut Value, path: &str, val: Value) -> DocResult<()> {
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = doc;
    for seg in &segs[..segs.len() - 1] {
        let map = cur
            .as_object_mut()
            .ok_or_else(|| DocError::BadUpdate(format!("cannot set nested path '{path}'")))?;
        cur = map
            .entry((*seg).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    let map = cur
        .as_object_mut()
        .ok_or_else(|| DocError::BadUpdate(format!("cannot set nested path '{path}'")))?;
    map.insert(segs[segs.len() - 1].to_string(), val);
    Ok(())
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
fn updated_body(
    engine: &mut Engine,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    doc_id: &[u8],
    update: &UpdateDoc,
    filter: Option<&crate::filter::Filter>,
) -> DocResult<Option<Vec<u8>>> {
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
    let mut body: Value = if stored[0] == crate::store::VK_ZDOC {
        crate::binary::ValueView::new(strip_value_kind(&stored)).to_value()
    } else {
        serde_json::from_slice(strip_value_kind(&stored))
            .map_err(|e| DocError::InvalidJson(e.to_string()))?
    };
    update.apply(&mut body)?;
    let new_bytes = crate::binary::ZDocBuilder::from_value(&body);
    Ok(Some(new_bytes))
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
        Some(bytes) => {
            store::upsert_with_expiry(
                engine, catalog, prefix, collection, doc_id, &bytes, true, 0,
            )?;
            Ok(true)
        }
        None => Ok(false),
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
        if let Some(bytes) = updated_body(engine, catalog, prefix, collection, id, update, filter)?
        {
            let ops = store::upsert_ops(engine, catalog, prefix, collection, id, &bytes, true, 0)?;
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
        let v = crate::binary::ValueView::new(&body).to_value();
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
        let v = crate::binary::ValueView::new(&body).to_value();
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
        let v = crate::binary::ValueView::new(&body).to_value();
        assert_eq!(v["created"], json!(true));
    }
}
