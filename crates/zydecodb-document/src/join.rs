//! `$lookup` execution: indexed nested-loop (INLJ) or bounded hash join,
//! chosen once per stage, under one snapshot.
//!
//! Strategy (value-independent, decided before the outer scan):
//! - usable inner index (`_id` or a leading-field index) → INLJ
//! - no index and `inner.doc_count <= max_scan_docs` → hash (one inner scan
//!   plus an in-memory map, capped by `max_hash_bytes`)
//! - otherwise → named error (create the index or shrink the inner side)
//!
//! There is no silent full-scan-per-document fallback. Both sides read under
//! one [`SnapshotHandle`]. Every bound rejects with a named error on exceed —
//! no spill, no silent truncation.

use crate::aggregation::{AggregationLimits, LookupSpec};
use crate::binary::{ValueView, ZDocBuilder};
use crate::catalog::CollectionMeta;
use crate::error::{DocError, DocResult};
use crate::filter::Filter;
use crate::planner::{self, AccessPath};
use crate::{encoding, keys, query, store};
use serde_json::Value;
use std::collections::HashMap;
use zydecodb_engine::SnapshotHandle;

/// Physical operator chosen for one `$lookup` stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Indexed nested-loop: one equality probe per outer document.
    Inlj,
    /// Bounded hash: one inner scan, then in-memory probes.
    Hash,
}

/// Output of a `$lookup` execution.
#[derive(Debug)]
pub struct JoinResult {
    /// Outer documents with the `as` array spliced in, in outer scan order.
    pub docs: Vec<Value>,
    /// Outer-side scan accounting.
    pub stats: query::MatchVisitStats,
    /// Peak estimated retained bytes for the joined documents.
    pub memory_bytes: usize,
    /// Operator that produced this result.
    pub strategy: JoinStrategy,
}

/// Conservative per-entry HashMap overhead: bucket slot + two Vec headers.
const HASH_ENTRY_OVERHEAD: usize = 48;

struct HashSide {
    map: HashMap<Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>>,
}

fn lookup_error(message: impl Into<String>) -> DocError {
    DocError::BadFilter(format!("lookup: {}", message.into()))
}

fn encode_join_key(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encoding::encode_value(v, &mut out);
    out
}

/// Choose the physical operator for this `$lookup`. Index presence is
/// value-independent; the hash path is gated on the catalog `doc_count`.
fn select_strategy(
    inner: &CollectionMeta,
    foreign_field: &str,
    limits: AggregationLimits,
) -> DocResult<JoinStrategy> {
    if planner::has_equality_index(inner, foreign_field) {
        return Ok(JoinStrategy::Inlj);
    }
    if inner.doc_count as usize <= limits.max_scan_docs {
        return Ok(JoinStrategy::Hash);
    }
    Err(lookup_error(format!(
        "no index on '{foreign_field}' for collection '{}'; create an index with '{foreign_field}' as the leading field (inner doc_count={} exceeds max_scan_docs={})",
        inner.name, inner.doc_count, limits.max_scan_docs
    )))
}

/// Extract the scalar join value at `local_field` from a stored outer body.
/// Missing or non-scalar values yield `None` (left-outer: empty `as` array).
fn local_join_value(stored: &[u8], local_field: &str) -> DocResult<Option<Value>> {
    let payload = store::strip_value_kind(stored);
    if stored.first() == Some(&store::VK_ZDOC) {
        let view = ValueView::new(payload);
        match view.get_path(local_field) {
            Some(v) => view_to_scalar(&v),
            None => Ok(None),
        }
    } else {
        let doc: Value = serde_json::from_slice(payload)
            .map_err(|e| DocError::Corrupt(format!("invalid stored JSON: {e}")))?;
        let zdoc = crate::binary::ZDocBuilder::from_value(&doc);
        let view = ValueView::new(&zdoc);
        match view.get_path(local_field) {
            Some(v) => view_to_scalar(&v),
            None => Ok(None),
        }
    }
}

/// Materialize a scalar `ValueView` as a JSON value; non-scalars → `None`.
fn view_to_scalar(v: &ValueView<'_>) -> DocResult<Option<Value>> {
    use crate::binary::*;
    Ok(match v.type_byte() {
        TYPE_NULL => Some(Value::Null),
        TYPE_BOOL_FALSE => Some(Value::Bool(false)),
        TYPE_BOOL_TRUE => Some(Value::Bool(true)),
        TYPE_I64 => Some(Value::Number(
            v.as_i64()
                .ok_or_else(|| DocError::Corrupt("truncated ZDoc integer".into()))?
                .into(),
        )),
        TYPE_F64 => {
            let f = v
                .as_f64()
                .ok_or_else(|| DocError::Corrupt("truncated ZDoc float".into()))?;
            if !f.is_finite() {
                return Ok(None);
            }
            serde_json::Number::from_f64(f).map(Value::Number)
        }
        TYPE_STRING => Some(Value::String(
            v.as_str()
                .ok_or_else(|| DocError::Corrupt("invalid ZDoc string".into()))?
                .to_string(),
        )),
        // Objects/arrays are not joinable scalars: left-outer empty array.
        _ => None,
    })
}

/// Decode a stored body to a JSON document with the virtual `_id` field
/// injected (same convention as the find path). ZDoc bodies go through
/// `ValueView::to_value()` directly — never JSON text — so f64 fields keep
/// their exact bits. Legacy raw-JSON bodies parse with the workspace
/// `float_roundtrip` decoder.
fn stored_to_doc(stored: &[u8], doc_id: &[u8]) -> DocResult<Value> {
    let kind = store::value_kind(stored)?;
    let payload = store::strip_value_kind(stored);
    let mut doc = if kind == store::VK_ZDOC {
        crate::binary::ValueView::new(payload)
            .to_value()
            .map_err(|e| {
                DocError::Corrupt(format!("document {}: {e}", String::from_utf8_lossy(doc_id)))
            })?
    } else {
        serde_json::from_slice(payload)
            .map_err(|e| DocError::Corrupt(format!("invalid stored JSON: {e}")))?
    };
    if let Value::Object(map) = &mut doc {
        map.entry(planner::ID_FIELD.to_string())
            .or_insert_with(|| Value::String(String::from_utf8_lossy(doc_id).into_owned()));
    }
    Ok(doc)
}

/// Fetch the inner documents matching one equality probe, bounded by
/// `max_matches_per_outer`. Documents rejected by the `$lookup` `filter` are
/// skipped before the bound is charged. Returns the surviving JSON bodies in
/// index order.
fn fetch_inner_matches(
    snap: &SnapshotHandle,
    prefix: &[u8],
    inner: &CollectionMeta,
    path: &AccessPath,
    filter: &Filter,
    max_matches: usize,
) -> DocResult<Vec<Value>> {
    let doc_prefix = keys::doc_prefix(prefix, inner.id);
    let mut docs: Vec<Value> = Vec::new();
    let push = |stored: &[u8], doc_id: &[u8], docs: &mut Vec<Value>| -> DocResult<()> {
        if docs.len() >= max_matches {
            return Err(lookup_error(format!(
                "more than {max_matches} matches for one outer document"
            )));
        }
        docs.push(stored_to_doc(stored, doc_id)?);
        Ok(())
    };
    let keep = |stored: &[u8], doc_id: &[u8]| -> DocResult<bool> {
        store::with_stored_view(stored, |v| Ok(filter.matches(v, Some(doc_id))))
    };
    match path {
        AccessPath::ById(id) => {
            let dk = keys::doc_key(prefix, inner.id, id);
            if let Some(stored) = snap.get(&dk)? {
                if keep(&stored, id)? {
                    push(&stored, id, &mut docs)?;
                }
            }
        }
        AccessPath::IndexScan { lo, hi, .. } => {
            let iter = snap.scan(lo.clone(), hi.clone())?;
            for item in iter {
                let (_ikey, doc_id) = item?;
                let mut dk = doc_prefix.clone();
                dk.extend_from_slice(&doc_id);
                // The index entry can be stale relative to the body (it cannot
                // under one snapshot, but a missing body is a skip, not a
                // corruption, for parity with the residual-check philosophy).
                if let Some(stored) = snap.get(&dk)? {
                    if keep(&stored, &doc_id)? {
                        push(&stored, &doc_id, &mut docs)?;
                    }
                }
            }
        }
        AccessPath::CollectionScan => {
            return Err(DocError::Corrupt(
                "lookup inner side planned as collection scan".into(),
            ));
        }
    }
    Ok(docs)
}

/// One bounded inner-collection scan into a hash map keyed by the same
/// scalar encoding the index would use. The `$lookup` `filter` plans and
/// residual-checks the scan, so rejected documents never enter the map or
/// count toward `max_hash_bytes`; they still count toward the `max_scan_docs`
/// candidate bound. Missing / non-scalar `foreignField` values are unjoinable
/// and skipped. Rejects the moment retained state would exceed
/// `max_hash_bytes`.
fn build_hash_side(
    snap: &SnapshotHandle,
    prefix: &[u8],
    inner: &CollectionMeta,
    spec: &LookupSpec,
    limits: AggregationLimits,
) -> DocResult<HashSide> {
    let mut map: HashMap<Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
    let mut hash_bytes = 0usize;
    query::visit_planned_matches_bounded_coll(
        snap,
        prefix,
        inner,
        &spec.filter,
        limits.max_scan_docs,
        |doc_id, stored| {
            let join_value = if spec.foreign_field == planner::ID_FIELD {
                Some(Value::String(String::from_utf8_lossy(&doc_id).into_owned()))
            } else {
                local_join_value(stored, &spec.foreign_field)?
            };
            let Some(v) = join_value else {
                return Ok(true);
            };
            let key = encode_join_key(&v);
            let stored_owned = stored.to_vec();
            let add = HASH_ENTRY_OVERHEAD
                + stored_owned.len()
                + doc_id.len()
                + if map.contains_key(&key) { 0 } else { key.len() };
            let next = hash_bytes
                .checked_add(add)
                .ok_or_else(|| lookup_error("hash memory accounting overflow"))?;
            if next > limits.max_hash_bytes {
                return Err(lookup_error(format!(
                    "hash join exceeds {} bytes",
                    limits.max_hash_bytes
                )));
            }
            hash_bytes = next;
            map.entry(key).or_default().push((doc_id, stored_owned));
            Ok(true)
        },
    )?;
    Ok(HashSide { map })
}

fn fetch_hash_matches(side: &HashSide, value: &Value, max_matches: usize) -> DocResult<Vec<Value>> {
    let key = encode_join_key(value);
    let Some(entries) = side.map.get(&key) else {
        return Ok(Vec::new());
    };
    if entries.len() > max_matches {
        return Err(lookup_error(format!(
            "more than {max_matches} matches for one outer document"
        )));
    }
    entries
        .iter()
        .map(|(id, stored)| stored_to_doc(stored, id))
        .collect()
}

fn join_value_for_outer(
    spec: &LookupSpec,
    doc_id: &[u8],
    stored: &[u8],
) -> DocResult<Option<Value>> {
    if spec.local_field == planner::ID_FIELD {
        Ok(Some(Value::String(
            String::from_utf8_lossy(doc_id).into_owned(),
        )))
    } else {
        local_join_value(stored, &spec.local_field)
    }
}

fn splice_matches(
    spec: &LookupSpec,
    post_filter: &Filter,
    stored: &[u8],
    doc_id: &[u8],
    matches: Vec<Value>,
    memory_bytes: &mut usize,
    max_memory_bytes: usize,
    docs: &mut Vec<Value>,
) -> DocResult<()> {
    let mut doc = stored_to_doc(stored, doc_id)?;
    match &mut doc {
        Value::Object(map) => {
            map.insert(spec.as_field.clone(), Value::Array(matches));
        }
        _ => return Err(DocError::Corrupt("stored document is not an object".into())),
    }
    // The post-join $match runs on the joined document. Rejected documents
    // are dropped before the memory bound is charged.
    if !matches!(post_filter, Filter::MatchAll) {
        let zdoc = ZDocBuilder::from_value(&doc);
        if !post_filter.matches(ValueView::new(&zdoc), Some(doc_id)) {
            return Ok(());
        }
    }
    let estimated = serde_json::to_vec(&doc).map(|v| v.len()).unwrap_or(0);
    let next_memory = memory_bytes
        .checked_add(estimated)
        .ok_or_else(|| lookup_error("memory accounting overflow"))?;
    if next_memory > max_memory_bytes {
        return Err(lookup_error(format!(
            "join state exceeds {max_memory_bytes} bytes"
        )));
    }
    *memory_bytes = next_memory;

    docs.push(doc);
    Ok(())
}

/// Execute a `$lookup` stage: pick INLJ or hash, stream the outer collection
/// (filtered by the pipeline's leading `$match`, bounded by `max_scan_docs`),
/// splice matches into the `as` array, then apply the optional post-join
/// `$match` to each joined document.
pub fn execute_lookup(
    snap: &SnapshotHandle,
    prefix: &[u8],
    outer: &CollectionMeta,
    inner: &CollectionMeta,
    filter: &Filter,
    spec: &LookupSpec,
    post_filter: &Filter,
    limits: AggregationLimits,
) -> DocResult<JoinResult> {
    let strategy = select_strategy(inner, &spec.foreign_field, limits)?;
    let hash_side = if strategy == JoinStrategy::Hash {
        Some(build_hash_side(snap, prefix, inner, spec, limits)?)
    } else {
        None
    };

    let mut docs: Vec<Value> = Vec::new();
    let mut memory_bytes = 0usize;

    let stats = query::visit_planned_matches_bounded_coll(
        snap,
        prefix,
        outer,
        filter,
        limits.max_scan_docs,
        |doc_id, stored| {
            let join_value = join_value_for_outer(spec, &doc_id, stored)?;
            // `_id` join values must be strings (ids are strings); any other
            // scalar can never match, so it is a left-outer empty array.
            let unmatchable = matches!(
                &join_value,
                Some(v) if spec.foreign_field == planner::ID_FIELD && !v.is_string()
            );
            let matches = match &join_value {
                None => Vec::new(),
                Some(_) if unmatchable => Vec::new(),
                Some(v) => match strategy {
                    JoinStrategy::Inlj => {
                        let path = planner::plan_equality(prefix, inner, &spec.foreign_field, v)?;
                        fetch_inner_matches(
                            snap,
                            prefix,
                            inner,
                            &path,
                            &spec.filter,
                            limits.max_matches_per_outer,
                        )?
                    }
                    JoinStrategy::Hash => fetch_hash_matches(
                        hash_side.as_ref().expect("hash side built"),
                        v,
                        limits.max_matches_per_outer,
                    )?,
                },
            };
            splice_matches(
                spec,
                post_filter,
                stored,
                &doc_id,
                matches,
                &mut memory_bytes,
                limits.max_memory_bytes,
                &mut docs,
            )?;
            Ok(true)
        },
    )?;

    Ok(JoinResult {
        docs,
        stats,
        memory_bytes,
        strategy,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored ZDoc f64 must survive every server read path with its exact
    /// bits: the join splice (`stored_to_doc`), the wire emit
    /// (`stored_to_json_vec`), and a JSON text re-parse (workspace serde_json
    /// uses `float_roundtrip`). The value below is one the default serde_json
    /// parser shifts by 1 ULP (verified: bits ...b9 parse as ...b8 without
    /// the feature).
    #[test]
    fn f64_bits_survive_stored_doc_and_json_text_roundtrip() {
        let f = 6.625958084293548e-77f64;
        let doc = serde_json::json!({"total": f, "name": "o1"});
        let zdoc = ZDocBuilder::from_value(&doc);
        let mut stored = vec![store::VK_ZDOC];
        stored.extend_from_slice(&zdoc);

        // Join splice path: no JSON text involved.
        let spliced = stored_to_doc(&stored, b"o1").unwrap();
        let got = spliced["total"].as_f64().unwrap();
        assert_eq!(got.to_bits(), f.to_bits(), "stored_to_doc shifted bits");
        assert_eq!(spliced["_id"], serde_json::json!("o1"));

        // Wire emit + client-side parse of that text.
        let text = store::stored_to_json_vec(&stored).unwrap();
        let reparsed: Value = serde_json::from_slice(&text).unwrap();
        let got = reparsed["total"].as_f64().unwrap();
        assert_eq!(
            got.to_bits(),
            f.to_bits(),
            "JSON text roundtrip shifted bits"
        );
    }
}
