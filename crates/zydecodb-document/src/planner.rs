//! Query planner: choose an access path from a filter and a collection's
//! indexes.
//!
//! The planner only affects **speed, never correctness**: every query
//! re-evaluates the full filter against the materialized document (the
//! "residual" check in [`crate::query::execute_find`]). An access path only
//! narrows the candidate set, so any chosen range is a safe *superset* of the
//! matching documents and a [`AccessPath::CollectionScan`] is always valid.
//!
//! Selection order:
//! 1. `_id` equality (string) -> [`AccessPath::ById`] (a direct doc-key get;
//!    `_id` behaves as a virtual always-present index).
//! 2. Otherwise, the index with the longest leading equality-prefix match
//!    (plus an optional range on the next field); ties resolve to the
//!    first-defined index.
//! 3. No usable index -> [`AccessPath::CollectionScan`].

use crate::catalog::CollectionMeta;
use crate::encoding;
use crate::filter::{Atom, Filter};
use crate::keys;
use serde_json::Value;
use std::collections::HashMap;

/// The virtual field whose value is the document id (its storage key).
pub const ID_FIELD: &str = "_id";

/// A resolved access path. Index/by-id paths carry full key material so the
/// executor can scan or fetch directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessPath {
    /// Direct document-key lookup by id bytes.
    ById(Vec<u8>),
    /// Range scan over one index: `[lo, hi)` full index keys.
    IndexScan {
        lo: Vec<u8>,
        hi: Vec<u8>,
        /// Index field paths, in order — lets the executor decide whether the
        /// scan order already satisfies a requested sort (key-cursor mode).
        fields: Vec<String>,
    },
    /// Full scan over the collection's document range.
    CollectionScan,
}

/// Per-field constraints distilled from the top-level conjunction.
#[derive(Default)]
struct Constraint {
    eq: Option<Value>,
    lo: Option<Value>,
    hi: Option<Value>,
}

impl Constraint {
    fn has_range(&self) -> bool {
        self.lo.is_some() || self.hi.is_some()
    }
}

fn is_scalar(v: &Value) -> bool {
    matches!(
        v,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

/// Collect equality/range constraints for fields ANDed at the top level.
fn collect_constraints(filter: &Filter) -> HashMap<String, Constraint> {
    let mut map: HashMap<String, Constraint> = HashMap::new();
    for fp in filter.top_level_fields() {
        let c = map.entry(fp.path.clone()).or_default();
        for atom in &fp.atoms {
            match atom {
                Atom::Eq(v) if is_scalar(v) => c.eq = Some(v.clone()),
                Atom::Gt(v) | Atom::Gte(v) if is_scalar(v) => c.lo = Some(v.clone()),
                Atom::Lt(v) | Atom::Lte(v) if is_scalar(v) => c.hi = Some(v.clone()),
                _ => {}
            }
        }
    }
    map
}

/// Plan an access path for `filter` over `coll`'s indexes.
pub fn plan(filter: &Filter, prefix: &[u8], coll: &CollectionMeta) -> AccessPath {
    let constraints = collect_constraints(filter);

    // 1. `_id` equality fast path (string ids only).
    if let Some(c) = constraints.get(ID_FIELD) {
        if let Some(Value::String(s)) = &c.eq {
            return AccessPath::ById(s.as_bytes().to_vec());
        }
    }

    // 2. Best index by longest equality prefix, then by having a range.
    let mut best: Option<(usize, bool, &crate::catalog::IndexMeta)> = None;
    for idx in &coll.indexes {
        let mut eq_len = 0usize;
        while eq_len < idx.fields.len() {
            match constraints.get(&idx.fields[eq_len]) {
                Some(c) if c.eq.is_some() => eq_len += 1,
                _ => break,
            }
        }
        let has_range = idx
            .fields
            .get(eq_len)
            .and_then(|f| constraints.get(f))
            .map(Constraint::has_range)
            .unwrap_or(false);
        let usable = eq_len + has_range as usize;
        if usable == 0 {
            continue;
        }
        let better = match best {
            None => true,
            Some((be, br, _)) => (eq_len, has_range) > (be, br),
        };
        if better {
            best = Some((eq_len, has_range, idx));
        }
    }

    match best {
        Some((eq_len, _, idx)) => build_index_scan(prefix, coll.id, idx, eq_len, &constraints),
        None => AccessPath::CollectionScan,
    }
}

fn build_index_scan(
    prefix: &[u8],
    collection_id: u32,
    idx: &crate::catalog::IndexMeta,
    eq_len: usize,
    constraints: &HashMap<String, Constraint>,
) -> AccessPath {
    let iprefix = keys::index_prefix(prefix, collection_id, idx.id);

    // base = iprefix + encode(eq values for the leading fields)
    let eq_vals: Vec<Value> = idx.fields[..eq_len]
        .iter()
        .map(|f| constraints[f].eq.clone().unwrap())
        .collect();
    let mut base = iprefix.clone();
    base.extend_from_slice(&encoding::encode_fields(&eq_vals));

    let range = idx.fields.get(eq_len).and_then(|f| constraints.get(f));

    let (lo, hi) = match range {
        Some(c) if c.has_range() => {
            let mut lo = base.clone();
            if let Some(lv) = &c.lo {
                encoding::encode_value(lv, &mut lo);
            }
            let hi = match &c.hi {
                Some(hv) => {
                    let mut h = base.clone();
                    encoding::encode_value(hv, &mut h);
                    keys::prefix_upper_bound(&h)
                }
                None => keys::prefix_upper_bound(&base),
            };
            (lo, hi)
        }
        _ => (base.clone(), keys::prefix_upper_bound(&base)),
    };

    AccessPath::IndexScan {
        lo,
        hi,
        fields: idx.fields.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CollectionMeta, IndexMeta};
    use serde_json::json;

    fn coll_with(indexes: Vec<IndexMeta>) -> CollectionMeta {
        CollectionMeta {
            id: 1,
            prefix: b"\x01".to_vec(),
            name: "users".into(),
            indexes,
        }
    }

    fn idx(id: u32, name: &str, fields: &[&str]) -> IndexMeta {
        IndexMeta {
            id,
            name: name.into(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
            unique: false,
            expire_after_seconds: None,
        }
    }

    fn plan_for(v: serde_json::Value, coll: &CollectionMeta) -> AccessPath {
        plan(&Filter::parse(&v).unwrap(), b"\x01", coll)
    }

    #[test]
    fn id_equality_uses_by_id() {
        let coll = coll_with(vec![]);
        assert_eq!(
            plan_for(json!({"_id": "abc"}), &coll),
            AccessPath::ById(b"abc".to_vec())
        );
    }

    #[test]
    fn no_index_falls_back_to_scan() {
        let coll = coll_with(vec![]);
        assert_eq!(
            plan_for(json!({"name": "x"}), &coll),
            AccessPath::CollectionScan
        );
    }

    #[test]
    fn single_field_equality_uses_index() {
        let coll = coll_with(vec![idx(0, "by_age", &["age"])]);
        match plan_for(json!({"age": 30}), &coll) {
            AccessPath::IndexScan { lo, hi, fields } => {
                assert!(lo < hi);
                assert_eq!(fields, vec!["age".to_string()]);
            }
            other => panic!("expected index scan, got {other:?}"),
        }
    }

    #[test]
    fn range_query_uses_index() {
        let coll = coll_with(vec![idx(0, "by_age", &["age"])]);
        match plan_for(json!({"age": {"$gte": 18, "$lt": 65}}), &coll) {
            AccessPath::IndexScan { lo, hi, .. } => assert!(lo < hi),
            other => panic!("expected index scan, got {other:?}"),
        }
    }

    #[test]
    fn compound_prefix_match_prefers_longer_equality() {
        let coll = coll_with(vec![
            idx(0, "by_city", &["city"]),
            idx(1, "by_city_age", &["city", "age"]),
        ]);
        // Both city and age constrained by equality -> the 2-field index wins.
        match plan_for(json!({"city": "NOLA", "age": 30}), &coll) {
            AccessPath::IndexScan { fields, .. } => {
                assert_eq!(fields, vec!["city".to_string(), "age".to_string()]);
            }
            other => panic!("expected index scan, got {other:?}"),
        }
    }

    #[test]
    fn index_not_leading_filter_falls_back() {
        // Index is on `age`, but the filter only constrains `name`.
        let coll = coll_with(vec![idx(0, "by_age", &["age"])]);
        assert_eq!(
            plan_for(json!({"name": "x"}), &coll),
            AccessPath::CollectionScan
        );
    }

    #[test]
    fn or_rooted_filter_falls_back() {
        let coll = coll_with(vec![idx(0, "by_age", &["age"])]);
        assert_eq!(
            plan_for(json!({"$or": [{"age": 1}, {"age": 2}]}), &coll),
            AccessPath::CollectionScan
        );
    }
}
