//! Query filter language: a Mongo-inspired JSON filter document parsed into a
//! predicate tree and evaluated against a document.
//!
//! Cleaned-up surface (vs MongoDB):
//! - Field conditions: `{field: value}` (equality) or `{field: {$op: operand}}`.
//! - Comparison operators: `$eq $ne $gt $gte $lt $lte $in $nin $exists`.
//! - Logical: top-level multiple fields are implicitly ANDed; `$and`/`$or` take
//!   arrays of sub-filters; `$not` wraps a single sub-filter (simpler than
//!   Mongo's per-field `$not`).
//! - Dotted paths (`"a.b.c"`) walk nested objects.
//!
//! Scalar comparisons reuse [`crate::encoding::encode_value`], so a filter sees
//! the exact same cross-type order (null < bool < number < string) that the
//! secondary indexes use. Equality on arrays/objects falls back to structural
//! JSON equality; ordering operators only apply to scalars.

use crate::error::{DocError, DocResult};
use crate::binary::ValueView;
use serde_json::{Map, Value};
use std::cmp::Ordering;

/// A parsed filter predicate tree.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Matches every document (empty filter `{}`).
    MatchAll,
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Field(FieldPred),
}

/// A predicate on one field path: a conjunction of atoms (e.g. `{$gt:1,$lt:9}`).
#[derive(Debug, Clone, PartialEq)]
pub struct FieldPred {
    pub path: String,
    pub atoms: Vec<Atom>,
}

/// A single comparison against a field's value.
#[derive(Debug, Clone, PartialEq)]
pub enum Atom {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
}

impl Filter {
    /// Parse a JSON filter document. `{}` becomes [`Filter::MatchAll`].
    pub fn parse(doc: &Value) -> DocResult<Filter> {
        let obj = doc
            .as_object()
            .ok_or_else(|| DocError::BadFilter("filter must be a JSON object".into()))?;
        parse_obj(obj)
    }

    /// Parse from raw JSON bytes.
    pub fn parse_bytes(bytes: &[u8]) -> DocResult<Filter> {
        if bytes.is_empty() {
            return Ok(Filter::MatchAll);
        }
        let v: Value =
            serde_json::from_slice(bytes).map_err(|e| DocError::BadFilter(e.to_string()))?;
        Filter::parse(&v)
    }

    /// Evaluate the filter against a document.
    pub fn matches(&self, doc: ValueView<'_>, doc_id: Option<&[u8]>) -> bool {
        match self {
            Filter::MatchAll => true,
            Filter::And(fs) => fs.iter().all(|f| f.matches(doc, doc_id)),
            Filter::Or(fs) => fs.iter().any(|f| f.matches(doc, doc_id)),
            Filter::Not(f) => !f.matches(doc, doc_id),
            Filter::Field(fp) => fp.matches(doc, doc_id),
        }
    }

    /// Field predicates that are ANDed at the top level (a bare field, or the
    /// conjuncts of a top-level `$and`). Used by the planner to find an index;
    /// `Or`/`Not`-rooted filters yield nothing and fall back to a scan.
    pub fn top_level_fields(&self) -> Vec<&FieldPred> {
        let mut out = Vec::new();
        collect_top_level(self, &mut out);
        out
    }
}

fn collect_top_level<'a>(f: &'a Filter, out: &mut Vec<&'a FieldPred>) {
    match f {
        Filter::Field(fp) => out.push(fp),
        Filter::And(fs) => {
            for sub in fs {
                collect_top_level(sub, out);
            }
        }
        _ => {}
    }
}

fn parse_obj(obj: &Map<String, Value>) -> DocResult<Filter> {
    let mut conjuncts: Vec<Filter> = Vec::new();
    for (key, val) in obj {
        match key.as_str() {
            "$and" => conjuncts.push(Filter::And(parse_filter_array(val, "$and")?)),
            "$or" => conjuncts.push(Filter::Or(parse_filter_array(val, "$or")?)),
            "$not" => conjuncts.push(Filter::Not(Box::new(Filter::parse(val)?))),
            field if field.starts_with('$') => {
                return Err(DocError::BadFilter(format!(
                    "unknown top-level operator '{field}'"
                )));
            }
            field => conjuncts.push(Filter::Field(parse_field(field, val)?)),
        }
    }
    Ok(match conjuncts.len() {
        0 => Filter::MatchAll,
        1 => conjuncts.into_iter().next().unwrap(),
        _ => Filter::And(conjuncts),
    })
}

fn parse_filter_array(val: &Value, op: &str) -> DocResult<Vec<Filter>> {
    let arr = val
        .as_array()
        .ok_or_else(|| DocError::BadFilter(format!("{op} requires an array of filters")))?;
    arr.iter().map(Filter::parse).collect()
}

fn parse_field(path: &str, val: &Value) -> DocResult<FieldPred> {
    // An object whose keys all start with '$' is an operator expression;
    // anything else (including a plain object) is an equality match.
    if let Value::Object(map) = val {
        if !map.is_empty() && map.keys().all(|k| k.starts_with('$')) {
            let mut atoms = Vec::with_capacity(map.len());
            for (op, operand) in map {
                atoms.push(parse_atom(op, operand)?);
            }
            return Ok(FieldPred {
                path: path.to_string(),
                atoms,
            });
        }
        if map.keys().any(|k| k.starts_with('$')) {
            return Err(DocError::BadFilter(format!(
                "field '{path}' mixes operators and plain keys"
            )));
        }
    }
    Ok(FieldPred {
        path: path.to_string(),
        atoms: vec![Atom::Eq(val.clone())],
    })
}

fn parse_atom(op: &str, operand: &Value) -> DocResult<Atom> {
    let arr = || -> DocResult<Vec<Value>> {
        operand
            .as_array()
            .map(|a| a.to_vec())
            .ok_or_else(|| DocError::BadFilter(format!("{op} requires an array")))
    };
    Ok(match op {
        "$eq" => Atom::Eq(operand.clone()),
        "$ne" => Atom::Ne(operand.clone()),
        "$gt" => Atom::Gt(operand.clone()),
        "$gte" => Atom::Gte(operand.clone()),
        "$lt" => Atom::Lt(operand.clone()),
        "$lte" => Atom::Lte(operand.clone()),
        "$in" => Atom::In(arr()?),
        "$nin" => Atom::Nin(arr()?),
        "$exists" => Atom::Exists(
            operand
                .as_bool()
                .ok_or_else(|| DocError::BadFilter("$exists requires a boolean".into()))?,
        ),
        other => {
            return Err(DocError::BadFilter(format!(
                "unsupported operator '{other}'"
            )))
        }
    })
}

impl FieldPred {
    fn matches(&self, doc: ValueView<'_>, doc_id: Option<&[u8]>) -> bool {
        if self.path == crate::planner::ID_FIELD {
            if let Some(id_bytes) = doc_id {
                let id_str = String::from_utf8_lossy(id_bytes).into_owned();
                let id_val = serde_json::Value::String(id_str);
                
                // We evaluate atoms against this Value
                // But atoms expect Option<ValueView>, which we can't easily make from an owned String without a ZDocBuilder
                // Let's just build a tiny ZDoc for the ID
                let temp_zdoc = crate::binary::ZDocBuilder::from_value(&id_val);
                let view = crate::binary::ValueView::new(&temp_zdoc);
                return self.atoms.iter().all(|atom| atom.matches(Some(view)));
            }
        }
        let field = doc.get_path(&self.path);
        self.atoms.iter().all(|atom| atom.matches(field))
    }
}

impl Atom {
    fn matches(&self, field: Option<ValueView<'_>>) -> bool {
        match self {
            Atom::Eq(target) => eq_match(field, target),
            Atom::Ne(target) => !eq_match(field, target),
            Atom::Gt(target) => ordering_match(field, target, &[Ordering::Greater]),
            Atom::Gte(target) => ordering_match(field, target, &[Ordering::Greater, Ordering::Equal]),
            Atom::Lt(target) => ordering_match(field, target, &[Ordering::Less]),
            Atom::Lte(target) => ordering_match(field, target, &[Ordering::Less, Ordering::Equal]),
            Atom::Exists(want) => field.is_some() == *want,
            Atom::In(list) => match field {
                Some(v) => list.iter().any(|t| value_eq_view(v, t)),
                None => list.iter().any(Value::is_null),
            },
            Atom::Nin(list) => match field {
                Some(v) => !list.iter().any(|t| value_eq_view(v, t)),
                None => !list.iter().any(Value::is_null),
            },
        }
    }
}

/// `$eq` semantics: a missing field matches only `$eq: null`.
fn eq_match(field: Option<crate::binary::ValueView<'_>>, target: &Value) -> bool {
    match field {
        Some(v) => value_eq_view(v, target),
        None => target.is_null(),
    }
}

/// Ordering operators require both sides to be scalars; missing or non-scalar
/// fields never match.
fn ordering_match(field: Option<crate::binary::ValueView<'_>>, target: &Value, accept: &[Ordering]) -> bool {
    match field.and_then(|v| cmp_scalar_view(v, target)) {
        Some(ord) => accept.contains(&ord),
        None => false,
    }
}

fn is_scalar(v: &Value) -> bool {
    matches!(
        v,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn cmp_scalar_view(a: crate::binary::ValueView<'_>, b: &Value) -> Option<Ordering> {
    if !is_scalar(b) {
        return None;
    }
    
    let a_type = match a.type_byte() {
        crate::binary::TYPE_NULL => 0,
        crate::binary::TYPE_BOOL_FALSE | crate::binary::TYPE_BOOL_TRUE => 1,
        crate::binary::TYPE_I64 | crate::binary::TYPE_F64 => 2,
        crate::binary::TYPE_STRING => 3,
        _ => return None,
    };
    
    let b_type = match b {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        _ => return None,
    };
    
    if a_type != b_type {
        return Some(a_type.cmp(&b_type));
    }
    
    match a_type {
        0 => Some(Ordering::Equal),
        1 => {
            let ab = a.as_bool().unwrap();
            let bb = b.as_bool().unwrap();
            Some(ab.cmp(&bb))
        }
        2 => {
            let af = a.as_f64().unwrap();
            let bf = b.as_f64().unwrap();
            af.partial_cmp(&bf)
        }
        3 => {
            let a_str = a.as_str().unwrap();
            let b_str = b.as_str().unwrap();
            Some(a_str.cmp(b_str))
        }
        _ => None,
    }
}

fn value_eq_view(a: crate::binary::ValueView<'_>, b: &Value) -> bool {
    if a.type_byte() == crate::binary::TYPE_ARRAY && b.is_array() {
        let a_arr = a.as_array().unwrap();
        let b_arr = b.as_array().unwrap();
        if a_arr.len() != b_arr.len() { return false; }
        for i in 0..a_arr.len() {
            if !value_eq_view(a_arr.get(i).unwrap(), &b_arr[i]) {
                return false;
            }
        }
        return true;
    }
    if a.type_byte() == crate::binary::TYPE_OBJECT && b.is_object() {
        let a_obj = a.as_object().unwrap();
        let b_obj = b.as_object().unwrap();
        if a_obj.len() != b_obj.len() { return false; }
        for i in 0..a_obj.len() {
            let (k, v) = a_obj.get_at(i).unwrap();
            if let Some(bv) = b_obj.get(k) {
                if !value_eq_view(v, bv) { return false; }
            } else {
                return false;
            }
        }
        return true;
    }
    
    match cmp_scalar_view(a, b) {
        Some(Ordering::Equal) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check_matches(f: &Filter, val: &Value) -> bool {
        let bytes = crate::binary::ZDocBuilder::from_value(val);
        f.matches(crate::binary::ValueView::new(&bytes), None)
    }

    fn parse(v: Value) -> Filter {
        Filter::parse(&v).unwrap()
    }

    #[test]
    fn empty_filter_matches_all() {
        assert!(check_matches(&parse(json!({})), &json!({"a": 1})));
    }

    #[test]
    fn equality_and_implicit_and() {
        let f = parse(json!({"city": "NOLA", "age": 30}));
        assert!(check_matches(&f, &json!({"city": "NOLA", "age": 30})));
        assert!(!check_matches(&f, &json!({"city": "NOLA", "age": 31})));
        // int vs float equality
        assert!(check_matches(&f, &json!({"city": "NOLA", "age": 30.0})));
    }

    #[test]
    fn comparison_operators() {
        let f = parse(json!({"age": {"$gt": 18, "$lt": 65}}));
        assert!(check_matches(&f, &json!({"age": 30})));
        assert!(!check_matches(&f, &json!({"age": 18})));
        assert!(!check_matches(&f, &json!({"age": 65})));
        assert!(!check_matches(&f, &json!({"age": 70})));
        // non-scalar / missing never match ordering
        assert!(!check_matches(&f, &json!({"age": [1, 2]})));
        assert!(!check_matches(&f, &json!({})));
    }

    #[test]
    fn in_nin_and_exists() {
        assert!(check_matches(&parse(json!({"c": {"$in": ["a", "b"]}})), &json!({"c": "b"})));
        assert!(!check_matches(&parse(json!({"c": {"$in": ["a", "b"]}})), &json!({"c": "z"})));
        assert!(check_matches(&parse(json!({"c": {"$nin": ["a"]}})), &json!({"c": "z"})));
        assert!(check_matches(&parse(json!({"c": {"$exists": true}})), &json!({"c": null})));
        assert!(!check_matches(&parse(json!({"c": {"$exists": true}})), &json!({"d": 1})));
        assert!(check_matches(&parse(json!({"c": {"$exists": false}})), &json!({"d": 1})));
    }

    #[test]
    fn ne_semantics() {
        let f = parse(json!({"status": {"$ne": "done"}}));
        assert!(check_matches(&f, &json!({"status": "open"})));
        assert!(!check_matches(&f, &json!({"status": "done"})));
        // missing field is treated as null, so $ne:"done" matches it
        assert!(check_matches(&f, &json!({})));
    }

    #[test]
    fn logical_and_or_not() {
        let f = parse(json!({"$or": [{"a": 1}, {"b": 2}]}));
        assert!(check_matches(&f, &json!({"a": 1})));
        assert!(check_matches(&f, &json!({"b": 2})));
        assert!(!check_matches(&f, &json!({"a": 9, "b": 9})));

        let f = parse(json!({"$not": {"a": 1}}));
        assert!(check_matches(&f, &json!({"a": 2})));
        assert!(!check_matches(&f, &json!({"a": 1})));
    }

    #[test]
    fn nested_path() {
        let f = parse(json!({"address.city": "London"}));
        assert!(check_matches(&f, &json!({"address": {"city": "London"}})));
        assert!(!check_matches(&f, &json!({"address": {"city": "Paris"}})));
        assert!(!check_matches(&f, &json!({"address": "London"})));
    }

    #[test]
    fn unknown_operator_is_rejected() {
        assert!(Filter::parse(&json!({"a": {"$regex": "x"}})).is_err());
        assert!(Filter::parse(&json!({"$weird": 1})).is_err());
    }

    #[test]
    fn top_level_fields_for_planner() {
        let f = parse(json!({"age": {"$gte": 18}, "city": "NOLA"}));
        let fields = f.top_level_fields();
        assert_eq!(fields.len(), 2);
        // Or-rooted yields nothing usable for a single index.
        let f = parse(json!({"$or": [{"a": 1}, {"b": 2}]}));
        assert!(f.top_level_fields().is_empty());
    }
}
