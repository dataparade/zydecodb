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

use crate::encoding;
use crate::error::{DocError, DocResult};
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
    pub fn matches(&self, doc: &Value) -> bool {
        match self {
            Filter::MatchAll => true,
            Filter::And(fs) => fs.iter().all(|f| f.matches(doc)),
            Filter::Or(fs) => fs.iter().any(|f| f.matches(doc)),
            Filter::Not(f) => !f.matches(doc),
            Filter::Field(fp) => fp.matches(doc),
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
    fn matches(&self, doc: &Value) -> bool {
        let field = lookup(doc, &self.path);
        self.atoms.iter().all(|atom| atom.matches(field))
    }
}

impl Atom {
    fn matches(&self, field: Option<&Value>) -> bool {
        match self {
            Atom::Exists(should) => field.is_some() == *should,
            Atom::Eq(target) => eq_match(field, target),
            Atom::Ne(target) => !eq_match(field, target),
            Atom::Gt(target) => ordering_match(field, target, &[Ordering::Greater]),
            Atom::Gte(target) => {
                ordering_match(field, target, &[Ordering::Greater, Ordering::Equal])
            }
            Atom::Lt(target) => ordering_match(field, target, &[Ordering::Less]),
            Atom::Lte(target) => ordering_match(field, target, &[Ordering::Less, Ordering::Equal]),
            Atom::In(list) => match field {
                Some(v) => list.iter().any(|t| value_eq(v, t)),
                None => list.iter().any(Value::is_null),
            },
            Atom::Nin(list) => match field {
                Some(v) => !list.iter().any(|t| value_eq(v, t)),
                None => !list.iter().any(Value::is_null),
            },
        }
    }
}

/// `$eq` semantics: a missing field matches only `$eq: null`.
fn eq_match(field: Option<&Value>, target: &Value) -> bool {
    match field {
        Some(v) => value_eq(v, target),
        None => target.is_null(),
    }
}

/// Ordering operators require both sides to be scalars; missing or non-scalar
/// fields never match.
fn ordering_match(field: Option<&Value>, target: &Value, accept: &[Ordering]) -> bool {
    match field.and_then(|v| cmp_scalar(v, target)) {
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

/// Compare two scalars using the index encoding order; None if either is an
/// array or object.
fn cmp_scalar(a: &Value, b: &Value) -> Option<Ordering> {
    if !is_scalar(a) || !is_scalar(b) {
        return None;
    }
    let mut ea = Vec::new();
    let mut eb = Vec::new();
    encoding::encode_value(a, &mut ea);
    encoding::encode_value(b, &mut eb);
    Some(ea.cmp(&eb))
}

/// Equality: scalars compare via the encoding order (so `1` == `1.0`);
/// arrays/objects compare structurally.
fn value_eq(a: &Value, b: &Value) -> bool {
    match cmp_scalar(a, b) {
        Some(ord) => ord == Ordering::Equal,
        None => a == b,
    }
}

/// Resolve a dotted path to the referenced value, or None if any segment is
/// missing or traverses a non-object.
fn lookup<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = doc;
    for seg in path.split('.') {
        match cur {
            Value::Object(map) => cur = map.get(seg)?,
            _ => return None,
        }
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: Value) -> Filter {
        Filter::parse(&v).unwrap()
    }

    #[test]
    fn empty_filter_matches_all() {
        assert!(parse(json!({})).matches(&json!({"a": 1})));
    }

    #[test]
    fn equality_and_implicit_and() {
        let f = parse(json!({"city": "NOLA", "age": 30}));
        assert!(f.matches(&json!({"city": "NOLA", "age": 30})));
        assert!(!f.matches(&json!({"city": "NOLA", "age": 31})));
        // int vs float equality
        assert!(f.matches(&json!({"city": "NOLA", "age": 30.0})));
    }

    #[test]
    fn comparison_operators() {
        let f = parse(json!({"age": {"$gt": 18, "$lt": 65}}));
        assert!(f.matches(&json!({"age": 30})));
        assert!(!f.matches(&json!({"age": 18})));
        assert!(!f.matches(&json!({"age": 65})));
        assert!(!f.matches(&json!({"age": 70})));
        // non-scalar / missing never match ordering
        assert!(!f.matches(&json!({"age": [1, 2]})));
        assert!(!f.matches(&json!({})));
    }

    #[test]
    fn in_nin_and_exists() {
        assert!(parse(json!({"c": {"$in": ["a", "b"]}})).matches(&json!({"c": "b"})));
        assert!(!parse(json!({"c": {"$in": ["a", "b"]}})).matches(&json!({"c": "z"})));
        assert!(parse(json!({"c": {"$nin": ["a"]}})).matches(&json!({"c": "z"})));
        assert!(parse(json!({"c": {"$exists": true}})).matches(&json!({"c": null})));
        assert!(!parse(json!({"c": {"$exists": true}})).matches(&json!({"d": 1})));
        assert!(parse(json!({"c": {"$exists": false}})).matches(&json!({"d": 1})));
    }

    #[test]
    fn ne_semantics() {
        let f = parse(json!({"status": {"$ne": "done"}}));
        assert!(f.matches(&json!({"status": "open"})));
        assert!(!f.matches(&json!({"status": "done"})));
        // missing field is treated as null, so $ne:"done" matches it
        assert!(f.matches(&json!({})));
    }

    #[test]
    fn logical_and_or_not() {
        let f = parse(json!({"$or": [{"a": 1}, {"b": 2}]}));
        assert!(f.matches(&json!({"a": 1})));
        assert!(f.matches(&json!({"b": 2})));
        assert!(!f.matches(&json!({"a": 9, "b": 9})));

        let f = parse(json!({"$not": {"a": 1}}));
        assert!(f.matches(&json!({"a": 2})));
        assert!(!f.matches(&json!({"a": 1})));
    }

    #[test]
    fn nested_path() {
        let f = parse(json!({"address.city": "London"}));
        assert!(f.matches(&json!({"address": {"city": "London"}})));
        assert!(!f.matches(&json!({"address": {"city": "Paris"}})));
        assert!(!f.matches(&json!({"address": "London"})));
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
