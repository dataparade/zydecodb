//! Query filter language: a JSON filter document parsed into a predicate tree
//! and evaluated against a document.
//!
//! Surface:
//! - Field conditions: `{field: value}` (equality) or `{field: {$op: operand}}`.
//! - Comparison operators: `$eq $ne $gt $gte $lt $lte $in $nin $exists $type`.
//! - Array operators: `$all` `$elemMatch` (residual filter only).
//! - String: `$regex` (gated: max pattern length, `i` flag only, string fields).
//! - Logical: top-level multiple fields are implicitly ANDed; `$and`/`$or` take
//!   arrays of sub-filters; `$not` wraps a single sub-filter.
//! - Dotted paths (`"a.b.c"`) walk nested objects.
//!
//! Scalar comparisons reuse [`crate::encoding::encode_value`], so a filter sees
//! the exact same cross-type order (null < bool < number < string) that the
//! secondary indexes use. Equality on arrays/objects falls back to structural
//! JSON equality; ordering operators only apply to scalars.

use crate::binary::ValueView;
use crate::error::{DocError, DocResult};
use regex::Regex;
use serde_json::{Map, Value};
use std::cmp::Ordering;

/// Max `$regex` pattern length (bytes). Longer patterns are rejected at parse.
pub const MAX_REGEX_PATTERN_LEN: usize = 256;

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
#[derive(Debug, Clone)]
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
    /// BSON-ish type name (`"string"`, `"number"`, …).
    Type(&'static str),
    /// Field must be an array containing every operand (equality per element).
    All(Vec<Value>),
    /// Nested filter evaluated against each array element.
    ElemMatch(Filter),
    /// Compiled regex; matches string fields only.
    Regex {
        pattern: String,
        case_insensitive: bool,
        re: Regex,
    },
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Atom::Eq(a), Atom::Eq(b))
            | (Atom::Ne(a), Atom::Ne(b))
            | (Atom::Gt(a), Atom::Gt(b))
            | (Atom::Gte(a), Atom::Gte(b))
            | (Atom::Lt(a), Atom::Lt(b))
            | (Atom::Lte(a), Atom::Lte(b)) => a == b,
            (Atom::In(a), Atom::In(b))
            | (Atom::Nin(a), Atom::Nin(b))
            | (Atom::All(a), Atom::All(b)) => a == b,
            (Atom::Exists(a), Atom::Exists(b)) => a == b,
            (Atom::Type(a), Atom::Type(b)) => a == b,
            (Atom::ElemMatch(a), Atom::ElemMatch(b)) => a == b,
            (
                Atom::Regex {
                    pattern: pa,
                    case_insensitive: ia,
                    ..
                },
                Atom::Regex {
                    pattern: pb,
                    case_insensitive: ib,
                    ..
                },
            ) => pa == pb && ia == ib,
            _ => false,
        }
    }
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
            let mut regex_pat: Option<&Value> = None;
            let mut regex_opts: Option<&Value> = None;
            for (op, operand) in map {
                match op.as_str() {
                    "$regex" => regex_pat = Some(operand),
                    "$options" => regex_opts = Some(operand),
                    other => atoms.push(parse_atom(other, operand)?),
                }
            }
            if let Some(pat) = regex_pat {
                atoms.push(parse_regex(pat, regex_opts)?);
            } else if regex_opts.is_some() {
                return Err(DocError::BadFilter(
                    "$options requires a sibling $regex".into(),
                ));
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
        "$type" => Atom::Type(parse_type_name(operand)?),
        "$all" => Atom::All(arr()?),
        "$elemMatch" => {
            let obj = operand
                .as_object()
                .ok_or_else(|| DocError::BadFilter("$elemMatch requires a filter object".into()))?;
            Atom::ElemMatch(parse_obj(obj)?)
        }
        other => {
            return Err(DocError::BadFilter(format!(
                "unsupported operator '{other}'"
            )))
        }
    })
}

fn parse_type_name(operand: &Value) -> DocResult<&'static str> {
    let name = operand
        .as_str()
        .ok_or_else(|| DocError::BadFilter("$type requires a string type name".into()))?;
    Ok(match name {
        "string" => "string",
        "number" => "number",
        "bool" => "bool",
        "null" => "null",
        "array" => "array",
        "object" => "object",
        "binary" => "binary",
        other => return Err(DocError::BadFilter(format!("unsupported $type '{other}'"))),
    })
}

fn parse_regex(pattern_val: &Value, options_val: Option<&Value>) -> DocResult<Atom> {
    let pattern = pattern_val
        .as_str()
        .ok_or_else(|| DocError::BadFilter("$regex requires a string pattern".into()))?;
    if pattern.is_empty() {
        return Err(DocError::BadFilter(
            "$regex pattern must not be empty".into(),
        ));
    }
    if pattern.len() > MAX_REGEX_PATTERN_LEN {
        return Err(DocError::BadFilter(format!(
            "$regex pattern exceeds max length ({MAX_REGEX_PATTERN_LEN})"
        )));
    }
    if looks_like_nested_quantifier_bomb(pattern) {
        return Err(DocError::BadFilter(
            "$regex pattern rejected: nested quantifier complexity".into(),
        ));
    }
    let mut case_insensitive = false;
    if let Some(opts) = options_val {
        let s = opts
            .as_str()
            .ok_or_else(|| DocError::BadFilter("$options requires a string".into()))?;
        for ch in s.chars() {
            match ch {
                'i' => case_insensitive = true,
                other => {
                    return Err(DocError::BadFilter(format!(
                        "unsupported $regex option '{other}' (only 'i' is allowed)"
                    )))
                }
            }
        }
    }
    let re = Regex::new(&format!(
        "{}{}",
        if case_insensitive { "(?i)" } else { "" },
        pattern
    ))
    .map_err(|e| DocError::BadFilter(format!("invalid $regex: {e}")))?;
    Ok(Atom::Regex {
        pattern: pattern.to_string(),
        case_insensitive,
        re,
    })
}

/// Cheap rejection of classic nested-quantifier ReDoS shapes like `(a+)+`.
fn looks_like_nested_quantifier_bomb(pattern: &str) -> bool {
    let b = pattern.as_bytes();
    if b.len() < 3 {
        return false;
    }
    for i in 0..b.len() - 2 {
        // `)+)` / `*)*` — quantified group immediately closed again.
        if b[i] == b')' && matches!(b[i + 1], b'+' | b'*') && b[i + 2] == b')' {
            return true;
        }
        // `+)+` / `*)+` — quantifier inside a group that is itself quantified.
        if matches!(b[i], b'+' | b'*') && b[i + 1] == b')' && matches!(b[i + 2], b'+' | b'*') {
            return true;
        }
    }
    false
}

impl FieldPred {
    fn matches(&self, doc: ValueView<'_>, doc_id: Option<&[u8]>) -> bool {
        if self.path == crate::planner::ID_FIELD {
            if let Some(id_bytes) = doc_id {
                // Match against the id string directly — no per-candidate ZDoc.
                // Non-UTF8 ids use the same lossy conversion as before.
                let id_str = String::from_utf8_lossy(id_bytes);
                return self
                    .atoms
                    .iter()
                    .all(|atom| atom.matches_str(id_str.as_ref()));
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
            Atom::Gte(target) => {
                ordering_match(field, target, &[Ordering::Greater, Ordering::Equal])
            }
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
            Atom::Type(name) => type_match(field, name),
            Atom::All(operands) => match field.and_then(|v| v.as_array()) {
                Some(arr) => operands
                    .iter()
                    .all(|op| (0..arr.len()).any(|i| value_eq_view(arr.get(i).unwrap(), op))),
                None => false,
            },
            Atom::ElemMatch(sub) => match field.and_then(|v| v.as_array()) {
                Some(arr) => (0..arr.len()).any(|i| sub.matches(arr.get(i).unwrap(), None)),
                None => false,
            },
            Atom::Regex { re, .. } => match field.and_then(|v| v.as_str()) {
                Some(s) => re.is_match(s),
                None => false,
            },
        }
    }

    /// Match against a string id — same semantics as a TYPE_STRING ValueView.
    fn matches_str(&self, id: &str) -> bool {
        match self {
            Atom::Eq(target) => str_eq(id, target),
            Atom::Ne(target) => !str_eq(id, target),
            Atom::Gt(target) => str_ordering(id, target, &[Ordering::Greater]),
            Atom::Gte(target) => str_ordering(id, target, &[Ordering::Greater, Ordering::Equal]),
            Atom::Lt(target) => str_ordering(id, target, &[Ordering::Less]),
            Atom::Lte(target) => str_ordering(id, target, &[Ordering::Less, Ordering::Equal]),
            Atom::Exists(want) => *want,
            Atom::In(list) => list.iter().any(|t| str_eq(id, t)),
            Atom::Nin(list) => !list.iter().any(|t| str_eq(id, t)),
            Atom::Type(name) => *name == "string",
            Atom::All(_) | Atom::ElemMatch(_) => false,
            Atom::Regex { re, .. } => re.is_match(id),
        }
    }
}

/// `$eq` against a string field (id path).
fn str_eq(id: &str, target: &Value) -> bool {
    match target {
        Value::String(s) => s == id,
        _ => false,
    }
}

/// Ordering against a string field; mirrors [`cmp_scalar_view`] type tags
/// (null=0, bool=1, number=2, string=3).
fn str_ordering(id: &str, target: &Value, accept: &[Ordering]) -> bool {
    if !is_scalar(target) {
        return false;
    }
    let ord = match target {
        Value::Null => Ordering::Greater, // string(3) > null(0)
        Value::Bool(_) => Ordering::Greater,
        Value::Number(_) => Ordering::Greater,
        Value::String(s) => id.cmp(s.as_str()),
        _ => return false,
    };
    accept.contains(&ord)
}

fn type_match(field: Option<ValueView<'_>>, name: &str) -> bool {
    let Some(v) = field else {
        return false;
    };
    match name {
        "string" => v.type_byte() == crate::binary::TYPE_STRING,
        "number" => matches!(
            v.type_byte(),
            crate::binary::TYPE_I64 | crate::binary::TYPE_F64
        ),
        "bool" => matches!(
            v.type_byte(),
            crate::binary::TYPE_BOOL_FALSE | crate::binary::TYPE_BOOL_TRUE
        ),
        "null" => v.is_null(),
        "array" => v.type_byte() == crate::binary::TYPE_ARRAY,
        "object" => v.type_byte() == crate::binary::TYPE_OBJECT,
        // No binary value tag in ZDoc today.
        "binary" => false,
        _ => false,
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
fn ordering_match(
    field: Option<crate::binary::ValueView<'_>>,
    target: &Value,
    accept: &[Ordering],
) -> bool {
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
        if a_arr.len() != b_arr.len() {
            return false;
        }
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
        if a_obj.len() != b_obj.len() {
            return false;
        }
        for i in 0..a_obj.len() {
            let (k, v) = a_obj.get_at(i).unwrap();
            if let Some(bv) = b_obj.get(k) {
                if !value_eq_view(v, bv) {
                    return false;
                }
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
        assert!(check_matches(
            &parse(json!({"c": {"$in": ["a", "b"]}})),
            &json!({"c": "b"})
        ));
        assert!(!check_matches(
            &parse(json!({"c": {"$in": ["a", "b"]}})),
            &json!({"c": "z"})
        ));
        assert!(check_matches(
            &parse(json!({"c": {"$nin": ["a"]}})),
            &json!({"c": "z"})
        ));
        assert!(check_matches(
            &parse(json!({"c": {"$exists": true}})),
            &json!({"c": null})
        ));
        assert!(!check_matches(
            &parse(json!({"c": {"$exists": true}})),
            &json!({"d": 1})
        ));
        assert!(check_matches(
            &parse(json!({"c": {"$exists": false}})),
            &json!({"d": 1})
        ));
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
        assert!(Filter::parse(&json!({"a": {"$bogus": "x"}})).is_err());
        assert!(Filter::parse(&json!({"$weird": 1})).is_err());
    }

    #[test]
    fn type_all_elem_match() {
        assert!(check_matches(
            &parse(json!({"n": {"$type": "number"}})),
            &json!({"n": 3})
        ));
        assert!(!check_matches(
            &parse(json!({"n": {"$type": "string"}})),
            &json!({"n": 3})
        ));
        assert!(check_matches(
            &parse(json!({"tags": {"$all": ["a", "b"]}})),
            &json!({"tags": ["a", "b", "c"]})
        ));
        assert!(!check_matches(
            &parse(json!({"tags": {"$all": ["a", "z"]}})),
            &json!({"tags": ["a", "b"]})
        ));
        assert!(check_matches(
            &parse(json!({"items": {"$elemMatch": {"x": 1, "y": {"$gt": 0}}}})),
            &json!({"items": [{"x": 1, "y": 2}, {"x": 9, "y": 0}]})
        ));
        assert!(!check_matches(
            &parse(json!({"items": {"$elemMatch": {"x": 1, "y": {"$gt": 5}}}})),
            &json!({"items": [{"x": 1, "y": 2}]})
        ));
    }

    #[test]
    fn regex_gated() {
        let f = parse(json!({"name": {"$regex": "^ad", "$options": "i"}}));
        assert!(check_matches(&f, &json!({"name": "Ada"})));
        assert!(!check_matches(&f, &json!({"name": "Bo"})));
        // Non-string fields never match.
        assert!(!check_matches(&f, &json!({"name": 1})));
        assert!(Filter::parse(&json!({"a": {"$regex": ""}})).is_err());
        assert!(
            Filter::parse(&json!({"a": {"$regex": "x".repeat(MAX_REGEX_PATTERN_LEN + 1)}}))
                .is_err()
        );
        assert!(Filter::parse(&json!({"a": {"$regex": "(a+)+"}})).is_err());
        assert!(Filter::parse(&json!({"a": {"$regex": "x", "$options": "m"}})).is_err());
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

    #[test]
    fn id_field_matches_str_parity() {
        let body = crate::binary::ZDocBuilder::from_value(&json!({"x": 1}));
        let view = crate::binary::ValueView::new(&body);
        let id = b"doc-42";

        let eq = parse(json!({"_id": "doc-42"}));
        assert!(eq.matches(view, Some(id)));
        assert!(!parse(json!({"_id": "other"})).matches(view, Some(id)));

        let ne = parse(json!({"_id": {"$ne": "other"}}));
        assert!(ne.matches(view, Some(id)));
        assert!(!parse(json!({"_id": {"$ne": "doc-42"}})).matches(view, Some(id)));

        let inn = parse(json!({"_id": {"$in": ["a", "doc-42"]}}));
        assert!(inn.matches(view, Some(id)));
        assert!(!parse(json!({"_id": {"$in": ["a", "b"]}})).matches(view, Some(id)));

        let re = parse(json!({"_id": {"$regex": "^doc-"}}));
        assert!(re.matches(view, Some(id)));
        assert!(!parse(json!({"_id": {"$regex": "^x"}})).matches(view, Some(id)));

        // Lossy UTF-8: invalid bytes become U+FFFD; match against that string.
        let bad = b"a\xffb";
        let lossy = String::from_utf8_lossy(bad).into_owned();
        assert!(parse(json!({ "_id": lossy })).matches(view, Some(bad)));
    }
}
