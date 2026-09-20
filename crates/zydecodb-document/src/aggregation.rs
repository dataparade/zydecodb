//! Bounded minimal aggregation: optional `$match`, followed by one `$group`.
//!
//! Execution streams a planner-selected candidate set and retains only bounded
//! per-group accumulator state. It never materializes the matching documents.

use crate::binary::{
    ValueView, ZDocBuilder, TYPE_ARRAY, TYPE_BOOL_FALSE, TYPE_BOOL_TRUE, TYPE_F64, TYPE_I64,
    TYPE_NULL, TYPE_OBJECT, TYPE_STRING,
};
use crate::catalog::Catalog;
use crate::error::{DocError, DocResult};
use crate::filter::Filter;
use crate::{encoding, query, store};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::mem::size_of;
use zydecodb_engine::SnapshotHandle;

pub const MAX_PIPELINE_STAGES: usize = 4;
pub const MAX_ACCUMULATORS: usize = 16;
pub const MAX_PIPELINE_BYTES: usize = 64 * 1024;

pub const DEFAULT_MAX_SCAN_DOCS: usize = 100_000;
pub const DEFAULT_MAX_GROUPS: usize = 10_000;
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_MATCHES_PER_OUTER: usize = 1_000;
pub const DEFAULT_MAX_HASH_BYTES: usize = 16 * 1024 * 1024;

/// Per-request resource bounds for aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationLimits {
    /// Maximum planner candidates examined, counted before residual filtering.
    pub max_scan_docs: usize,
    /// Maximum distinct group keys retained.
    pub max_groups: usize,
    /// Maximum estimated bytes retained by group keys and accumulator states.
    pub max_memory_bytes: usize,
    /// Wire-layer result limit. The core reports `result_bytes` but deliberately
    /// leaves enforcement to the eventual response encoder.
    pub max_result_bytes: usize,
    /// Maximum inner documents attached to one outer document by `$lookup`.
    pub max_matches_per_outer: usize,
    /// Maximum estimated bytes retained by a hash-join build (inner side).
    pub max_hash_bytes: usize,
}

impl Default for AggregationLimits {
    fn default() -> Self {
        Self {
            max_scan_docs: DEFAULT_MAX_SCAN_DOCS,
            max_groups: DEFAULT_MAX_GROUPS,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            max_matches_per_outer: DEFAULT_MAX_MATCHES_PER_OUTER,
            max_hash_bytes: DEFAULT_MAX_HASH_BYTES,
        }
    }
}

/// Parsed, validated aggregation pipeline.
///
/// Legal shapes (see `docs/DESIGN-joins.md`):
/// `[$group]`, `[$match, $group]`, `[$lookup]`, `[$lookup, $group]`,
/// `[$lookup, $match]`, `[$lookup, $match, $group]`, `[$match, $lookup]`,
/// `[$match, $lookup, $group]`, `[$match, $lookup, $match]`,
/// `[$match, $lookup, $match, $group]`. `$lookup` appears at most once and
/// never after `$group`; a `$match` after `$lookup` runs on the joined
/// documents (outer plus the `as` array).
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationPipeline {
    pub filter: Filter,
    pub lookup: Option<LookupSpec>,
    pub post_filter: Filter,
    pub group: Option<GroupSpec>,
}

impl AggregationPipeline {
    /// Parse the strict minimal pipeline grammar from JSON bytes.
    pub fn parse(bytes: &[u8]) -> DocResult<Self> {
        parse_pipeline(bytes)
    }
}

/// A parsed `$lookup` stage: equality left-outer join against one collection
/// under the same tenant prefix, with an optional residual filter applied to
/// inner documents before they are attached.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupSpec {
    pub from: String,
    pub local_field: String,
    pub foreign_field: String,
    pub as_field: String,
    pub filter: Filter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpec {
    pub id: GroupId,
    pub accumulators: Vec<AccumulatorSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupId {
    Null,
    Field(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccumulatorSpec {
    pub output_field: String,
    pub op: AccumulatorOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccumulatorOp {
    Sum(String),
    Count,
    Size(String),
}

/// Completed aggregation rows and accounting needed by a future wire encoder.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationResult {
    /// Group documents in deterministic scalar-encoding order.
    pub rows: Vec<Value>,
    /// Planner candidates examined before residual filtering.
    pub scanned_docs: usize,
    /// Documents accepted by the complete filter.
    pub matched_docs: usize,
    /// Peak estimated retained group-state bytes.
    pub memory_bytes: usize,
    /// JSON byte length of `rows` serialized as one array.
    pub result_bytes: usize,
}

fn bad_aggregation(message: impl Into<String>) -> DocError {
    DocError::BadFilter(format!("aggregation: {}", message.into()))
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path
            .split('.')
            .all(|segment| !segment.is_empty() && !segment.starts_with('$'))
}

fn parse_field_reference(value: &Value, context: &str) -> DocResult<String> {
    let raw = value
        .as_str()
        .ok_or_else(|| bad_aggregation(format!("{context} must be a '$path' string")))?;
    let path = raw
        .strip_prefix('$')
        .ok_or_else(|| bad_aggregation(format!("{context} must start with '$'")))?;
    if !valid_path(path) {
        return Err(bad_aggregation(format!(
            "{context} must contain a valid dotted path"
        )));
    }
    Ok(path.to_string())
}

/// Parse the strict pipeline grammar: optional leading `$match`, optional
/// `$lookup`, optional post-join `$match`, optional `$group` — at least one
/// stage, `$lookup` at most once and never after `$group`, `$group` last, and
/// a `$lookup`-free pipeline must end in `$group`.
pub fn parse_pipeline(bytes: &[u8]) -> DocResult<AggregationPipeline> {
    if bytes.len() > MAX_PIPELINE_BYTES {
        return Err(bad_aggregation(format!(
            "pipeline exceeds {MAX_PIPELINE_BYTES} bytes"
        )));
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| bad_aggregation(format!("invalid JSON: {e}")))?;
    let stages = value
        .as_array()
        .ok_or_else(|| bad_aggregation("pipeline must be a JSON array"))?;
    if stages.is_empty() || stages.len() > MAX_PIPELINE_STAGES {
        return Err(bad_aggregation(format!(
            "pipeline must have 1..={MAX_PIPELINE_STAGES} stages"
        )));
    }

    let mut filter = Filter::MatchAll;
    let mut lookup: Option<LookupSpec> = None;
    let mut post_filter = Filter::MatchAll;
    let mut post_filter_seen = false;
    let mut group: Option<GroupSpec> = None;
    for (i, stage) in stages.iter().enumerate() {
        let operator = stage_operator(stage)?;
        match operator {
            "$match" => {
                if i == 0 {
                    filter = Filter::parse(one_stage(stage, "$match")?)?;
                    continue;
                }
                if lookup.is_none() || group.is_some() {
                    return Err(bad_aggregation(
                        "$match must be the first stage or directly follow $lookup",
                    ));
                }
                if post_filter_seen {
                    return Err(bad_aggregation(
                        "$match after $lookup may appear at most once",
                    ));
                }
                post_filter = Filter::parse(one_stage(stage, "$match")?)?;
                post_filter_seen = true;
            }
            "$lookup" => {
                if lookup.is_some() {
                    return Err(bad_aggregation("$lookup may appear at most once"));
                }
                if group.is_some() {
                    return Err(bad_aggregation("$lookup must not follow $group"));
                }
                lookup = Some(parse_lookup(one_stage(stage, "$lookup")?)?);
            }
            "$group" => {
                if group.is_some() {
                    return Err(bad_aggregation("$group may appear at most once"));
                }
                if i + 1 != stages.len() {
                    return Err(bad_aggregation("$group must be the last stage"));
                }
                group = Some(parse_group(one_stage(stage, "$group")?)?);
            }
            other => {
                return Err(bad_aggregation(format!("unsupported stage '{other}'")));
            }
        }
    }
    if lookup.is_none() && group.is_none() {
        return Err(bad_aggregation(
            "pipeline must contain one $group, optionally preceded by one $match",
        ));
    }
    Ok(AggregationPipeline {
        filter,
        lookup,
        post_filter,
        group,
    })
}

/// The single operator key of a stage object.
fn stage_operator(stage: &Value) -> DocResult<&str> {
    let object = stage
        .as_object()
        .ok_or_else(|| bad_aggregation("each stage must be an object"))?;
    if object.len() != 1 {
        return Err(bad_aggregation(
            "each stage must contain exactly one operator",
        ));
    }
    Ok(object.keys().next().unwrap())
}

/// Parse a `$lookup` body: `from`, `localField`, `foreignField`, `as` are
/// required; `filter` (a normal filter object applied to inner documents) is
/// optional. No other keys.
fn parse_lookup(value: &Value) -> DocResult<LookupSpec> {
    let object = value
        .as_object()
        .ok_or_else(|| bad_aggregation("$lookup must be an object"))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "from" | "localField" | "foreignField" | "as" | "filter") {
            return Err(bad_aggregation(format!("$lookup: unknown key '{key}'")));
        }
    }
    let required = |name: &str| -> DocResult<&str> {
        object
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| bad_aggregation(format!("$lookup requires a string '{name}'")))
    };
    let from = required("from")?;
    if from.is_empty() {
        return Err(bad_aggregation("$lookup 'from' must be non-empty"));
    }
    let local_field = required("localField")?;
    let foreign_field = required("foreignField")?;
    let as_field = required("as")?;
    for (name, path) in [
        ("localField", local_field),
        ("foreignField", foreign_field),
        ("as", as_field),
    ] {
        if !valid_path(path) {
            return Err(bad_aggregation(format!(
                "$lookup '{name}' must be a valid dotted path"
            )));
        }
    }
    let filter = match object.get("filter") {
        Some(v) => {
            Filter::parse(v).map_err(|e| bad_aggregation(format!("$lookup 'filter': {e}")))?
        }
        None => Filter::MatchAll,
    };
    Ok(LookupSpec {
        from: from.to_string(),
        local_field: local_field.to_string(),
        foreign_field: foreign_field.to_string(),
        as_field: as_field.to_string(),
        filter,
    })
}

fn one_stage<'a>(stage: &'a Value, expected: &str) -> DocResult<&'a Value> {
    let object = stage
        .as_object()
        .ok_or_else(|| bad_aggregation("each stage must be an object"))?;
    if object.len() != 1 {
        return Err(bad_aggregation(
            "each stage must contain exactly one operator",
        ));
    }
    object.get(expected).ok_or_else(|| {
        bad_aggregation(format!(
            "expected {expected} stage, found {}",
            object
                .keys()
                .next()
                .map(String::as_str)
                .unwrap_or("<empty>")
        ))
    })
}

fn parse_group(value: &Value) -> DocResult<GroupSpec> {
    let object = value
        .as_object()
        .ok_or_else(|| bad_aggregation("$group must be an object"))?;
    let id_value = object
        .get("_id")
        .ok_or_else(|| bad_aggregation("$group requires _id"))?;
    let id = if id_value.is_null() {
        GroupId::Null
    } else {
        GroupId::Field(parse_field_reference(id_value, "$group._id")?)
    };

    let accumulator_count = object.len().saturating_sub(1);
    if accumulator_count > MAX_ACCUMULATORS {
        return Err(bad_aggregation(format!(
            "$group exceeds {MAX_ACCUMULATORS} accumulators"
        )));
    }

    let mut accumulators = Vec::with_capacity(accumulator_count);
    for (output_field, expression) in object {
        if output_field == "_id" {
            continue;
        }
        if output_field.is_empty() || output_field.starts_with('$') {
            return Err(bad_aggregation(
                "accumulator output fields must be non-empty and must not start with '$'",
            ));
        }
        let expression = expression.as_object().ok_or_else(|| {
            bad_aggregation(format!("accumulator '{output_field}' must be an object"))
        })?;
        if expression.len() != 1 {
            return Err(bad_aggregation(format!(
                "accumulator '{output_field}' must contain exactly one operator"
            )));
        }
        let (operator, operand) = expression.iter().next().unwrap();
        let op = match operator.as_str() {
            "$sum" => AccumulatorOp::Sum(parse_field_reference(
                operand,
                &format!("'{output_field}.$sum'"),
            )?),
            "$size" => AccumulatorOp::Size(parse_field_reference(
                operand,
                &format!("'{output_field}.$size'"),
            )?),
            "$count" => {
                if !matches!(operand, Value::Object(map) if map.is_empty()) {
                    return Err(bad_aggregation(format!(
                        "'{output_field}.$count' must be an empty object"
                    )));
                }
                AccumulatorOp::Count
            }
            _ => {
                return Err(bad_aggregation(format!(
                    "unsupported accumulator '{operator}'"
                )));
            }
        };
        accumulators.push(AccumulatorSpec {
            output_field: output_field.clone(),
            op,
        });
    }

    Ok(GroupSpec { id, accumulators })
}

#[derive(Debug, Clone)]
struct GroupState {
    id: Value,
    accumulators: Vec<AccumulatorState>,
}

#[derive(Debug, Clone, Copy)]
enum AccumulatorState {
    Sum(SumState),
    Count(i64),
    Size(i64),
}

#[derive(Debug, Clone, Copy)]
enum SumState {
    Integer(i64),
    Float(f64),
}

impl AccumulatorState {
    fn new(spec: &AccumulatorSpec) -> Self {
        match spec.op {
            AccumulatorOp::Sum(_) => Self::Sum(SumState::Integer(0)),
            AccumulatorOp::Count => Self::Count(0),
            AccumulatorOp::Size(_) => Self::Size(0),
        }
    }

    fn update(&mut self, spec: &AccumulatorSpec, root: ValueView<'_>) -> DocResult<()> {
        match (self, &spec.op) {
            (AccumulatorState::Count(count), AccumulatorOp::Count) => {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| bad_aggregation("$count integer overflow"))?;
            }
            (AccumulatorState::Sum(sum), AccumulatorOp::Sum(path)) => {
                let segments: Vec<&str> = path.split('.').collect();
                for_each_at_path(root, &segments, 0, &mut |value| {
                    sum_add_terminal(sum, value, segments.len())
                })?;
            }
            (AccumulatorState::Size(total), AccumulatorOp::Size(path)) => {
                let segments: Vec<&str> = path.split('.').collect();
                for_each_at_path(root, &segments, 0, &mut |value| {
                    if let Some(arr) = value.as_array() {
                        let len = i64::try_from(arr.len())
                            .map_err(|_| bad_aggregation("$size array length overflow"))?;
                        *total = total
                            .checked_add(len)
                            .ok_or_else(|| bad_aggregation("$size integer overflow"))?;
                    }
                    Ok(())
                })?;
            }
            _ => return Err(DocError::Corrupt("aggregation accumulator mismatch".into())),
        }
        Ok(())
    }

    fn into_value(self) -> DocResult<Value> {
        match self {
            AccumulatorState::Count(count) => Ok(Value::Number(Number::from(count))),
            AccumulatorState::Size(total) => Ok(Value::Number(Number::from(total))),
            AccumulatorState::Sum(SumState::Integer(sum)) => Ok(Value::Number(Number::from(sum))),
            AccumulatorState::Sum(SumState::Float(sum)) => Number::from_f64(sum)
                .map(Value::Number)
                .ok_or_else(|| bad_aggregation("$sum produced a non-finite number")),
        }
    }
}

/// Maximum recursion depth for accumulator path fan-out, mirroring the ZDoc
/// materialization cap in `binary.rs`. Stored bodies that passed the write
/// path are capped well below this; the guard exists for bodies that reached
/// the store without a parse cap (raw-KV overlap, legacy data, corruption).
const MAX_ACCUMULATOR_PATH_DEPTH: usize = crate::binary::MAX_ZDOC_DEPTH;

/// Terminal handler for `$sum`: a scalar adds once; an array fans out
/// recursively so every numeric leaf reachable at the path contributes.
fn sum_add_terminal(sum: &mut SumState, value: ValueView<'_>, depth: usize) -> DocResult<()> {
    if depth > MAX_ACCUMULATOR_PATH_DEPTH {
        return Err(DocError::Corrupt(
            "accumulator path exceeds maximum depth".into(),
        ));
    }
    if let Some(arr) = value.as_array() {
        for i in 0..arr.len() {
            let Some(elem) = arr.get(i) else {
                return Err(DocError::Corrupt("truncated ZDoc array element".into()));
            };
            sum_add_terminal(sum, elem, depth + 1)?;
        }
        Ok(())
    } else {
        sum.add(value)
    }
}

/// Visit every value reachable at a dotted path, fanning out over arrays at
/// any segment (including the last). Objects descend by key; a missing key
/// contributes nothing. This is accumulator semantics only — filter paths and
/// group keys do not walk arrays.
fn for_each_at_path(
    root: ValueView<'_>,
    segments: &[&str],
    depth: usize,
    f: &mut dyn FnMut(ValueView<'_>) -> DocResult<()>,
) -> DocResult<()> {
    if depth > MAX_ACCUMULATOR_PATH_DEPTH {
        return Err(DocError::Corrupt(
            "accumulator path exceeds maximum depth".into(),
        ));
    }
    if segments.is_empty() {
        return f(root);
    }
    match root.type_byte() {
        TYPE_OBJECT => {
            let Some(obj) = root.as_object() else {
                return Err(DocError::Corrupt("truncated ZDoc object".into()));
            };
            match obj.get(segments[0]) {
                Some(child) => for_each_at_path(child, &segments[1..], depth + 1, f),
                None => Ok(()),
            }
        }
        TYPE_ARRAY => {
            let Some(arr) = root.as_array() else {
                return Err(DocError::Corrupt("truncated ZDoc array".into()));
            };
            for i in 0..arr.len() {
                let Some(elem) = arr.get(i) else {
                    return Err(DocError::Corrupt("truncated ZDoc array element".into()));
                };
                for_each_at_path(elem, segments, depth + 1, f)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

impl SumState {
    fn add(&mut self, value: ValueView<'_>) -> DocResult<()> {
        match value.type_byte() {
            TYPE_I64 => {
                let value = value
                    .as_i64()
                    .ok_or_else(|| DocError::Corrupt("truncated ZDoc integer".into()))?;
                match self {
                    SumState::Integer(sum) => {
                        *sum = sum
                            .checked_add(value)
                            .ok_or_else(|| bad_aggregation("$sum integer overflow"))?;
                    }
                    SumState::Float(sum) => {
                        *sum += value as f64;
                        ensure_finite(*sum, "$sum")?;
                    }
                }
            }
            TYPE_F64 => {
                let value = value
                    .as_f64()
                    .ok_or_else(|| DocError::Corrupt("truncated ZDoc float".into()))?;
                ensure_finite(value, "$sum input")?;
                let next = match *self {
                    SumState::Integer(sum) => sum as f64 + value,
                    SumState::Float(sum) => sum + value,
                };
                ensure_finite(next, "$sum")?;
                *self = SumState::Float(next);
            }
            _ => {}
        }
        Ok(())
    }
}

fn ensure_finite(value: f64, context: &str) -> DocResult<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(bad_aggregation(format!("{context} must be finite")))
    }
}

fn group_key(root: ValueView<'_>, id: &GroupId) -> DocResult<(Vec<u8>, Value)> {
    let field = match id {
        GroupId::Null => None,
        GroupId::Field(path) => root.get_path(path),
    };
    let mut encoded = Vec::new();
    encoding::encode_view(field.as_ref(), &mut encoded);
    let value = match field {
        None => Value::Null,
        Some(value) => match value.type_byte() {
            TYPE_NULL => Value::Null,
            TYPE_BOOL_FALSE => Value::Bool(false),
            TYPE_BOOL_TRUE => Value::Bool(true),
            TYPE_I64 => Value::Number(Number::from(
                value
                    .as_i64()
                    .ok_or_else(|| DocError::Corrupt("truncated ZDoc integer".into()))?,
            )),
            TYPE_F64 => {
                let number = value
                    .as_f64()
                    .ok_or_else(|| DocError::Corrupt("truncated ZDoc float".into()))?;
                ensure_finite(number, "group key")?;
                Value::Number(
                    Number::from_f64(number)
                        .ok_or_else(|| bad_aggregation("group key must be finite"))?,
                )
            }
            TYPE_STRING => Value::String(
                value
                    .as_str()
                    .ok_or_else(|| DocError::Corrupt("invalid ZDoc string".into()))?
                    .to_string(),
            ),
            TYPE_ARRAY | TYPE_OBJECT => {
                return Err(bad_aggregation(
                    "group key must be scalar or null, not an object or array",
                ));
            }
            other => {
                return Err(DocError::Corrupt(format!("unknown ZDoc type byte {other}")));
            }
        },
    };
    Ok((encoded, value))
}

fn estimated_group_bytes(
    encoded_key: &[u8],
    key: &Value,
    accumulator_count: usize,
) -> DocResult<usize> {
    let string_bytes = key.as_str().map(str::len).unwrap_or(0);
    let accumulator_bytes = accumulator_count
        .checked_mul(size_of::<AccumulatorState>())
        .ok_or_else(|| bad_aggregation("memory accounting overflow"))?;
    size_of::<GroupState>()
        .checked_add(size_of::<Vec<u8>>())
        .and_then(|n| n.checked_add(encoded_key.len()))
        .and_then(|n| n.checked_add(string_bytes))
        .and_then(|n| n.checked_add(accumulator_bytes))
        .ok_or_else(|| bad_aggregation("memory accounting overflow"))
}

/// Bounded group-state accumulator for a `$group` stage.
struct GroupEngine<'a> {
    spec: &'a GroupSpec,
    limits: AggregationLimits,
    groups: BTreeMap<Vec<u8>, GroupState>,
    memory_bytes: usize,
}

impl<'a> GroupEngine<'a> {
    fn new(spec: &'a GroupSpec, limits: AggregationLimits) -> Self {
        Self {
            spec,
            limits,
            groups: BTreeMap::new(),
            memory_bytes: 0,
        }
    }

    fn add(&mut self, root: ValueView<'_>) -> DocResult<()> {
        let (encoded_key, id) = group_key(root, &self.spec.id)?;
        if !self.groups.contains_key(&encoded_key) {
            if self.groups.len() >= self.limits.max_groups {
                return Err(bad_aggregation(format!(
                    "group count exceeds {}",
                    self.limits.max_groups
                )));
            }
            let added = estimated_group_bytes(&encoded_key, &id, self.spec.accumulators.len())?;
            let next_memory = self
                .memory_bytes
                .checked_add(added)
                .ok_or_else(|| bad_aggregation("memory accounting overflow"))?;
            if next_memory > self.limits.max_memory_bytes {
                return Err(bad_aggregation(format!(
                    "group state exceeds {} bytes",
                    self.limits.max_memory_bytes
                )));
            }
            self.memory_bytes = next_memory;
            self.groups.insert(
                encoded_key.clone(),
                GroupState {
                    id,
                    accumulators: self
                        .spec
                        .accumulators
                        .iter()
                        .map(AccumulatorState::new)
                        .collect(),
                },
            );
        }

        let state = self
            .groups
            .get_mut(&encoded_key)
            .ok_or_else(|| DocError::Corrupt("aggregation group disappeared".into()))?;
        for (accumulator, spec) in state.accumulators.iter_mut().zip(&self.spec.accumulators) {
            accumulator.update(spec, root)?;
        }
        Ok(())
    }

    fn finish(self) -> DocResult<(Vec<Value>, usize)> {
        let mut rows = Vec::with_capacity(self.groups.len());
        for (_, group) in self.groups {
            let mut row = Map::new();
            row.insert("_id".into(), group.id);
            for (state, spec) in group.accumulators.into_iter().zip(&self.spec.accumulators) {
                row.insert(spec.output_field.clone(), state.into_value()?);
            }
            rows.push(Value::Object(row));
        }
        Ok((rows, self.memory_bytes))
    }
}

/// Execute a parsed pipeline against a pinned snapshot.
pub fn execute_aggregation(
    snap: &SnapshotHandle,
    catalog: &Catalog,
    prefix: &[u8],
    collection: &str,
    pipeline: &AggregationPipeline,
    limits: AggregationLimits,
) -> DocResult<AggregationResult> {
    let outer = catalog
        .collection(prefix, collection)
        .ok_or_else(|| DocError::CollectionNotFound(collection.to_string()))?;
    let inner = match &pipeline.lookup {
        Some(spec) => Some(
            catalog
                .collection(prefix, &spec.from)
                .ok_or_else(|| DocError::CollectionNotFound(spec.from.clone()))?,
        ),
        None => None,
    };
    execute_aggregation_coll(snap, prefix, outer, inner, pipeline, limits)
}

/// Execute a parsed pipeline against caller-resolved collection metadata, so
/// the dispatch layer can release the catalog lock before the scan.
///
/// Both sides of a `$lookup` read under the SAME pinned snapshot — the
/// single-snapshot consistency claim application-tier joins cannot make.
pub fn execute_aggregation_coll(
    snap: &SnapshotHandle,
    prefix: &[u8],
    outer: &crate::catalog::CollectionMeta,
    inner: Option<&crate::catalog::CollectionMeta>,
    pipeline: &AggregationPipeline,
    limits: AggregationLimits,
) -> DocResult<AggregationResult> {
    let (rows, scanned_docs, matched_docs, memory_bytes) = match &pipeline.lookup {
        Some(spec) => {
            let inner = inner.ok_or_else(|| DocError::CollectionNotFound(spec.from.clone()))?;
            let join = crate::join::execute_lookup(
                snap,
                prefix,
                outer,
                inner,
                &pipeline.filter,
                spec,
                &pipeline.post_filter,
                limits,
            )?;
            match &pipeline.group {
                Some(spec) => {
                    let mut engine = GroupEngine::new(spec, limits);
                    for doc in join.docs {
                        let zdoc = ZDocBuilder::from_value(&doc);
                        engine.add(ValueView::new(&zdoc))?;
                    }
                    let (rows, memory_bytes) = engine.finish()?;
                    (
                        rows,
                        join.stats.candidates,
                        join.stats.matches,
                        memory_bytes,
                    )
                }
                None => (
                    join.docs,
                    join.stats.candidates,
                    join.stats.matches,
                    join.memory_bytes,
                ),
            }
        }
        None => {
            let spec = pipeline
                .group
                .as_ref()
                .ok_or_else(|| bad_aggregation("pipeline requires a $group stage"))?;
            let mut engine = GroupEngine::new(spec, limits);
            let stats = query::visit_planned_matches_bounded_coll(
                snap,
                prefix,
                outer,
                &pipeline.filter,
                limits.max_scan_docs,
                |_doc_id, stored| {
                    store::with_stored_view(stored, |root| engine.add(root))?;
                    Ok(true)
                },
            )?;
            let (rows, memory_bytes) = engine.finish()?;
            (rows, stats.candidates, stats.matches, memory_bytes)
        }
    };

    let result_bytes = serde_json::to_vec(&rows)
        .map_err(|e| DocError::Corrupt(format!("aggregation result serialization failed: {e}")))?
        .len();

    Ok(AggregationResult {
        rows,
        scanned_docs,
        matched_docs,
        memory_bytes,
        result_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(value: Value) -> DocResult<AggregationPipeline> {
        parse_pipeline(&serde_json::to_vec(&value).unwrap())
    }

    #[test]
    fn parses_only_match_then_group_grammar() {
        let pipeline = parse(json!([
            {"$match": {"active": true}},
            {"$group": {
                "_id": "$team.name",
                "total": {"$sum": "$amount"},
                "count": {"$count": {}}
            }}
        ]))
        .unwrap();
        let group = pipeline.group.as_ref().unwrap();
        assert_eq!(group.id, GroupId::Field("team.name".into()));
        assert_eq!(group.accumulators.len(), 2);
        assert!(pipeline.lookup.is_none());

        assert!(parse(
            json!([{"$match": {}}, {"$group": {"_id": null}}, {"$group": {"_id": null}}])
        )
        .is_err());
        assert!(parse(json!([{"$group": {"_id": null}}, {"$match": {}}])).is_err());
        assert!(parse(json!([{"$match": {}}])).is_err());
        assert!(parse(json!([{"$sort": {}}, {"$group": {"_id": null}}])).is_err());
    }

    #[test]
    fn parses_all_ten_legal_pipeline_shapes() {
        let lookup = json!({"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }});
        let group = json!({"$group": {"_id": null, "n": {"$count": {}}}});
        let match_stage = json!({"$match": {"active": true}});
        let post_match = json!({"$match": {"hits": {"$ne": []}}});

        // [$group], [$match, $group] still parse with no lookup.
        assert!(parse(json!([group.clone()])).unwrap().lookup.is_none());
        assert!(parse(json!([match_stage.clone(), group.clone()]))
            .unwrap()
            .lookup
            .is_none());

        // [$lookup]
        let p = parse(json!([lookup.clone()])).unwrap();
        let spec = p.lookup.as_ref().unwrap();
        assert_eq!(spec.from, "orders");
        assert_eq!(spec.local_field, "_id");
        assert_eq!(spec.foreign_field, "user_id");
        assert_eq!(spec.as_field, "hits");
        assert!(p.group.is_none());
        assert_eq!(p.post_filter, Filter::MatchAll);

        // [$lookup, $group]
        let p = parse(json!([lookup.clone(), group.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_some());
        // [$match, $lookup]
        let p = parse(json!([match_stage.clone(), lookup.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_none());
        // [$match, $lookup, $group]
        let p = parse(json!([match_stage.clone(), lookup.clone(), group.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_some());
        // [$lookup, $match]
        let p = parse(json!([lookup.clone(), post_match.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_none());
        assert_ne!(p.post_filter, Filter::MatchAll);
        // [$lookup, $match, $group]
        let p = parse(json!([lookup.clone(), post_match.clone(), group.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_some());
        assert_ne!(p.post_filter, Filter::MatchAll);
        // [$match, $lookup, $match]
        let p = parse(json!([match_stage.clone(), lookup.clone(), post_match.clone()])).unwrap();
        assert!(p.lookup.is_some() && p.group.is_none());
        assert_ne!(p.post_filter, Filter::MatchAll);
        // [$match, $lookup, $match, $group]
        let p = parse(json!([
            match_stage.clone(),
            lookup.clone(),
            post_match.clone(),
            group.clone()
        ]))
        .unwrap();
        assert!(p.lookup.is_some() && p.group.is_some());
        assert_ne!(p.post_filter, Filter::MatchAll);
    }

    #[test]
    fn rejects_illegal_lookup_pipelines() {
        let lookup = json!({"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }});
        let group = json!({"$group": {"_id": null}});
        let match_stage = json!({"$match": {}});

        // $lookup after $group
        assert!(parse(json!([group.clone(), lookup.clone()])).is_err());
        // second $lookup
        assert!(parse(json!([lookup.clone(), lookup.clone()])).is_err());
        // $match after $group
        assert!(parse(json!([lookup.clone(), group.clone(), match_stage.clone()])).is_err());
        // two $match stages after $lookup
        assert!(parse(json!([lookup.clone(), match_stage.clone(), match_stage.clone()])).is_err());
        // non-first $match with no $lookup
        assert!(parse(json!([match_stage.clone(), match_stage.clone(), group.clone()])).is_err());
        // five stages
        assert!(parse(json!([
            match_stage.clone(),
            lookup.clone(),
            match_stage.clone(),
            group.clone(),
            group.clone()
        ]))
        .is_err());
        // bare $match
        assert!(parse(json!([match_stage.clone()])).is_err());
    }

    #[test]
    fn rejects_malformed_lookup_stages() {
        let good = json!({"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }});
        assert!(parse(json!([good])).is_ok());

        // unknown key
        assert!(parse(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits",
            "pipeline": []
        }}]))
        .is_err());
        // missing required fields
        for missing in ["from", "localField", "foreignField", "as"] {
            let mut body = serde_json::Map::new();
            body.insert("from".into(), json!("orders"));
            body.insert("localField".into(), json!("_id"));
            body.insert("foreignField".into(), json!("user_id"));
            body.insert("as".into(), json!("hits"));
            body.remove(missing);
            assert!(
                parse(json!([{"$lookup": body}])).is_err(),
                "missing {missing}"
            );
        }
        // bad paths
        assert!(parse(json!([{"$lookup": {
            "from": "orders", "localField": "a..b", "foreignField": "user_id", "as": "hits"
        }}]))
        .is_err());
        assert!(parse(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "$x", "as": "hits"
        }}]))
        .is_err());
        assert!(parse(json!([{"$lookup": {
            "from": "", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }}]))
        .is_err());
        // multi-operator stage object
        assert!(parse(json!([{"$lookup": {}, "$match": {}}])).is_err());
    }

    #[test]
    fn rejects_non_strict_group_expressions() {
        assert!(parse(json!([{"$group": {"_id": {"x": 1}}}])).is_err());
        assert!(parse(json!([{"$group": {"_id": "$a..b"}}])).is_err());
        assert!(parse(json!([{"$group": {"_id": null, "n": {"$sum": 1}}}])).is_err());
        assert!(parse(json!([{"$group": {"_id": null, "n": {"$count": 1}}}])).is_err());
        assert!(parse(json!([{"$group": {"_id": null, "n": {"$avg": "$x"}}}])).is_err());
    }

    #[test]
    fn enforces_parser_caps() {
        let accumulators: Map<String, Value> = (0..=MAX_ACCUMULATORS)
            .map(|i| (format!("n{i}"), json!({"$count": {}})))
            .chain(std::iter::once(("_id".into(), Value::Null)))
            .collect();
        assert!(parse(Value::Array(vec![json!({"$group": accumulators})])).is_err());
        assert!(parse_pipeline(&vec![b' '; MAX_PIPELINE_BYTES + 1]).is_err());
    }

    fn run_group(docs: &[Value], group_stage: Value) -> DocResult<Vec<Value>> {
        let pipeline = parse(json!([group_stage]))?;
        let spec = pipeline.group.as_ref().unwrap();
        let mut engine = GroupEngine::new(spec, AggregationLimits::default());
        for doc in docs {
            let zdoc = ZDocBuilder::from_value(doc);
            engine.add(ValueView::new(&zdoc))?;
        }
        let (rows, _) = engine.finish()?;
        Ok(rows)
    }

    #[test]
    fn sum_walks_intermediate_arrays() {
        let rows = run_group(
            &[json!({"items": [{"price": 1}, {"price": 2}, {"price": 3}]})],
            json!({"$group": {"_id": null, "t": {"$sum": "$items.price"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "t": 6})]);
    }

    #[test]
    fn sum_walks_terminal_array() {
        let rows = run_group(
            &[json!({"amounts": [1, 2, 3]})],
            json!({"$group": {"_id": null, "t": {"$sum": "$amounts"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "t": 6})]);
    }

    #[test]
    fn sum_reaches_numeric_leaves_through_nested_arrays() {
        let rows = run_group(
            &[json!({"a": [[1, 2], [3]]})],
            json!({"$group": {"_id": null, "t": {"$sum": "$a"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "t": 6})]);
    }

    #[test]
    fn sum_skips_non_numeric_leaves_and_missing_paths() {
        let rows = run_group(
            &[
                json!({"amounts": [1, "x", null, true, 2.5]}),
                json!({"other": 9}),
            ],
            json!({"$group": {"_id": null, "t": {"$sum": "$amounts"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "t": 3.5})]);
    }

    #[test]
    fn size_counts_array_at_path() {
        let rows = run_group(
            &[
                json!({"orders": [1, 2]}),
                json!({"orders": "not-an-array"}),
                json!({"other": 1}),
            ],
            json!({"$group": {"_id": null, "n": {"$size": "$orders"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "n": 2})]);
    }

    #[test]
    fn size_walks_intermediate_arrays() {
        let rows = run_group(
            &[json!({"a": [{"b": [1, 2]}, {"b": [3]}, {"c": 0}]})],
            json!({"$group": {"_id": null, "n": {"$size": "$a.b"}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "n": 3})]);
    }

    #[test]
    fn accumulator_path_longer_than_document_is_zero() {
        let long_path = (0..300).map(|_| "d").collect::<Vec<_>>().join(".");
        let rows = run_group(
            &[json!({"d": {"d": {"d": 1}}})],
            json!({"$group": {"_id": null, "t": {"$sum": format!("${long_path}")}}}),
        )
        .unwrap();
        assert_eq!(rows, vec![json!({"_id": null, "t": 0})]);
    }

    #[test]
    fn accumulator_path_beyond_depth_cap_is_corrupt_not_a_crash() {
        let mut doc = Value::from(1);
        for _ in 0..(MAX_ACCUMULATOR_PATH_DEPTH + 50) {
            let mut m = Map::new();
            m.insert("d".to_string(), doc);
            doc = Value::Object(m);
        }
        let long_path = (0..300).map(|_| "d").collect::<Vec<_>>().join(".");
        let err = run_group(
            &[doc],
            json!({"$group": {"_id": null, "t": {"$sum": format!("${long_path}")}}}),
        )
        .unwrap_err();
        assert!(matches!(err, DocError::Corrupt(_)), "got {err:?}");
    }

    #[test]
    fn rejects_malformed_lookup_filters() {
        let lookup = |filter: Value| {
            json!([{"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits",
                "filter": filter
            }}])
        };

        // filter is not an object
        let err = parse(lookup(json!(5))).unwrap_err();
        assert!(err.to_string().contains("$lookup 'filter'"), "got: {err}");
        // unknown operator inside the filter
        assert!(parse(lookup(json!({"total": {"$bogus": 1}}))).is_err());
        // over-length regex inside the filter
        assert!(parse(lookup(json!({"_id": {"$regex": "x".repeat(257)}}))).is_err());
        // unknown top-level $-key inside the filter
        assert!(parse(lookup(json!({"$weird": 1}))).is_err());
        // same rules in the post-join $match position
        assert!(parse(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits"
            }},
            {"$match": {"hits": {"$bogus": 1}}}
        ]))
        .is_err());
    }

    #[test]
    fn deeply_nested_lookup_filter_errors_cleanly() {
        // 200 levels of $and nesting blows serde_json's parse recursion cap:
        // a named error, not a crash.
        let mut deep = json!({"a": 1});
        for _ in 0..200 {
            deep = json!({"$and": [deep]});
        }
        assert!(parse(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits",
            "filter": deep
        }}]))
        .is_err());

        // 50 levels parses fine.
        let mut shallow = json!({"a": 1});
        for _ in 0..50 {
            shallow = json!({"$and": [shallow]});
        }
        assert!(parse(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits",
            "filter": shallow
        }}]))
        .is_ok());

        // A filter that pushes the pipeline past the byte cap is rejected.
        let big = json!({"note": "x".repeat(MAX_PIPELINE_BYTES)});
        let err = parse(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits",
            "filter": big
        }}]))
        .unwrap_err();
        assert!(err.to_string().contains("pipeline exceeds"), "got: {err}");
    }

    #[test]
    fn parses_size_accumulator() {
        let pipeline = parse(json!([{"$group": {"_id": null, "n": {"$size": "$orders"}}}]))
            .unwrap();
        let group = pipeline.group.as_ref().unwrap();
        assert_eq!(
            group.accumulators,
            vec![AccumulatorSpec {
                output_field: "n".into(),
                op: AccumulatorOp::Size("orders".into()),
            }]
        );
        // $size still requires a '$path' string.
        assert!(parse(json!([{"$group": {"_id": null, "n": {"$size": 1}}}])).is_err());
    }
}
