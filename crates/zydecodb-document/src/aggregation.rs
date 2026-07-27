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

pub const MAX_PIPELINE_STAGES: usize = 2;
pub const MAX_ACCUMULATORS: usize = 16;
pub const MAX_PIPELINE_BYTES: usize = 64 * 1024;

pub const DEFAULT_MAX_SCAN_DOCS: usize = 100_000;
pub const DEFAULT_MAX_GROUPS: usize = 10_000;
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;

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
}

impl Default for AggregationLimits {
    fn default() -> Self {
        Self {
            max_scan_docs: DEFAULT_MAX_SCAN_DOCS,
            max_groups: DEFAULT_MAX_GROUPS,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

/// Parsed, validated aggregation pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationPipeline {
    pub filter: Filter,
    pub group: GroupSpec,
}

impl AggregationPipeline {
    /// Parse the strict minimal pipeline grammar from JSON bytes.
    pub fn parse(bytes: &[u8]) -> DocResult<Self> {
        parse_pipeline(bytes)
    }
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

/// Parse exactly `[{$group: ...}]` or `[{$match: ...}, {$group: ...}]`.
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
        return Err(bad_aggregation(
            "pipeline must contain one $group, optionally preceded by one $match",
        ));
    }

    let (filter, group_stage) = match stages.as_slice() {
        [group] => (Filter::MatchAll, group),
        [match_stage, group] => {
            let match_value = one_stage(match_stage, "$match")?;
            (Filter::parse(match_value)?, group)
        }
        _ => unreachable!("stage count validated above"),
    };
    let group_value = one_stage(group_stage, "$group")?;
    let group = parse_group(group_value)?;
    Ok(AggregationPipeline { filter, group })
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
                if let Some(value) = root.get_path(path) {
                    sum.add(value)?;
                }
            }
            _ => return Err(DocError::Corrupt("aggregation accumulator mismatch".into())),
        }
        Ok(())
    }

    fn into_value(self) -> DocResult<Value> {
        match self {
            AccumulatorState::Count(count) => Ok(Value::Number(Number::from(count))),
            AccumulatorState::Sum(SumState::Integer(sum)) => Ok(Value::Number(Number::from(sum))),
            AccumulatorState::Sum(SumState::Float(sum)) => Number::from_f64(sum)
                .map(Value::Number)
                .ok_or_else(|| bad_aggregation("$sum produced a non-finite number")),
        }
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

fn with_stored_view<T>(
    stored: &[u8],
    f: impl FnOnce(ValueView<'_>) -> DocResult<T>,
) -> DocResult<T> {
    let Some((&kind, payload)) = stored.split_first() else {
        return Err(DocError::Corrupt("empty stored document".into()));
    };
    if kind == store::VK_ZDOC {
        return f(ValueView::new(payload));
    }

    let value: Value = serde_json::from_slice(payload)
        .map_err(|e| DocError::Corrupt(format!("invalid stored JSON: {e}")))?;
    let zdoc = ZDocBuilder::from_value(&value);
    f(ValueView::new(&zdoc))
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
    let mut groups: BTreeMap<Vec<u8>, GroupState> = BTreeMap::new();
    let mut memory_bytes = 0usize;

    let stats = query::visit_planned_matches_bounded(
        snap,
        catalog,
        prefix,
        collection,
        &pipeline.filter,
        limits.max_scan_docs,
        |_doc_id, stored| {
            with_stored_view(stored, |root| {
                let (encoded_key, id) = group_key(root, &pipeline.group.id)?;
                if !groups.contains_key(&encoded_key) {
                    if groups.len() >= limits.max_groups {
                        return Err(bad_aggregation(format!(
                            "group count exceeds {}",
                            limits.max_groups
                        )));
                    }
                    let added = estimated_group_bytes(
                        &encoded_key,
                        &id,
                        pipeline.group.accumulators.len(),
                    )?;
                    let next_memory = memory_bytes
                        .checked_add(added)
                        .ok_or_else(|| bad_aggregation("memory accounting overflow"))?;
                    if next_memory > limits.max_memory_bytes {
                        return Err(bad_aggregation(format!(
                            "group state exceeds {} bytes",
                            limits.max_memory_bytes
                        )));
                    }
                    memory_bytes = next_memory;
                    groups.insert(
                        encoded_key.clone(),
                        GroupState {
                            id,
                            accumulators: pipeline
                                .group
                                .accumulators
                                .iter()
                                .map(AccumulatorState::new)
                                .collect(),
                        },
                    );
                }

                let state = groups
                    .get_mut(&encoded_key)
                    .ok_or_else(|| DocError::Corrupt("aggregation group disappeared".into()))?;
                for (accumulator, spec) in state
                    .accumulators
                    .iter_mut()
                    .zip(&pipeline.group.accumulators)
                {
                    accumulator.update(spec, root)?;
                }
                Ok(())
            })?;
            Ok(true)
        },
    )?;

    let mut rows = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let mut row = Map::new();
        row.insert("_id".into(), group.id);
        for (state, spec) in group
            .accumulators
            .into_iter()
            .zip(&pipeline.group.accumulators)
        {
            row.insert(spec.output_field.clone(), state.into_value()?);
        }
        rows.push(Value::Object(row));
    }
    let result_bytes = serde_json::to_vec(&rows)
        .map_err(|e| DocError::Corrupt(format!("aggregation result serialization failed: {e}")))?
        .len();

    Ok(AggregationResult {
        rows,
        scanned_docs: stats.candidates,
        matched_docs: stats.matches,
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
        assert_eq!(pipeline.group.id, GroupId::Field("team.name".into()));
        assert_eq!(pipeline.group.accumulators.len(), 2);

        assert!(parse(
            json!([{"$match": {}}, {"$group": {"_id": null}}, {"$group": {"_id": null}}])
        )
        .is_err());
        assert!(parse(json!([{"$group": {"_id": null}}, {"$match": {}}])).is_err());
        assert!(parse(json!([{"$match": {}}])).is_err());
        assert!(parse(json!([{"$sort": {}}, {"$group": {"_id": null}}])).is_err());
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
}
