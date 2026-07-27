# Minimal Aggregation

ZydecoDB supports a **bounded**, **deterministic** aggregation opcode (`Aggregate = 0x2B`) for simple rollups. This is not MongoDB aggregation compatibility.

## Supported pipeline

Exactly:

1. Optional first stage: `$match` (same filter language as `Find`)
2. Required final stage: `$group`

`$group` rules:

- `_id` must be JSON `null` (one global bucket) or a single field reference `"$dotted.path"`
- Accumulator fields may only be:
  - `{"$sum":"$path"}` — missing/non-numeric inputs contribute `0`
  - `{"$count":{}}` — counts every document that reaches `$group`

Hard parser ceilings (not configurable):

- At most two stages
- At most 16 accumulators
- Pipeline JSON ≤ 64 KiB

## Numeric semantics

- Integer-only sums stay checked `i64` until any float input promotes the group to finite `f64`
- Integer overflow or non-finite float output is rejected
- Missing group-key fields map to `null`
- Object/array group keys are rejected
- Result groups are emitted in deterministic scalar/`null` key order

## Resource limits (`[aggregation]`)

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_scan_docs` | `100000` | Candidates inspected (counted before residual filter) |
| `max_groups` | `10000` | Distinct group keys |
| `max_memory_bytes` | `16MiB` | Group key + accumulator state |
| `max_result_bytes` | `4MiB` | Encoded response size (enforced while encoding) |

Aggregation is an authenticated **read**. It is unavailable inside bounded transactions. Tenant prefix and collection-prefix ACL apply like other document reads.

## Explicit non-goals

The following remain unsupported and are rejected by the parser:

- `$lookup`, joins, `$unwind`
- Expression languages, window functions, `$facet`
- Multi-stage pipelines beyond `$match` → `$group`
- Spilling to disk / unbounded group maps

## Client APIs

Official clients expose `aggregate(pipeline)` on collection handles (Python, Go, TypeScript). Codec conformance vectors cover request/response framing.
