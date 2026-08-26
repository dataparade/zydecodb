# Design: `$lookup` joins (1.1)

## 1. Pipeline grammar

[`aggregation.rs`](../crates/zydecodb-document/src/aggregation.rs) accepts the
`$group` / `$match`→`$group` shapes and one additional `$lookup` stage
(`MAX_PIPELINE_STAGES = 3`).

Legal pipelines after the change:

- `[$group]`
- `[$match, $group]`
- `[$lookup]`
- `[$lookup, $group]`
- `[$match, $lookup]`
- `[$match, $lookup, $group]`

Rules:

- `$lookup` appears at most once and never after `$group`.
- Bare `[$lookup]` (and `[$lookup, $group]`) is legal. The outer side is a
  collection scan bounded by `max_scan_docs`, same as any other candidate scan.
- `MAX_PIPELINE_STAGES` becomes 3.
- `MAX_PIPELINE_BYTES` stays `64 * 1024`.
- `$group` after `$lookup` groups the join-output documents (outer document plus
  the `as` array), not the pre-join collection.

## 2. Join semantics

- Equality-only, left-outer. Output is the outer document with matched inner
  documents in an array field.
- Stage shape (the one users already know):

  ```json
  { "$lookup": { "from": "inner", "localField": "k", "foreignField": "k", "as": "hits" } }
  ```

- `from`, `localField`, `foreignField`, and `as` are required. No extra keys.
  `localField` / `foreignField` / `as` follow existing dotted-path rules
  (non-empty segments, no `$`-prefixed segments).
- `from` must name a collection under the **same tenant prefix**. Joins never
  cross tenants. A missing `from` collection is a named error.
- Both sides read under one `SnapshotHandle`. That single-snapshot consistency
  is the semantic claim application-tier joins cannot make.
- Missing or non-scalar `localField` values produce an empty `as` array (left
  outer, not an error). Equality uses the same scalar encoding as indexes.
- Out of scope: non-equi joins, right/full outer, `pipeline:` / `let:` form,
  `$unwind`, multi-`$lookup`, sort-merge, Leapfrog Triejoin, incremental view
  maintenance.

## 3. Failure posture

**Never a silent full-scan-per-document fallback.** That O(n×m) footgun is
MongoDB's mistake and we will not ship it.

A usable inner index is:

- `foreignField == "_id"` — the virtual always-present index
  (`AccessPath::ById` in [`planner.rs`](../crates/zydecodb-document/src/planner.rs)).
- Otherwise, an index whose **leading** field is `foreignField` (equality prefix
  of length 1). A compound index on `[foreignField, …]` counts; one on
  `[other, foreignField]` does not.

Strategy is chosen once, before the outer scan:

- usable index → indexed nested-loop (INLJ)
- no index and `inner.doc_count <= max_scan_docs` → bounded hash join (one
  inner scan plus an in-memory map, capped by `max_hash_bytes`)
- otherwise → named error naming the index to create and the inner size that
  disqualified the hash path

Hash join is not a silent fallback. It is a single bounded inner scan, not a
per-outer collection scan.

## 4. Bounds

Mirror `AggregationLimits`. Every bound rejects with a named error on exceed —
no spill, no silent truncation.

| Bound | Default | Meaning |
| --- | --- | --- |
| `max_scan_docs` | `100_000` | Outer candidates examined; also the hash-join inner-size gate |
| `max_matches_per_outer` | `1_000` | Inner documents attached to one outer document |
| `max_memory_bytes` | `16 MiB` | Retained join-output state |
| `max_hash_bytes` | `16 MiB` | Hash-join build (inner-side map); hard reject, no spill |
| `max_result_bytes` | `4 MiB` | Encoded response size |
