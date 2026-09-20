# Design: `$lookup` joins (1.1; filters and post-join `$match` added in 1.3-dev)

## 1. Pipeline grammar

[`aggregation.rs`](../crates/zydecodb-document/src/aggregation.rs) accepts the
`$group` / `$match`→`$group` shapes, one `$lookup` stage, and one optional
post-join `$match` (`MAX_PIPELINE_STAGES = 4`).

Legal pipelines:

- `[$group]`
- `[$match, $group]`
- `[$lookup]`
- `[$lookup, $group]`
- `[$lookup, $match]`
- `[$lookup, $match, $group]`
- `[$match, $lookup]`
- `[$match, $lookup, $group]`
- `[$match, $lookup, $match]`
- `[$match, $lookup, $match, $group]`

Rules:

- `$lookup` appears at most once and never after `$group`.
- A `$match` directly after `$lookup` (at most one) filters the joined
  documents — outer plus the `as` array. Rejected documents are dropped before
  the `max_memory_bytes` charge and never reach `$group`.
- Bare `[$lookup]` (and `[$lookup, $group]`) is legal. The outer side is a
  collection scan bounded by `max_scan_docs`, same as any other candidate scan.
- `MAX_PIPELINE_BYTES` stays `64 * 1024`.
- `$group` after `$lookup` groups the join-output documents (outer document plus
  the `as` array), not the pre-join collection.

## 2. Join semantics

- Equality-only, left-outer. Output is the outer document with matched inner
  documents in an array field.
- Stage shape (the one users already know), with one optional addition:

  ```json
  { "$lookup": { "from": "inner", "localField": "k", "foreignField": "k", "as": "hits",
                 "filter": { "total": { "$gte": 10 } } } }
  ```

- `from`, `localField`, `foreignField`, and `as` are required; `filter` is the
  only optional key. `localField` / `foreignField` / `as` follow existing
  dotted-path rules (non-empty segments, no `$`-prefixed segments).
- `filter` is a normal filter object applied to inner documents **before they
  are attached**. On the INLJ path it is a residual check per probe; on the
  hash path it plans the inner scan, so rejected documents never enter the map.
  Rejected documents do not count toward `max_matches_per_outer` or
  `max_hash_bytes`; they still count toward the `max_scan_docs` candidate bound
  on the hash build (the scan gate is not bypassed by a selective filter).
- `from` must name a collection under the **same tenant prefix**. Joins never
  cross tenants. A missing `from` collection is a named error.
- Both sides read under one `SnapshotHandle`. That single-snapshot consistency
  is the semantic claim application-tier joins cannot make.
- Missing or non-scalar `localField` values produce an empty `as` array (left
  outer, not an error). Equality uses the same scalar encoding as indexes.
- Out of scope: non-equi joins, right/full outer, `pipeline:` / `let:` form,
  `$unwind`, multi-`$lookup`, `$match` after `$group`, `$limit` / `$sort`,
  sort-merge, Leapfrog Triejoin, incremental view maintenance, pushing the
  `filter` range into the index scan.

## 2a. Path asymmetry: accumulators walk arrays, filters do not

Accumulator paths (`$sum`, `$size`) fan out over arrays at **every** segment,
including the last: `$sum` adds every numeric leaf reachable at the path;
`$size` adds the length of each array found there (non-array or missing
contributes 0). This is what makes `[$lookup, $group]` able to aggregate over
the joined children (`{"$sum": "$orders.total"}`). Fan-out recursion is capped
at the ZDoc depth limit (256); beyond it the query fails with a named error,
never a stack overflow.

Filter paths — in `$match` and in the `$lookup` `filter` — do NOT walk arrays.
That is the shipped 1.0 `find` contract and is unchanged. To filter on joined
children use `$elemMatch` (`{"orders": {"$elemMatch": {"total": {"$gt": 500}}}}`),
`{"orders": []}` (no children), or `{"orders": {"$ne": []}}` (at least one).
Group-key `_id` paths are likewise unchanged: the key must resolve to a scalar
or null.

**Behavior change:** `{"$sum": "$amounts"}` where `amounts` is `[1,2,3]`
returned `0` in 1.0–1.2; it now returns `6`.

## 3. Failure posture

**Never a silent full-scan-per-document fallback.** That O(n×m) footgun is
MongoDB's mistake and we will not ship it.

A usable inner index is:

- `foreignField == "_id"` — the virtual always-present index
  (`AccessPath::ById` in [`planner.rs`](../crates/zydecodb-document/src/planner.rs)).
- Otherwise, an index whose **leading** field is `foreignField` (equality prefix
  of length 1). A compound index on `[foreignField, …]` counts; one on
  `[other, foreignField]` does not.

Strategy is chosen once, before the outer scan, and the `filter` key does not
influence the choice:

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
| `max_scan_docs` | `100_000` | Outer candidates examined; also the hash-join inner-size gate (inner `filter` does not reduce this) |
| `max_matches_per_outer` | `1_000` | Inner documents attached to one outer document (charged after the inner `filter`) |
| `max_memory_bytes` | `16 MiB` | Retained join-output state (charged after the post-join `$match`) |
| `max_hash_bytes` | `16 MiB` | Hash-join build (inner-side map, post-`filter`); hard reject, no spill |
| `max_result_bytes` | `4 MiB` | Encoded response size |
