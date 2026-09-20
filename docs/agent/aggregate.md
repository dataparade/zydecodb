zydecodb-agent-docs: 1
zydecodb: 1.2.0
topic: aggregate
next: query, pitfalls

# Aggregation

Bounded, deterministic rollups plus one equality `$lookup`. **Not Mongo
aggregation.** `collection.aggregate(pipeline)` (Python / TypeScript) /
`Aggregate` (Go). Authenticated read. Illegal inside a transaction.

## Ten legal shapes

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

At most four stages. `$lookup` at most once and never after `$group`.
A `$match` directly after `$lookup` filters the JOINED documents (outer
plus the `as` array).

## `$lookup` (equality left-outer)

Required keys: `from`, `localField`, `foreignField`, `as`.
Optional key: `filter` — a normal filter object applied to inner documents
BEFORE they are attached. No `pipeline:`, no `let:`. Same tenant, one
snapshot.

```python
users.aggregate([
    {"$lookup": {
        "from": "orders", "localField": "_id", "foreignField": "user_id",
        "as": "orders", "filter": {"total": {"$gte": 10}},
    }},
    {"$match": {"orders": {"$ne": []}}},          # post-join: has children
    {"$group": {"_id": "$_id",
                "spend": {"$sum": "$orders.total"},
                "n": {"$size": "$orders"}}},
])
```

## `$group`

- `_id` is JSON `null` (one bucket) or `"$dotted.path"` (must resolve to a
  scalar or null; group-key paths do NOT walk arrays)
- Accumulators: `{"$sum":"$path"}`, `{"$size":"$path"}`, `{"$count":{}}`
- At most 16 accumulators
- `$sum` / `$size` paths walk into arrays at EVERY segment, including the
  last: `$sum` adds every numeric leaf reachable; `$size` adds the length
  of each array found at the path. Non-numeric / non-array / missing
  inputs contribute 0.

## Filter paths do NOT walk arrays (asymmetry)

Filters (`$match`, `$lookup filter`) keep the shipped `find` semantics: a
path matches the value AT that path. To test elements of a joined array,
use `$elemMatch` (`{"orders": {"$elemMatch": {"total": {"$gt": 500}}}}`),
`{"orders": []}` for "no children", `{"orders": {"$ne": []}}` for "at
least one". Accumulator paths are the ones that fan out over arrays.

## Rejected

`$unwind`, multi-`$lookup`, `pipeline:` / `let:` `$lookup`, `$facet`,
expressions, window functions, `$match` after `$group`, `$limit` / `$sort`,
any shape outside the ten above.

Hard ceilings: pipeline JSON ≤ 64 KiB; server `[aggregation]` caps scan /
groups / memory / matches-per-outer / hash bytes. If `$lookup` has no
usable inner index and the inner collection is too large, the server
errors — it will not nest a collection scan per outer row.
