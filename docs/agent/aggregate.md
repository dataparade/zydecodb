zydecodb-agent-docs: 1
zydecodb: 1.2.0
topic: aggregate
next: query, pitfalls

# Aggregation

Bounded, deterministic rollups plus one equality `$lookup`. **Not Mongo
aggregation.** `collection.aggregate(pipeline)` (Python / TypeScript) /
`Aggregate` (Go). Authenticated read. Illegal inside a transaction.

## Six legal shapes

- `[$group]`
- `[$match, $group]`
- `[$lookup]`
- `[$lookup, $group]`
- `[$match, $lookup]`
- `[$match, $lookup, $group]`

At most three stages. `$lookup` at most once and never after `$group`.

## `$lookup` (equality left-outer)

Required keys only: `from`, `localField`, `foreignField`, `as`.
No `pipeline:`, no `let:`. Same tenant, one snapshot.

## `$group`

- `_id` is JSON `null` (one bucket) or `"$dotted.path"`
- Accumulators: `{"$sum":"$path"}` or `{"$count":{}}` only
- At most 16 accumulators

```python
users.aggregate([
    {"$match": {"city": "London"}},
    {"$group": {"_id": "$city", "n": {"$count": {}}, "ages": {"$sum": "$age"}}},
])
```

## Rejected

`$unwind`, multi-`$lookup`, `pipeline:` / `let:` `$lookup`, `$facet`,
expressions, window functions, any shape outside the six above.

Hard ceilings: pipeline JSON ≤ 64 KiB; server `[aggregation]` caps scan /
groups / memory. If `$lookup` has no usable inner index and the inner
collection is too large, the server errors — it will not nest a collection
scan per outer row.
