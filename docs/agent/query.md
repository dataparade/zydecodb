zydecodb-agent-docs: 1
zydecodb: 1.4.0
topic: query
next: python, aggregate, pitfalls

# Query, updates, indexes

Filters are JSON. Any field is queryable. The planner uses `_id`, then the
best index equality prefix plus an optional range, else a collection scan.
Every candidate is re-checked against the full filter. Indexes change speed,
not results.

## Filter operators

Comparison: `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type`.
Array: `$all` `$elemMatch`.
String: `$regex` (pattern ≤256 chars, `i` flag only, residual scan).
Logical: implicit AND (`{a: 1, b: 2}`), `$and`, `$or`, `$not` (one sub-filter).
Paths: dotted (`address.city`). `_id` is always present.

Cross-type order matches the index encoding: null < bool < number < string.

## Find

`find` supports sort, include-or-exclude projection (not mixed), skip, limit.
Cursor pagination is repeatable-read: later pages pin the same snapshot, so
concurrent writes do not shift page contents.

Sort streams from an index when it matches that index (or its exact reverse).
Otherwise a bounded sort buffer applies (`max_sort_buffer`).

`find_one` is `find` with `limit=1`. `count_documents` and `distinct` are
supported.

## Updates

Bare (non-`$`) update documents are rejected. Operators: `$set` `$inc` `$unset`
`$push` `$setOnInsert`. `$setOnInsert` applies only on upsert insert; on path
conflict `$set`/`$inc`/`$unset`/`$push` win.

Filtered positional `$set` updates exactly one array element:

```text
items.$[skuId=ABC].qty          # string token
items.$[skuId="ABC"].qty        # JSON literal
items.$[skuId=ABC]              # replace the element
```

Rules: `$set` only; exactly one match (zero or many → error); at most one
`$[field=value]` per path. No bare `$`, `$[]`, `$[<id>]`, `arrayFilters`, or
“update all matches”.

`update_one` / `update_many` / `delete_one` / `delete_many`: each matched
document is one atomic body+index write. Multi-doc is not globally atomic.

Upsert (`upsert=True` / upsert flag): if nothing matches, insert at most one
doc from top-level equality fields plus the update. Response may include
`upserted_id`.

## Indexes

`create_index` maintains secondary keys atomically with the body (one WAL
record). Unique indexes: duplicate insert/update → `Conflict`. TTL indexes:
`expire_after_seconds` plus a field of unix millis. Compound + DESC: pass
direction tuples / `IndexField` / `{ path, ascending }`.
