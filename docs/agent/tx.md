zydecodb-agent-docs: 1
zydecodb: 1.4.1
topic: tx
next: python, kv, pitfalls

# Bounded transactions

One pinned connection. Related by-id document writes plus raw KV, atomically.
Cap: ≤1024 keys. Not Mongo transactions. Not serializable MVCC.

## Legal

- KV `put` / `get` / `delete`
- Document put / delete / get-by-id / if-match variants
- Collections must already exist (`create_index` is DDL — rejected)

## Illegal (rejected)

- Filter `find` / `update_one` / `update_many` / `delete_one` / `delete_many`
- `aggregate`, `watch`, `create_index` / any DDL
- Nested begin

No automatic retries inside a transaction. Pin lasts until commit/rollback.

## Commit failure

If commit fails at the transport layer, the write may have landed. Raise /
return:

- Python / TypeScript: `UnknownCommitError`
- Go: `ErrUnknownCommitResult`

Reconcile by re-reading keys. Do not blindly retry the whole transaction.

## Drivers

```python
with db.transaction() as tx:
    tx.put(b"session", b"active")
    tx.put_document("users", "u1", {"n": 1})
```

```go
seq, err := db.WithTransaction(ctx, func(tx *zydecodb.Tx) error {
    if err := tx.Put(ctx, []byte("session"), []byte("active"), 0); err != nil {
        return err
    }
    return tx.PutDocument(ctx, "users", "u1", zydecodb.Document{"n": 1})
})
// or: tx, err := db.BeginTx(ctx)
```

```ts
const { seq } = await db.withTransaction(async (tx) => {
  await tx.put(Buffer.from("session"), Buffer.from("active"));
  await tx.putDocument("users", "u1", { n: 1 });
});
```

Older servers reject `Begin` with a protocol / invalid-request error.
