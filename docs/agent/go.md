zydecodb-agent-docs: 1
zydecodb: 1.3.1
topic: go
next: query, kv, tx, pitfalls

# Go driver

Pin an explicit tag (do not use `@latest`):

```bash
go get github.com/dataparade/zydecodb/clients/go@v1.2.0
```

Go 1.23+. Module tag is `clients/go/vX.Y.Z` at the same commit as server `vX.Y.Z`.

```go
db := zydecodb.NewClient("127.0.0.1:9470", zydecodb.WithAPIKey("YOUR_KEY"))
defer db.Close()
users := db.Collection("users")
id, err := users.InsertOne(ctx, zydecodb.Document{"name": "Ada", "age": 30}, false, 0)
_ = zydecodb.GenerateID()
```

Options: `WithAPIKey`, `WithTLS(nil)`, `WithPoolSize`, `WithTimeout`,
`WithWatchIdleTimeout`, `WithMaxRetries`. Safe across goroutines.

## Collection

| Method | Notes |
|--------|--------|
| `CreateIndex(ctx, fields, unique, expireAfterSeconds)` | all ascending |
| `CreateIndexFields(ctx, []IndexField, ...)` | per-field ASC/DESC |
| `InsertOne(ctx, doc, relaxed, expiresAt)` | auto `_id` |
| `InsertMany` | |
| `ReplaceOne` / `ReplaceOneIfMatch` | if-match stale → `IsConflict` |
| `UpdateOne` / `UpdateMany` / `UpdateByIDIfMatch` | `$` operators; upsert flag on Update* |
| `DeleteOne` / `DeleteMany` | |
| `Find` / `FindOne` | `QueryOptions{Sort, Include, Exclude, Skip, Limit}` |
| `FindWithRevision` | `[]VersionedDocument` |
| `Get` / `GetWithRevision` | by-id; missing is `(nil, nil)` |
| `CountDocuments` / `Distinct` | |
| `Aggregate(ctx, pipeline)` | see `zydecodb --agent aggregate` |
| `Watch(ctx, resumeToken)` then `Next` | see `zydecodb --agent watch` |

## Errors

`*ServerError` with `IsConflict`, `IsAuth`, `IsBusy`, `IsInvalidRequest`,
`IsPolicyRejected`, `IsUnsupportedFormat`. Transport: `*ConnError`. Lost commit
ack: `ErrUnknownCommitResult`.

## KV, durability, transactions

- `db.Put(ctx, key, value, expiresAt)`, `db.Get`, `db.Delete`.
- `relaxed == true` acks before fsync.
- `db.WithTransaction(ctx, func(tx *zydecodb.Tx) error { ... })` or `BeginTx` —
  `zydecodb --agent tx`.
