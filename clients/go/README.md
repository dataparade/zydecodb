# ZydecoDB Go driver

Official Go client for [ZydecoDB](../../README.md) — a MongoDB-style document
store without the fluff. Standard library only, no third-party dependencies.

## Install

```bash
# In your go.mod, use a local replace or fetch from the repository once pushed:
go get github.com/dataparade/zydecodb/clients/go@latest
```

Requires Go 1.23+.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	zydecodb "github.com/dataparade/zydecodb/clients/go"
)

func main() {
	db := zydecodb.NewClient("127.0.0.1:9470", zydecodb.WithAPIKey("YOUR_KEY"))
	defer db.Close()

	ctx := context.Background()
	users := db.Collection("users")
	if _, err := users.CreateIndex(ctx, []string{"email"}, true); err != nil {
		log.Fatal(err)
	}

	id, err := users.InsertOne(ctx, zydecodb.Document{
		"email": "ada@example.com", "name": "Ada", "age": 30,
	}, false)
	if err != nil {
		log.Fatal(err)
	}

	adults, _ := users.Find(ctx, zydecodb.Document{"age": zydecodb.Document{"$gte": 18}},
		zydecodb.QueryOptions{Sort: []zydecodb.SortKey{{Field: "age", Ascending: true}}})
	for _, u := range adults {
		fmt.Println(u["name"], u["age"])
	}

	_, _ = users.UpdateOne(ctx, zydecodb.Document{"_id": id},
		zydecodb.Document{"$inc": zydecodb.Document{"age": 1}}, false)
}
```

## What you get

- **Connection pooling.** `Client` owns a bounded, concurrency-safe pool
  (`WithPoolSize`, default 8) and is safe to share across goroutines.
- **Automatic retries with backoff.** Transient transport failures and server
  `EngineBusy` responses are retried (full-jitter exponential backoff) for
  operations that are safe to repeat. Operator updates and deletes are never
  retried automatically. Every call honors its `context.Context` deadline and
  cancellation.
- **Keepalive.** Idle pooled connections are validated with a `Ping` on
  checkout and transparently replaced if dead.
- **Typed errors.** Non-OK responses return a `*ServerError` carrying the wire
  `Status` byte; use `IsConflict`, `IsAuth`, `IsBusy`, and `IsInvalidRequest`
  to branch. Transport problems return a `*ConnError`.
- **MongoDB-style `Collection` API.** `InsertOne/Many`, `Find`/`FindOne`,
  `UpdateOne/Many`, `DeleteOne/Many`, `CountDocuments`, `Distinct`, and
  `CreateIndex`, with `$`-operators, sort, projection, and skip/limit.
  Pagination is repeatable-read across pages.
- **Raw KV with TTL.** Side-channel `Put` (with `expiresAt`), `Get`, and `Delete` methods on `Client` for session data that needs a time-to-live.

## Durability

Writes are durable (fsync-on-commit) by default. For latency-sensitive,
loss-tolerant writes, pass `relaxed = true` on any write to acknowledge before
the fsync.

```go
users.InsertOne(ctx, doc, true)
users.UpdateOne(ctx, zydecodb.Document{"_id": "ada"},
	zydecodb.Document{"$inc": zydecodb.Document{"hits": 1}}, true)
```

## Examples

- [`examples/quickstart`](examples/quickstart) — end-to-end collection demo.
- [`examples/user_backend`](examples/user_backend) — a small `net/http` users
  API sharing one pooled client across concurrent requests.

```bash
go run ./examples/quickstart
go run ./examples/user_backend
```

Both read `ZYDECODB_ADDR` (default `127.0.0.1:9470`) and `ZYDECODB_API_KEY`.

## Running the tests

The codec is verified byte-for-byte against the shared
[conformance vectors](../conformance) — no server required:

```bash
cd clients/go
go test ./...
```

Integration tests run against a live server selected by environment variables
(skipped automatically if it is unreachable):

```bash
ZYDECODB_TEST_HOST=127.0.0.1 ZYDECODB_TEST_PORT=9470 go test ./...
```
