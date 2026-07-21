# ZydecoDB Python driver

Official Python client for [ZydecoDB](../../README.md). Pure standard library,
no runtime dependencies.

## Install

```bash
pip install zydecodb
```

Requires Python 3.9+. (Working from a checkout of this repo:
`pip install -e clients/python`.)

## Quick start

```python
from zydecodb import Client

# Plain TCP (localhost). For TLS: Client(..., api_key="YOUR_KEY", tls=True)
with Client("127.0.0.1", 9470, api_key="YOUR_KEY") as db:
    users = db.collection("users")
    users.create_index(["email"], unique=True)

    uid = users.insert_one({"email": "ada@example.com", "name": "Ada", "age": 30})

    for u in users.find({"age": {"$gte": 18}}, sort=[("age", True)]):
        print(u["name"], u["age"])

    users.update_one({"_id": uid}, {"$inc": {"age": 1}})
    print(users.count_documents())
```

## What you get

- **Connection pooling.** `Client` owns a thread-safe pool (`pool_size`,
  default 8) and is safe to share across threads.
- **Automatic retries with backoff.** Transient transport failures and server
  `EngineBusy` responses are retried (full-jitter exponential backoff) for
  operations that are safe to repeat. Operator updates and deletes are never
  retried automatically.
- **Keepalive.** Idle pooled connections are validated with a `Ping` on
  checkout and transparently replaced if dead.
- **Typed error taxonomy.** Non-OK responses raise a specific subclass:
  `ConflictError` (unique-index violation), `AuthError`, `ServerBusyError`,
  `InvalidRequestError`, or the base `ServerError` — each carrying the wire
  `status` byte. Transport problems raise `ConnectionError`.
- **`Collection` API.** `insert_one/many`, `find`/`find_one`,
  `update_one/many`, `delete_one/many`, `count_documents`, `distinct`,
  `create_index`, with `$`-operators, sort, projection, and skip/limit.
  Pagination is repeatable-read across pages.
- **Raw KV with TTL.** Side-channel `put` (with `expires_at`), `get`, and `delete` methods on `Client` for session data that needs a time-to-live.
- **TLS.** Pass `tls=True` for system CA defaults, or an `ssl.SSLContext` for custom roots / verification.

## Durability

Writes are durable (fsync-on-commit) by default. For latency-sensitive,
loss-tolerant writes, pass `relaxed=True` to acknowledge before the fsync.
It is available on every write: `insert_one`, `replace_one`, `update_one`,
`update_many`, `delete_one`, and `delete_many`.

```python
users.insert_one(doc, relaxed=True)
users.update_one({"_id": "ada"}, {"$inc": {"hits": 1}}, relaxed=True)
users.delete_many({"stale": True}, relaxed=True)
```

## Running the tests

Unit tests for the wire codecs need no server:

```bash
cd clients/python
pip install -e ".[dev]"
pytest tests/test_protocol.py
```

Integration tests run against a live server selected by environment variables
(skipped automatically if it is unreachable):

```bash
ZYDECODB_TEST_HOST=127.0.0.1 ZYDECODB_TEST_PORT=9470 pytest
```
