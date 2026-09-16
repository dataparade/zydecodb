zydecodb-agent-docs: 1
zydecodb: 1.2.0
topic: python
next: query, kv, tx, pitfalls

# Python driver

```bash
pip install zydecodb
```

Python 3.9+. Pin a version in production. `Client` is a thread-safe pool
(default `pool_size=8`).

```python
import os
from zydecodb import Client, ConflictError, generate_id

host = os.environ.get("ZYDECODB_TEST_HOST", "127.0.0.1")
port = int(os.environ.get("ZYDECODB_TEST_PORT", "9470"))

with Client(host, port) as db:
    users = db.collection("agent_python_users")
    users.create_index(["email"], unique=True)
    uid = users.insert_one({"email": "bo@example.com", "name": "Bo", "age": 25})
    users.insert_many([{"email": "cy@example.com", "age": 40}])
    print(users.find_one({"name": "Bo"}))
    print(list(users.find({"age": {"$gte": 18}}, sort=[("age", True)], limit=10)))
    users.update_one({"_id": uid}, {"$inc": {"age": 1}})
    print(users.count_documents())
    print(users.distinct("email"))
    print(generate_id())
    try:
        users.insert_one({"email": "bo@example.com"})
    except ConflictError:
        pass
```

`Client(host, port, api_key=...)` or `ZYDECODB_API_KEY`. TLS: `tls=True` or an
`ssl.SSLContext`.

## Collection

| Method | Notes |
|--------|--------|
| `create_index(fields, unique=False, expire_after_seconds=0)` | `fields` is `list[str]` or `list[(field, ascending)]` |
| `insert_one(doc, relaxed=False, expires_at=0)` | auto `_id`; returns id |
| `insert_many(docs)` | |
| `replace_one(id, doc, relaxed=False)` | full replace |
| `replace_one_if_match(id, doc, if_match=rev)` | optimistic; stale → `ConflictError` |
| `update_one(filter, update, relaxed=False, upsert=False)` | `$` operators only |
| `update_many(...)` | per-doc atomic; not one global txn |
| `update_by_id_if_match(id, update, if_match=rev)` | |
| `delete_one` / `delete_many` | |
| `find` / `find_one` | sort, projection, skip, limit; `find` auto-paginates |
| `find_with_revision` | yields `(doc, rev)` |
| `get(id)` / `get_with_revision(id)` | by-id fast path |
| `count_documents` / `distinct` | |
| `aggregate(pipeline)` | see `zydecodb --agent aggregate` |
| `watch(resume_token=None)` | see `zydecodb --agent watch` |

Revisions are opaque ints. `generate_id()` builds a time-ordered `_id`.

## Errors

`ConflictError`, `AuthError`, `ServerBusyError`, `InvalidRequestError`,
`ServerError`, `PolicyError`, `UnsupportedFormatError`. Transport:
`ConnectionError`. Commit-ack lost: `UnknownCommitError`.

## KV, durability, transactions

- Raw KV (TTL): `db.put(key, value, expires_at=unix_ms)`, `db.get`, `db.delete`.
- `relaxed=True` on writes acks before fsync (loss-tolerant).
- `with db.transaction() as tx:` — `zydecodb --agent tx`.
