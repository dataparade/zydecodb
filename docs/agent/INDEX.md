zydecodb-agent-docs: 1
zydecodb: 1.4.0
topic: index
next: python, query, pitfalls

# ZydecoDB — agent index

ZydecoDB is a document store on an LSM engine. Official drivers (Python, Go,
TypeScript) speak a length-prefixed binary protocol to `127.0.0.1:9470`. It is
not MongoDB, not Postgres, and not an HTTP API. Use an official driver. Do not
hand-roll the wire.

## Start

```bash
curl -sSL https://zydeco.dev/install.sh | sh
zydecodb serve          # loopback :9470; state in ~/.zydecodb/; auth optional
```

Then `pip install zydecodb` (or the Go / npm driver). Fetch a language page:
`zydecodb --agent python`.

## Canonical example (Python)

Host/port default to localhost. Override with `ZYDECODB_TEST_HOST` /
`ZYDECODB_TEST_PORT` when the server is not on 9470.

```python
import os
from zydecodb import Client

host = os.environ.get("ZYDECODB_TEST_HOST", "127.0.0.1")
port = int(os.environ.get("ZYDECODB_TEST_PORT", "9470"))

with Client(host, port) as db:
    users = db.collection("agent_index_users")
    users.create_index(["email"], unique=True)
    uid = users.insert_one({"email": "ada@example.com", "name": "Ada", "age": 30})
    print(users.find_one({"_id": uid}))
    users.update_one({"_id": uid}, {"$inc": {"age": 1}})
    print(users.count_documents({"age": {"$gte": 30}}))
```

## Security

Put your HTTP API in front. Hold `ZYDECODB_API_KEY` in the app. Never expose
`:9470` to the public internet. Auth is required automatically when `listen` is
not loopback.

## Hard no's

- Not Mongo aggregation: no `$unwind`, no `pipeline:`/`let:` `$lookup`, no `$facet`.
- No Raft / consensus / autonomous failover.
- No general MVCC or serializable isolation.
- Transactions are bounded per-connection (by-id + KV, ≤1024 keys). No filter
  queries or DDL inside a transaction.
- No collection schemas (`SchemaDef` is reserved).
- Multi-document `update_many` / `delete_many` is not one global transaction.

## Topics

Run `zydecodb --agent <topic>`:

| Topic | What you get |
|-------|----------------|
| `python` | Python driver |
| `go` | Go driver |
| `typescript` | TypeScript / Node driver |
| `query` | Filters, find, updates, indexes |
| `kv` | Raw KV + TTL |
| `tx` | Bounded transactions |
| `watch` | Change streams |
| `aggregate` | Legal pipelines only |
| `ops` | serve, keys, Docker |
| `pitfalls` | What not to invent |
