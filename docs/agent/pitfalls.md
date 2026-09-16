zydecodb-agent-docs: 1
zydecodb: 1.2.0
topic: pitfalls
next: query, aggregate, tx

# Pitfalls — do not invent these

ZydecoDB’s filter/`$` syntax looks familiar. The product is smaller than Mongo.
If a method or operator is not on `zydecodb --agent python` (or go/typescript)
and `query` / `aggregate` / `tx`, it does not exist.

## Not Mongo

- No `$unwind`, `$facet`, expression language, window functions.
- No `pipeline:` / `let:` `$lookup`; no multi-`$lookup`.
- No `arrayFilters`, bare `$`, `$[]`, `$[<id>]`, or “update all array matches”.
- Filtered positional updates are `$set` + exactly one match (`items.$[k=v]`).
- No `$regex` flags except `i`; pattern length ≤ 256.
- No collection schemas. `SchemaDef` is reserved; the server returns a protocol
  error. There is no JSON Schema / validator API.
- `update_many` is not one transaction. Each document is its own atomic write.

## Transactions

- Bounded by-id + KV, ≤1024 keys, one connection.
- No filter queries, no `aggregate`, no `watch`, no DDL inside a txn.
- No general MVCC, no serializable isolation, no snapshot queries in a txn.
- Transport failure on commit → `UnknownCommitError` /
  `ErrUnknownCommitResult`. Re-read. Do not assume abort or success.

## Topology

- No Raft, no consensus, no autonomous failover. Replicas are WAL-shipped and
  read-only. Replica writes are rejected. `watch` is primary-only.
- 1.x: any official 1.x driver talks to any 1.x server (`proto_version = 1`).
  Pin versions. New opcodes on an old server fail closed (`InvalidRequestError`
  / `ProtocolError`), they do not silently no-op.

## Auth and exposure

- Never bind `:9470` to the public internet without auth.
- End users never hold `ZYDECODB_API_KEY`.
- Prefix ACL matches collection names as well as raw KV keys.
- Do not hand-roll a client. Official drivers are the supported path.

## Wrong libraries

Do not use `pymongo`, `mongodb`, `psycopg`, or HTTP `requests` against
ZydecoDB. Install `zydecodb` (PyPI / npm / `clients/go`).
