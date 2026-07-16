# ZydecoDB

Source-available database written in Rust (BSL 1.1). Runs as a standalone server — use any language that speaks TCP.

Two layers, one engine:

- **Document store** — collections of JSON documents with a MongoDB-style query layer: filter with `$`-operators, sort/projection/pagination, partial updates (`$set`/`$inc`/`$unset`/`$push`), `count`/`distinct`, and secondary indexes the server keeps in sync automatically. Any field is queryable, indexed or not.
- **Key-value core** — the LSM storage engine underneath: ordered keys, atomic multi-key batches, snapshots, and WAL crash recovery.

**License:** [BSL 1.1](LICENSE) (converts to Apache 2.0 on 2029-06-07)

## Quick start

```bash
cargo build --release -p zydecodb
cp config/zydecodb.dev.toml /tmp/zydecodb.toml
./target/release/zydecodb serve --config /tmp/zydecodb.toml
```

Default listen: `127.0.0.1:9470`. Wire protocol: length-prefixed binary frames (see `zydecodb-engine::frame`).

### Try the Python examples

```bash
# Terminal 1 — database (above)

# Terminal 2 — user-management HTTP API
pip install -r examples/user_backend/requirements.txt
python3 examples/user_backend/app.py --seed
```

See [`examples/README.md`](examples/README.md) for the full walkthrough.

If you know MongoDB, you already know the driver (Python client shown):

```python
users = db.collection("users")
users.create_index(["age"])                          # optional; speeds up age queries

users.insert_one({"name": "Ada", "age": 30, "city": "London"})   # auto _id, returned
users.insert_many([{ "name": "Bo", "age": 25 }, { "name": "Cy", "age": 40 }])

users.find_one({"name": "Ada"})                                  # filter by any field
users.find({"age": {"$gte": 30}}, sort=[("age", True)], limit=10) # operators + sort, auto-paginated
users.find({"city": "London"}, projection={"name": 1})           # works even with no index on city

users.update_one({"name": "Ada"}, {"$inc": {"age": 1}})          # partial update
users.count_documents({"age": {"$gte": 30}})
users.distinct("city")
users.delete_many({"age": {"$lt": 18}})
```

### API keys (when auth is required)

```bash
zydecodb admin keys create --id backend --role read_write --keys-file /tmp/zydecodb-keys.toml
export ZYDECODB_API_KEY="zdk_..."   # save the key printed once
export PYTHONPATH=clients/python
python3 examples/user_backend/app.py --seed
```

Details: [`docs/SECURITY.md`](docs/SECURITY.md).

## Features

**Document store**
- JSON document collections with auto-generated time-ordered `_id`
- MongoDB-style filters: `$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$exists`, implicit-AND, `$and/$or/$not`, dotted paths
- `find` with sort, projection, skip/limit, and cursor pagination; `find_one`, `count_documents`, `distinct`
- Partial updates (`$set/$inc/$unset/$push`), `update_one/many`, `delete_one/many`
- A query planner that uses an index (or `_id` lookup) when one fits and falls back to a collection scan otherwise — so any field is queryable
- Secondary indexes maintained automatically and atomically on every write; synchronous backfill when added to an existing collection
- **Unique indexes** enforced server-side (`create_index(..., unique=True)` → `Conflict` on duplicates)
- **Repeatable-read pagination** — a cursor pins its snapshot, so later pages never shift under concurrent writes
- Official drivers for Python ([`clients/python`](clients/python)), Go ([`clients/go`](clients/go)), and TypeScript/Node ([`clients/typescript`](clients/typescript)) — connection pooling, retries, and a typed error taxonomy, all verified byte-for-byte against shared [conformance vectors](clients/conformance)

**Key-value core**
- `put`, `get`, `delete` over TCP with optional TTL (`expires_at`)
- *(Engine/Admin only)* Atomic multi-key writes (`write_batch`) — one WAL record, all-or-nothing on crash
- *(Engine/Admin only)* Ordered range scans and point-in-time snapshots
- Crash recovery (WAL replay)
- Optional off-box WAL backup — [`docs/SHIPPING.md`](docs/SHIPPING.md)

**Operations**
- API-key auth, tenant isolation, TLS, rate limits, audit logging
- Durability you choose: `sync` (fsync-on-commit, default) or `periodic` (bounded-loss, higher throughput), plus a per-write `relaxed` flag
- Prometheus `/metrics` plus `/healthz`/`/readyz` HTTP endpoints; optional per-tenant request counters
- Exclusive `data_dir` lock (no accidental double-open) and graceful `SIGTERM`/`SIGINT` shutdown that writes a clean-shutdown marker
- Read replicas via WAL shipping with assisted failover: a liveness heartbeat, a `replica status` health probe, and `replica promote` with a cooperative epoch fence — [`docs/REPLICATION.md`](docs/REPLICATION.md)
- Base snapshots and point-in-time restore (`admin snapshot` / `admin restore --to-seq|--to-time`) — [`docs/SHIPPING.md`](docs/SHIPPING.md)

**Multi-tenant hosting (pods)**
- One process can host many tenants. Operational levers are `zydecodb admin ...` subcommands an external control plane shells out to:
  - `admin drop-tenant --tenant <hex> [--compact]` — offboard a tenant (delete its data + catalog entries)
  - `admin tenant set-limit --tenant <hex> [--max-bytes N] [--rate-rps R]` / `admin tenant list` — per-tenant byte cap and request-rate ceiling (reloaded live on `SIGHUP`)
- Optional Unix-domain-socket listener (`listen_unix`) for local control-plane traffic without a per-instance TCP port
- A `[runtime] profile = "low_footprint"` that trims cache, open readers, and idle wakeups for dense multi-instance boxes

## Beta scope

**Today:** single-node document + KV database, binary protocol, API-key auth (optional on localhost). MongoDB-style filters, sort, projection, pagination, partial updates, `count`/`distinct`, and automatic index maintenance; three official drivers (Python, Go, TypeScript). Queries are correct on any field (collection scan) and fast when an index fits.

**Not yet:** aggregation pipeline (`$group`/`$lookup`/`$unwind`), `$regex`/`$type`/array operators, upsert, document TTL, MVCC/multi-document transactions, and *autonomous* failover (promotion is assisted — an orchestrator decides death and does hard fencing; the database automates draining, the epoch fence, and the role switch). See [`docs/DOCUMENT_STORE.md`](docs/DOCUMENT_STORE.md) for the gap list and roadmap.

## Expectations, gotchas, advice

- **Beta** (`0.9.0-beta.1`). API, wire protocol, and on-disk format may change before 1.0.
- **BSL license.** Self-hosting (including in production) is free; you may not offer ZydecoDB to third parties as a competing hosted/managed service. Converts to Apache 2.0 on the change date — see [LICENSE](LICENSE).
- **Security:** run behind your API on localhost or a private network. See [`docs/SECURITY.md`](docs/SECURITY.md). Do not expose `:9470` to the internet without auth.
- **Keys on the wire** are opaque bytes; the server stores them under the user keyspace (`KS_USER` prefix).
- **Heavy sustained writes** can leave extra small on-disk files. Cosmetic — no data loss.

## Embedding

Power users can link the storage core crate (`zydecodb-engine`) in Rust. The server binary is the supported product surface for everyone else.

## Development

```bash
cargo test --workspace
```

## More docs

- Official drivers: [`clients/python`](clients/python/README.md), [`clients/go`](clients/go/README.md), [`clients/typescript`](clients/typescript/README.md) — each with pooling, retries, and typed errors
- [`clients/conformance/README.md`](clients/conformance/README.md) — shared wire conformance vectors that keep every driver byte-compatible with the server
- [`examples/README.md`](examples/README.md) — client and user-backend walkthroughs
- [`docs/DOCUMENT_STORE.md`](docs/DOCUMENT_STORE.md) — document layer architecture, gaps, and roadmap
- [`docs/SECURITY.md`](docs/SECURITY.md) — auth, TLS, tenants, deployment
- [`docs/SHIPPING.md`](docs/SHIPPING.md) — off-box WAL durability for disaster recovery
- [`docs/REPLICATION.md`](docs/REPLICATION.md) — read replicas, WAL shipping, and the failover runbook
