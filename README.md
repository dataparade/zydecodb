<div align="center">

<img src="docs/assets/logo_black.jpeg" alt="ZydecoDB" width="520">

**Document store on an LSM engine**

[![CI](https://github.com/dataparade/zydecodb/actions/workflows/ci.yml/badge.svg)](https://github.com/dataparade/zydecodb/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-BSL_1.1-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange)](https://www.rust-lang.org/)
[![PyPI](https://img.shields.io/pypi/v/zydecodb)](https://pypi.org/project/zydecodb/)
[![npm](https://img.shields.io/npm/v/zydecodb)](https://www.npmjs.com/package/zydecodb)
[![Cloud](https://img.shields.io/badge/ZydecoDB-Cloud-1d4ed8)](https://zydecodb.com)

</div>

**ZydecoDB** is a source-available database written in **Rust**. It runs as a standalone server — any language that speaks TCP can talk to it.

Two layers, one engine:

- **Document store** — collections of JSON documents with a filter/query layer: `$`-operators, sort/projection/pagination, partial updates (`$set`/`$inc`/`$unset`/`$push`/`$setOnInsert`), `count`/`distinct`, and secondary indexes the server keeps in sync automatically. Any field is queryable, indexed or not.
- **Key-value core** — the LSM storage engine underneath: ordered keys, atomic multi-key batches, snapshots, and WAL crash recovery.

**License:** [BSL 1.1](LICENSE) (converts to Apache 2.0 on 2029-06-07). Self-hosting is free. For a managed instance, use **[ZydecoDB Cloud](https://zydecodb.com)**.

<div align="center">

[Quick start](#quick-start) •
[Drivers](#more-docs) •
[Operator guide](docs/GUIDE.md) •
[Protocol](docs/PROTOCOL.md) •
[Examples](examples/README.md) •
[Compatibility](docs/COMPATIBILITY.md) •
[Cloud](https://zydecodb.com)

</div>

## Quick start

```bash
# 1. Install the prebuilt binary (Linux/macOS, x86_64/arm64)
curl -sSL https://zydeco.dev/install.sh | sh

# Later: upgrade the server binary in place (drivers stay on pip/npm/go get)
# zydecodb update

# 2. Start the server — no config needed for local use
zydecodb serve          # listens on 127.0.0.1:9470, data in ~/.zydecodb
```

Then grab a driver and make your first write (Python shown; also on [npm](clients/typescript) and [Go](clients/go)):

```bash
pip install zydecodb
```

```python
from zydecodb import Client

with Client("127.0.0.1", 9470) as db:
    users = db.collection("users")

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

Wire protocol: length-prefixed binary frames (see `zydecodb-engine::frame`) on `127.0.0.1:9470`.

### Build from source

```bash
cargo build --release -p zydecodb
./target/release/zydecodb serve                          # local defaults, or:
cp config/zydecodb.dev.toml /tmp/zydecodb.toml
./target/release/zydecodb serve --config /tmp/zydecodb.toml
```

### Try the examples

```bash
# Terminal 1 — database (above)

# Terminal 2 — user-management HTTP API
pip install -r examples/user_backend/requirements.txt
python3 examples/user_backend/app.py --seed
```

See [`examples/README.md`](examples/README.md) for the full walkthrough.

### API keys (when auth is required)

```bash
zydecodb admin keys create --id backend --role read_write --keys-file /tmp/zydecodb-keys.toml
export ZYDECODB_API_KEY="zdk_..."   # save the key printed once
python3 examples/user_backend/app.py --seed
```

Details: [`docs/GUIDE.md`](docs/GUIDE.md#security).

### Docker

```bash
# Create API keys first (auth is required in the Docker config)
./target/release/zydecodb admin keys create \
  --id docker --role admin --keys-file config/keys.toml

docker compose up -d --build
```

Compose publishes `:9470` only. Metrics stay on loopback inside the container. See [`docs/GUIDE.md`](docs/GUIDE.md#docker).

## Features

**Document store**
- JSON document collections with auto-generated time-ordered `_id`
- Filters: `$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$exists`, implicit-AND, `$and/$or/$not`, dotted paths
- `find` with sort, projection, skip/limit, and cursor pagination; `find_one`, `count_documents`, `distinct`
- Partial updates (`$set/$inc/$unset/$push/$setOnInsert`), filter upsert, `update_one/many`, `delete_one/many`
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
- Optional off-box WAL backup — [`docs/GUIDE.md`](docs/GUIDE.md#wal-shipping-and-restore)

**Operations**
- API-key auth, tenant isolation, TLS, rate limits, audit logging
- Durability you choose: `sync` (fsync-on-commit, default) or `periodic` (bounded-loss, higher throughput), plus a per-write `relaxed` flag
- Prometheus `/metrics` plus `/healthz`/`/readyz` HTTP endpoints; optional per-tenant request counters
- Exclusive `data_dir` lock (no accidental double-open) and graceful `SIGTERM`/`SIGINT` shutdown that writes a clean-shutdown marker
- Read replicas via WAL shipping with assisted failover: a liveness heartbeat, a `replica status` health probe, and `replica promote` with a cooperative epoch fence — [`docs/GUIDE.md`](docs/GUIDE.md#replication-and-failover)
- Base snapshots and point-in-time restore (`admin snapshot` / `admin restore --to-seq|--to-time`) — [`docs/GUIDE.md`](docs/GUIDE.md#wal-shipping-and-restore)

**Multi-tenant hosting (pods)**
- One process can host many tenants. Operational levers are `zydecodb admin ...` subcommands an external control plane shells out to:
  - `admin drop-tenant --tenant <hex> [--compact]` — offline offboard (node stopped)
  - `admin drop-tenant --live --tenant <hex> [--compact]` — live offboard via running server (`ZYDECODB_API_KEY` admin; prefers `listen_unix`)
  - `admin tenant set-limit --tenant <hex> [--max-bytes N] [--rate-rps R]` / `admin tenant list` — per-tenant byte cap and request-rate ceiling (reloaded live on `SIGHUP`)
- Optional Unix-domain-socket listener (`listen_unix`) for local control-plane traffic without a per-instance TCP port
- A `[runtime] profile = "low_footprint"` that trims cache, open readers, and idle wakeups for dense multi-instance boxes

**Multi-tenant sharing model (read this):** tenants get **namespace isolation** (key prefix, ACLs, byte/RPS quotas, drop-tenant). Write/catalog mutations still serialize on the engine write lock; block cache, fair-share accounting, and WAL fsync are separate domains. δ-fair memtable/cache/stall isolation is **off by default** for local/single-tenant; pods hosts should start from [`config/zydecodb.pods.example.toml`](config/zydecodb.pods.example.toml) (`[fair] enabled = true`) and follow the one-page runbook [`docs/GUIDE.md`](docs/GUIDE.md#multi-tenant-pods). Until fair is on and soak-proven, do not assume one tenant’s write storm cannot affect another’s latency. See [`docs/GUIDE.md`](docs/GUIDE.md#multi-tenant-sharing-model).

## 0.11 scope

**Today:** single-node document + KV database, binary protocol, API-key auth (optional on localhost). Filters, sort, projection, pagination, partial updates, `count`/`distinct`, bounded `$match`→`$group` aggregation, optional primary-only change streams, and automatic index maintenance; three official drivers (Python, Go, TypeScript). Queries are correct on any field (collection scan) and fast when an index fits. See [`docs/PROTOCOL.md`](docs/PROTOCOL.md#aggregation) and [`docs/PROTOCOL.md`](docs/PROTOCOL.md#change-streams).

**Not yet:** Mongo-compatible aggregation (`$lookup`/`$unwind`/expressions), general MVCC/multi-document transactional queries (bounded by-ID+KV transactions ship), and *autonomous* failover (promotion is assisted — an orchestrator decides death and does hard fencing; the database automates draining, the epoch fence, and the role switch). δ-fair multi-tenant isolation clears the simulated pods soak ship bar (steady victim put p99 δ ≤ 50 ms with `[fair]` on) and the FairDB-style ramp-up reclaim gate (≤ 350 ms) but stays **off by default** — enable via [`config/zydecodb.pods.example.toml`](config/zydecodb.pods.example.toml) and prove on your hardware via `scripts/tenant-isolation-soak.sh` (`MODE=rampup` for reclaim). See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the gap list and roadmap. Field-based TTL indexes (`expireAfterSeconds` on a unix-millis number field) and per-document DocPut `expires_at` are supported in the server and official drivers. TTL compaction reclamation ships for SST expiry drops.

## Expectations, gotchas, advice

- **1.0.0.** Wire `proto_version = 1` opcodes, write flags, and status bytes
  are **frozen for 1.x** (append-only; see
  [`docs/PROTOCOL.md`](docs/PROTOCOL.md#wire-protocol) and
  [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)). Reserved opcodes may gain
  semantics without renumbering. On-disk format changes follow
  [`docs/GUIDE.md`](docs/GUIDE.md#upgrading).
- **BSL license.** Self-hosting (including in production) is free; you may not offer ZydecoDB to third parties as a competing hosted/managed service. Converts to Apache 2.0 on the change date — see [LICENSE](LICENSE).
- **Security:** run behind your API on localhost or a private network. See [`docs/GUIDE.md`](docs/GUIDE.md#security). Do not expose `:9470` to the internet without auth.
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
- [`docs/GUIDE.md`](docs/GUIDE.md) — security, pods, replication, WAL shipping/restore, upgrades
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) — document layer, wire protocol, aggregation, change streams
- [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) — 1.x semver contract, releases, and pinning
- [`CHANGELOG.md`](CHANGELOG.md) — server and driver release notes
- [`docs/INTERNAL.md`](docs/INTERNAL.md) — architecture pillars and soak testing (contributors)
