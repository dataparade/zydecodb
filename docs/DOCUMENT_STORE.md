# ZydecoDB Document Store — Architecture

How the document layer works today: collections of JSON documents with a
query/index layer, riding on the durable LSM key-value engine.

**Audience:** Engineers working on collections, indexes, query execution, or the
wire protocol.

**Related:** [`docs/SECURITY.md`](SECURITY.md), [`docs/REPLICATION.md`](REPLICATION.md),
[`docs/SHIPPING.md`](SHIPPING.md). Pre-implementation design notes and ADRs live
in [`docs/archive/`](archive/) and are lineage only.

---

## The three layers

```text
┌──────────────────────────────────────────────────────────────┐
│  zydecodb (TCP/UDS server)                                     │
│  SessionInit / tenants / ACL / rate limits / quotas            │
│  docdispatch.rs routes the document opcodes (Find/Update/...)  │
└───────────────────────────────┬──────────────────────────────┘
                                 │
┌───────────────────────────────▼──────────────────────────────┐
│  zydecodb-document                                             │
│  catalog · encoding · keys · store · planner · query · update  │
│  parse doc → body + N index keys → ONE Engine::write_batch     │
└───────────────────────────────┬──────────────────────────────┘
                                 │
┌───────────────────────────────▼──────────────────────────────┐
│  zydecodb-engine                                               │
│  WAL · memtable · SSTables · compaction · snapshots · TTL      │
│  put/get/del/scan/prefix_scan · write_batch · WritePolicy hook │
└──────────────────────────────────────────────────────────────┘
```

| Crate | Role |
|-------|------|
| `zydecodb-engine` | Storage core: WAL, memtable, SSTables, compaction, point ops, range/prefix scan, atomic `write_batch`, snapshots, system keyspace, `WritePolicy` hook |
| `zydecodb-document` | Document model: collection/index catalog, JSON body + index-key assembly, planner, query execution, partial updates |
| `zydecodb` | `serve`, session/auth, tenant key prefixing, ACLs, rate limits, quotas, the admin CLI, and `docdispatch.rs` (document opcode routing) |

---

## Data model

| Concept | Detail |
|---------|--------|
| Collection | A named namespace; documents and indexes live under a per-collection key prefix from the catalog |
| Document | A JSON object |
| `_id` | Auto-generated, time-ordered (sortable, roughly insertion-ordered); a virtual always-present field equal to the document key. Filterable like any field |
| Body storage | The document body is stored as a **zero-copy ZDoc binary format** behind a one-byte `value_kind` discriminator (`VK_ZDOC = 0x01`). Legacy JSON bytes (`VK_RAW = 0x00`) are still supported for backwards compatibility, but all new writes and updates are compiled to ZDoc. The ZDoc format eliminates JSON parsing overhead during queries and updates by allowing O(log N) field lookups directly against raw bytes. |
| Index entry | Order-preserving encoded field value(s) → document id (see [Indexes](#indexes)) |

The engine itself stays byte-opaque: it stores values as bytes and never parses
JSON. All document semantics live in `zydecodb-document`.

---

## Query language

Filters are JSON documents (`filter.rs`):

| Category | Supported |
|----------|-----------|
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type` |
| Array | `$all` `$elemMatch` |
| String | `$regex` (gated: max pattern length 256, `i` flag only, string fields, residual scan) |
| Logical | implicit-AND (`{a: 1, b: 2}`), `$and`, `$or`, `$not` (wraps one sub-filter) |
| Paths | dotted (`"address.city"`); `_id` is a virtual always-present field |

Comparisons use the same cross-type order as the index encoding
(null < bool < number < string), so filter semantics and index ordering agree.

### Planner (`planner.rs`)

The planner picks, in order: an `_id` lookup, then the index with the longest
equality-prefix match (including compound indexes) plus an optional range on the
next field, otherwise a full collection scan. The planner only affects **speed**:
every candidate document is re-checked against the complete filter, so any field
is queryable whether it is indexed or not.

### Execution (`query.rs`)

- `find` with sort, projection (include/exclude), and `skip`/`limit`.
- **Cursor pagination** that is **repeatable-read**: a cursor carries both the
  position and the snapshot-sequence ceiling, and the next page re-pins the same
  read view via `Engine::snapshot_at`. Later pages never shift under concurrent
  writes. Index-ordered pages key-stream; otherwise a bounded offset applies,
  capped by `MAX_SORT_BUFFER`.
- `count` and `distinct`.
- `find_one` is a `find` with `limit = 1`.

### Writes (`update.rs`, `store.rs`)

- Update operators: `$set`, `$inc`, `$unset`, `$push`, `$setOnInsert`. Bare
  (non-`$`) update documents are rejected. `$setOnInsert` applies only when an
  upsert inserts; normal updates ignore it. On insert, `$setOnInsert` runs
  before regular ops so `$set`/`$inc`/`$unset`/`$push` win on path conflicts.
- `update_one` / `update_many`, `delete_one` / `delete_many`: candidate ids come
  from a lock-free snapshot, then **each matched document is rewritten in one
  atomic `write_batch`** (body + all of its index keys). Per-document writes are
  atomic; a multi-document update is **not** globally atomic.
- Filter upsert (`FLAG_UPSERT` on Update): when no document survives the
  under-lock filter recheck, insert at most one document built from top-level
  equality fields in the filter plus the operator update (including
  `$setOnInsert`). Response includes `upserted_id` on insert; omit it on a
  normal update.

---

## Indexes

- **Secondary indexes** maintained automatically and atomically on every write:
  the document body and every affected index key move in a single
  `Engine::write_batch` (one WAL record, one CRC), so a crash can never leave an
  index disagreeing with its document.
- **Compound indexes** supported; the planner can use an equality prefix plus one
  trailing range.
- **Unique indexes** (`create_index(..., unique=True)`) are enforced server-side;
  a duplicate key returns `Conflict`.
- **Synchronous backfill**: adding an index to a populated collection indexes the
  existing documents before the call returns.
- **Order-preserving encoding** (`encoding.rs`): scalar field values encode so
  that lexicographic byte order equals logical order, and encodings are
  prefix-free, so composite keys and the trailing doc-id suffix never disturb
  field ordering. Non-scalar fields (objects, arrays) sort as `null` and are not
  usefully indexable.

Indexes are not free: extra keys mean write amplification, more compaction, and
more disk. That cost is deliberate and synchronous, not deferred to a background
indexer.

---

## Wire protocol

### Envelope

```text
[1] protocol version (0x01)
[1] command code
[4] payload length (u32 big-endian)
[N] payload
```

### Command codes (`frame.rs`)

| Code | Command | Status |
|------|---------|--------|
| `0x01` | `Put` | Implemented (raw KV) |
| `0x02` | `Get` | Implemented (raw KV) |
| `0x03` | `Del` | Implemented (raw KV) |
| `0x20` | `Query` | Implemented (document layer) |
| `0x21` | `DocPut` | Implemented (document upsert; optional `expires_at` trailer) |
| `0x22` | `DocDel` | Implemented (document delete) |
| `0x23` | `Find` | Implemented (filter + sort/projection/pagination) |
| `0x24` | `Update` | Implemented (filter-based partial update; `FLAG_UPSERT`) |
| `0x25` | `Delete` | Implemented (filter-based delete) |
| `0x26` | `Count` | Implemented (count / distinct) |
| `0x30` | `IndexDef` | Implemented (index create + backfill) |
| `0x40` | `SessionInit` | Implemented (API-key auth handshake) |
| `0x41` | `SetContext` | Implemented (admin tenant switch) |
| `0x42` | `AdminDropTenant` | Implemented (live tenant offboard; admin path) |
| `0xF0` | `Ping` | Implemented |
| `0xF1` | `Stats` | Implemented |
| `0x10`–`0x12` | `Begin`/`Commit`/`Rollback` | Reserved (parseable; `ProtocolError` until transactions) |
| `0x31` | `SchemaDef` | Reserved (parseable; `ProtocolError` until schemas) |

**0.9 freeze:** implemented opcodes above, write flags (`FLAG_RELAXED=0x01`,
`FLAG_UPSERT=0x02`; unused flag bits must be zero), and status bytes in
`zydecodb-engine::errors` (including `PolicyRejected` / `UnsupportedFormat`) are
**frozen for 0.9.x** — append-only, no renumbering. Reserved slots and the
Not-yet list may gain semantics later without changing existing codes. On-disk
format upgrades follow [`UPGRADE.md`](UPGRADE.md). Official drivers do not yet
expose every admin opcode or DocPut TTL; freeze is the wire contract, not full
driver coverage.

Payload codecs are in `zydecodb-document/src/wire.rs`. The official drivers (Python, Go, TypeScript) are the intended product surface; the binary wire sits behind them.

### Storage key layout

Clients send **logical keys**; the server prepends the keyspace + tenant before
the engine sees them:

```text
storage_key = 0x01 | tenant[16] | client_key     # multi-tenant
            | 0x01 | client_key                   # legacy_single_tenant (tenant all-zero)
```

Within a tenant the document layer lays out:

```text
doc:<collection>:<doc_id>                       → value_kind || JSON body
idx:<collection>:<index>:<encoded_value(s)>     → doc_id
```

Do not embed the `0x01` (`KS_USER`) prefix in client keys — the server adds it.
Catalog and bookkeeping records live in the system keyspace (`0x00`,
`KS_SYSTEM`) via `sys_*`.

---

## Concurrency model

The server holds the engine as
[`EngineHandle`](../crates/zydecodb-engine/src/engine_handle.rs)
(`write` mutex plus separate cache / fair / WAL-sync domains) and serves each
connection on its own thread (`spawn_tcp_conn` / `spawn_uds_conn`), bounded by
`security.max_connections`. Queries are **two-phase**: the planner takes a
consistent snapshot under the write lock, then the scan runs lock-free against
that pinned view, so a long scan does not block other clients' writes. This is
why pagination can be repeatable-read across pages.

### Single write lane (by design)

ZydecoDB is **single-node, single-write-lane** on purpose. Every mutation
funnels through the engine write mutex, so writes are totally ordered by a
single `seq` counter and there is exactly one writer at any instant. The target
deployment is **one application per database**, where this is the right trade:

- **Reads scale** via the two-phase snapshot path above (lock held only to pin a
  snapshot; the scan is lock-free).
- **Write throughput** is bounded by fsync latency, not by core count. It is
  widened — not by adding writers — through **group commit** (many pending
  commits share one fsync) and the **relaxed durability** knob below, never by a
  second write path.

This is a deliberate ceiling, not an oversight. Do **not** introduce a write
path that bypasses the engine lock or allocates `seq` out of band: it would
break the total order that crash recovery, snapshots, and pagination all rely
on. Sharding into multiple independent write lanes is explicitly out of scope
for the single-node product.

The payoff of one lane is that **multi-document transactions** (opcodes
`0x10`–`0x12`, reserved) become tractable later: stage N operations under the
lock and commit them as one atomic WAL batch (the engine's existing
all-or-nothing `WAL_BATCH` primitive), with no cross-shard coordination to
reason about.

### Durability is per-write

Durability is chosen per commit, not globally. `sync` mode (default) fsyncs
before acknowledging; `periodic` mode acks after the buffered append and fsyncs
on an interval. Independently, any single write may pass a **`relaxed`** flag to
acknowledge before its fsync (see `crates/zydecodb/src/commit.rs`). `relaxed` is
available on every user write — inserts, replaces, filter-based updates, and
filter-based deletes. DDL (`IndexDef`) and delete-by-id (`DocDel`) are always
made durable before acknowledging.

---

## Design notes

### `WritePolicy` is a gate, not an index engine

`WritePolicy` (`zydecodb-engine/src/policy.rs`) runs around a single user write:

- `pre_write` rejects before any WAL/memtable mutation (size/validation gates,
  per-tenant byte quotas).
- `post_write` does bookkeeping after the primary write is in the memtable.

Policy-side durable writes use `sys_put_policy`, which only accepts system-keyspace
(`0x00`) keys. **Index maintenance does not live in `WritePolicy`** — the document
module assembles index keys and commits them in the same `write_batch` as the
body.

### `value_kind` and typed bodies

The first byte of every stored document value is a `value_kind` tag owned by the
document layer (`VK_ZDOC = 0x01` = ZDoc binary format, `VK_RAW = 0x00` = Legacy JSON). The new ZDoc format stores nested objects and arrays with length prefixes and sorted key offsets, allowing O(log N) zero-copy field extraction during query filtering.

### ZDoc Performance Trade-offs

There is a slight CPU cost during initial ingestion to compile incoming JSON to the ZDoc binary byte array. However, this unlocks massive CPU and memory savings on read and update paths, as filters can be evaluated directly against binary slices (`ValueView`) without allocating `serde_json::Value` trees. In the future, the ZDoc binary protocol could be exposed directly to the client drivers (Go, TypeScript, Python) to eliminate JSON serialization edge-to-edge.

---

## Not yet

- Aggregation pipeline (`$group` / `$lookup` / `$unwind`)
- Projection pushdown / covered queries (the body is always fetched)
- Other upsert edge-case Mongo parity beyond `$setOnInsert`
- TTL indexes / `expireAfterSeconds` on a date field (per-document `expires_at` on
  `DocPut` is supported and swept periodically by the server)
- MVCC / multi-document transactions (opcodes `0x10`–`0x12` reserved; `seq` is
  ordering only)
- Enforced collection schemas (`SchemaDef`, `0x31`, reserved)
- Marketed multi-tenant p99 SLA as a universal default (simulated soak ship bar
  δ≤50 ms clears with `[fair]` on; CI gates on ubuntu-latest via
  `tenant-isolation-soak.yml`; still off by default for single-tenant — enable via
  `config/zydecodb.pods.example.toml`; see [`SECURITY.md`](SECURITY.md#multi-tenant-sharing-model))
- ZDoc-to-client wire (not part of the 0.9 freeze)

---

## Source reading order

1. [`crates/zydecodb-engine/src/frame.rs`](../crates/zydecodb-engine/src/frame.rs) — wire envelope and command codes
2. [`crates/zydecodb-engine/src/keys.rs`](../crates/zydecodb-engine/src/keys.rs) — keyspaces, limits, `InternalKey`
3. [`crates/zydecodb-document/src/keys.rs`](../crates/zydecodb-document/src/keys.rs) — `doc:`/`idx:` layout
4. [`crates/zydecodb-document/src/encoding.rs`](../crates/zydecodb-document/src/encoding.rs) — order-preserving index encoding
5. [`crates/zydecodb-document/src/catalog.rs`](../crates/zydecodb-document/src/catalog.rs) — collection/index metadata
6. [`crates/zydecodb-document/src/store.rs`](../crates/zydecodb-document/src/store.rs) — body + index write batch
7. [`crates/zydecodb-document/src/planner.rs`](../crates/zydecodb-document/src/planner.rs) / [`query.rs`](../crates/zydecodb-document/src/query.rs) — plan + execution
8. [`crates/zydecodb/src/docdispatch.rs`](../crates/zydecodb/src/docdispatch.rs) — opcode routing
9. [`crates/zydecodb/src/server.rs`](../crates/zydecodb/src/server.rs) — `EngineHandle` + thread-per-connection

**Tests:** `crates/zydecodb/tests/document_e2e.rs`, `crates/zydecodb-engine/tests/range_scan.rs`.
