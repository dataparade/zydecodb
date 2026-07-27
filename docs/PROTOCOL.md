# Protocol and document store

How the document layer works today: collections of JSON documents with a
query/index layer, riding on the durable LSM key-value engine.

**Audience:** Engineers working on collections, indexes, query execution, or the
wire protocol.

**Related:** [`GUIDE.md`](GUIDE.md) (security, pods, replication, shipping, upgrades),
[`COMPATIBILITY.md`](COMPATIBILITY.md) (1.x semver contract). Pre-implementation
design notes and ADRs live in [`docs/archive/`](archive/) and are lineage only.

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
- **When sort streams from an index vs the sort buffer:**
  - Streams (key-cursor) when the requested sort matches the chosen index’s
    field directions (or the exact reverse — then the engine reverse-scans),
    including omitting equality-covered leading fields. Example: index
    `[ownerId ASC, updatedAt ASC]` or `[ownerId ASC, updatedAt DESC]` with
    filter `{ownerId}` and `sort: updatedAt DESC` key-streams without buffering.
  - Uses the sort buffer when sort is neither the index order nor its reverse
    (still capped by `max_sort_buffer`).
- `count` and `distinct`.
- `find_one` is a `find` with `limit = 1`.

### Writes (`update.rs`, `store.rs`)

- Update operators: `$set`, `$inc`, `$unset`, `$push`, `$setOnInsert`. Bare
  (non-`$`) update documents are rejected. `$setOnInsert` applies only when an
  upsert inserts; normal updates ignore it. On insert, `$setOnInsert` runs
  before regular ops so `$set`/`$inc`/`$unset`/`$push` win on path conflicts.
- **Filtered positional `$set`:** update exactly one array element by identity
  without replacing the whole document. Path form:
  `items.$[skuId=ABC].qty` (bare token = string), `items.$[skuId="ABC"].qty`
  (JSON string/number/bool literal), or `items.$[skuId=ABC]` (replace the whole
  element). Rules:
  - **`$set` only** — filtered paths on `$inc` / `$unset` / `$push` /
    `$setOnInsert` are rejected (`BadUpdate`).
  - Exactly **one** matching element required; zero or multiple matches →
    `BadUpdate`.
  - At most one `$[field=value]` segment per path; the prefix must resolve to
    an array.
  - **Rejected (non-goals):** bare `$`, `$[]`, `$[<identifier>]` /
    `arrayFilters`, multi-segment `$[…]` in one path, and Mongo-style
    “update all matches” semantics.
  - No new wire opcodes: clients send the path inside existing opaque update
    JSON (`Update` / `DocUpdateIfMatch`). Index maintenance stays the usual
    atomic body+index batch.
- `update_one` / `update_many`, `delete_one` / `delete_many`: candidate ids come
  from a lock-free snapshot, then **each matched document is rewritten in one
  atomic `write_batch`** (body + all of its index keys). Per-document writes are
  atomic; a multi-document update is **not** globally atomic.
- Filter upsert (`FLAG_UPSERT` on Update): when no document survives the
  under-lock filter recheck, insert at most one document built from top-level
  equality fields in the filter plus the operator update (including
  `$setOnInsert`). Response includes `upserted_id` on insert; omit it on a
  normal update.
- **Optimistic concurrency:** each document's revision is the engine
  `InternalKey.seq` of its latest body write (opaque `u64`). `DocGetRev` /
  `FindRev` return it; `DocPutIfMatch` / `DocUpdateIfMatch` require an
  `ifMatch` revision checked under the write lock. Stale or missing documents
  return `Conflict`. Unconditional `DocPut` / `Update` are unchanged. Admin TTL
  backfill may advance a revision without a client-visible field change (safe
  false conflict, never a lost write).

---

## Indexes

- **Secondary indexes** maintained automatically and atomically on every write:
  the document body and every affected index key move in a single
  `Engine::write_batch` (one WAL record, one CRC), so a crash can never leave an
  index disagreeing with its document.
- **Directional indexes:** each field may be ascending or descending. DESC fields
  use an order-reversed encoding so forward LSM order matches the declared
  logical order. `IndexDef` wire: optional trailer after TTL —
  `0x02` + N direction bytes (`1`=ASC, `0`=DESC). All-ASC indexes omit the
  trailer (identical to pre-direction payloads). Clients: Go
  `CreateIndexFields`, Python `create_index([("f", False)])`, TypeScript
  `createIndex([{ path, ascending: false }])`.
- **Compound indexes** supported; the planner can use an equality prefix plus one
  trailing range.
- **Unique indexes** (`create_index(..., unique=True)`) are enforced server-side;
  a duplicate key returns `Conflict`.
- **Synchronous backfill**: adding an index to a populated collection indexes the
  existing documents before the call returns.
- **TTL indexes** (`create_index(..., expire_after_seconds=N)`): at most one per
  collection; single field whose value is unix millis; body and index-key
  `expires_at` become `field_ms + N * 1000` on write/update. Missing/invalid
  field → no expiry. Wire trailer `0`/omitted = not a TTL index. Upserts that
  change only `expires_at` rewrite every current index key so body and secondary
  keys stay aligned for reclaim.

### TTL visibility and disk reclaim

Expiry is **wall-clock at observation time** (same rule as `get` / `scan`). Live
snapshots do **not** keep expired keys readable; a key that has passed
`expires_at` is invisible even under a pinned snapshot.

- **Lazy hide:** reads filter expired entries without requiring a prior delete.
- **Memtable sweeper (~30s):** inserts non-durable tombstones in the active
  memtable only (no WAL). Metric: `ttl_sweep_tombstones_total`. Crash loses those
  tombstones; invisibility still holds via lazy read after reopen.
- **SST reclaim:** Tidewalker compaction drops expired newest values and all
  older versions of that user key (so an older non-expired version cannot
  resurrect). Metric: `compaction_expired_dropped_total`. Disk space returns only
  after expired data has flushed to SST **and** been compacted.
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
| `0x10` | `Begin` | Implemented (start bounded per-connection transaction) |
| `0x11` | `Commit` | Implemented (validate + one atomic WAL batch) |
| `0x12` | `Rollback` | Implemented (discard staged ops) |
| `0x20` | `Query` | Implemented (document layer; modes below) |
| `0x21` | `DocPut` | Implemented (document upsert; optional `expires_at` trailer) |
| `0x22` | `DocDel` | Implemented (document delete) |
| `0x23` | `Find` | Implemented (filter + sort/projection/pagination) |
| `0x24` | `Update` | Implemented (filter-based partial update; `FLAG_UPSERT`) |
| `0x25` | `Delete` | Implemented (filter-based delete) |
| `0x26` | `Count` | Implemented (count / distinct; modes below) |
| `0x27` | `DocGetRev` | Implemented (by-id get returning opaque revision) |
| `0x28` | `FindRev` | Implemented (find page rows include opaque revision) |
| `0x29` | `DocPutIfMatch` | Implemented (conditional replace; stale/missing → `Conflict`) |
| `0x2A` | `DocUpdateIfMatch` | Implemented (conditional by-id update; stale/missing → `Conflict`) |
| `0x2B` | `Aggregate` | Implemented (optional `$match` + one `$group`; see [Aggregation](#aggregation)) |
| `0x2C` | `Watch` | Implemented (primary-only collection change stream; see [Change streams](#change-streams)) |
| `0x30` | `IndexDef` | Implemented (index create + backfill; optional TTL / direction trailers) |
| `0x31` | `SchemaDef` | Reserved (parseable; responds `ProtocolError` until schemas) |
| `0x40` | `SessionInit` | Implemented (API-key auth handshake) |
| `0x41` | `SetContext` | Implemented (admin tenant switch) |
| `0x42` | `AdminDropTenant` | Implemented (live tenant offboard; admin path) |
| `0xF0` | `Ping` | Implemented |
| `0xF1` | `Stats` | Implemented |

### Status bytes (`errors.rs`)

| Byte | Name | Meaning |
|------|------|---------|
| `0x00` | `Ok` | Success |
| `0x01` | `NotFound` | Missing key/document (drivers often map to absence, not an exception) |
| `0x02` | `Error` | Generic server failure |
| `0x03` | `Conflict` | Constraint / revision conflict |
| `0x04` | `IoError` | I/O failure |
| `0x05` | `InvalidKey` | Malformed key |
| `0x06` | `InvalidValue` | Malformed value / document body |
| `0x07` | `EngineBusy` | Load shedding / rate limit (idempotent ops may retry) |
| `0x08` | `ProtocolError` | Malformed payload, unused flag bits, unknown/unimplemented opcode |
| `0x09` | `PolicyRejected` | Write-policy / quota rejection |
| `0x0A` | `UnsupportedFormat` | On-disk artifact version the engine cannot read |
| `0x0B` | `Unauthorized` | Missing/invalid API key or pre-SessionInit |
| `0x0C` | `Forbidden` | Authenticated but not permitted (role, ACL, replica) |

### Payload discriminators (`wire.rs`)

| Surface | Bytes | Notes |
|---------|-------|-------|
| Write flags | `FLAG_RELAXED=0x01`, `FLAG_UPSERT=0x02` | Trailing flags on DocPut / Update / Delete / if-match writes. **Unused bits must be zero** — non-zero unknown bits → `ProtocolError`. |
| Query mode | `0x00` by-id, `0x01` index-range | First payload byte |
| Query `include_bodies` | trailing `u8` on index-range | `0` = ids only; omitted/`1` = include bodies (append-only trailer) |
| Find projection | `0x00` none, `0x01` include, `0x02` exclude | |
| Count mode | `0x00` count, `0x01` distinct | First payload byte |
| IndexDef direction | trailer tag `0x02` + per-field ASC/DESC | Optional; omitted = all ASC |
| IndexDef TTL | optional `expire_after_seconds` u64 | |
| DocPut / DocPutIfMatch | optional `expires_at` u64 trailer | Absolute unix millis; `0`/omitted = never |
| Watch frames | `0x01` ack, `0x02` event, `0x03` heartbeat | First byte of Ok streaming payloads |
| Watch ops | `0x01` upsert, `0x02` delete | Inside event frames |

Payload codecs live in `zydecodb-document/src/wire.rs`. Golden encode/decode
vectors are in [`clients/conformance/vectors.json`](../clients/conformance/vectors.json).

### 1.x wire freeze

`proto_version = 1` opcodes, status bytes, write flags, and payload discriminators
above are **frozen for 1.x**:

- Existing encodings never change shape or renumber.
- New opcodes and status bytes may append; they must not reuse assigned values.
- Unknown opcodes (bytes outside `Command::from_u8`) and reserved-but-unimplemented
  opcodes (e.g. `SchemaDef`) both respond with status `ProtocolError` (`0x08`) and
  **keep the connection open**. Hard connection close is reserved for unparseable
  framing (bad version, truncated header/payload, desync).
- Official drivers never send opcodes they do not implement. Older servers answer
  new opcodes with `ProtocolError` rather than silently degrading.
- On-disk format upgrades follow [`GUIDE.md`](GUIDE.md#upgrading). Semver / driver
  compatibility policy and tagging are in [`COMPATIBILITY.md`](COMPATIBILITY.md#releases-and-tagging).

The official drivers (Python, Go, TypeScript) are the intended product surface;
the binary wire sits behind them.

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

The payoff of one lane is that **bounded multi-document transactions**
(opcodes `0x10`–`0x12`) stage N operations in connection memory and commit them
as one atomic WAL batch (the engine's existing all-or-nothing `WAL_BATCH`
primitive), with no cross-shard coordination.

### Bounded transactions

Per-connection `Begin` / `Commit` / `Rollback` provide atomic multi-key writes
across documents (by ID) and raw KV without claiming general-purpose MVCC.

**Isolation:** at `Begin` the connection captures an owned engine snapshot.
Direct gets merge that snapshot with a staged overlay (read-your-writes). Other
connections see nothing until `Commit`. Commit re-checks every touched key's
revision/existence against live state and validates unique indexes across the
whole staged set, then persists body + index + KV ops in **one** `write_batch`
(≤ 1,024 physical keys).

**Allowed in a transaction:** `Put`/`Get`/`Del`, `DocPut`/`DocDel`/`DocGetRev`,
`DocPutIfMatch`/`DocUpdateIfMatch`, plus `Ping`/`Stats`. Collections must already
exist (no implicit create). Relaxed durability is rejected inside a transaction;
transaction commits are always durable.

**Rejected in a transaction:** filter `Find`/`Update`/`Delete`/`Count`,
`IndexDef`, auth/tenant/admin context changes, nested `Begin`.

**Limits:** one open transaction per connection; 30s lifetime; 256 logical
staged ops; 32 MiB staged bodies; 1,024 physical batch keys. Disconnect, idle
close, timeout, or protocol abort drops staging with nothing persisted.

**Not provided:** serializable isolation, query overlays, long-running
transactions, cross-connection transactions, or historical MVCC reads. If
`Commit`'s transport fails after the request may have reached the server,
clients surface an unknown-commit-result error — reconcile by re-reading keys.

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

- Full Mongo aggregation compatibility (`$lookup`, joins, `$unwind`, expressions, multi-stage pipelines beyond `$match`→`$group` — see [Aggregation](#aggregation) for the supported subset)
- Projection pushdown / covered queries (the body is always fetched)
- Other upsert edge-case Mongo parity beyond `$setOnInsert`
- Mongo `arrayFilters`, bare `$` / `$[]` / `$[<id>]`, multi-match positional
  updates, and filtered `$inc`/`$unset`/`$push` (v1 is `$set` + exactly-one)
- Arbitrary mixed sorts that are neither index order nor the exact reverse of
  index order (those still use the bounded sort buffer)
- General MVCC / serializable isolation / transactional queries (bounded
  by-ID+KV transactions ship; `seq` remains ordering + opaque revision, not
  multi-version history)
- Enforced collection schemas (`SchemaDef`, `0x31`, reserved)
- Marketed multi-tenant p99 SLA as a universal default (simulated soak ship bar
  δ≤50 ms clears with `[fair]` on; CI gates on ubuntu-latest via
  `tenant-isolation-soak.yml`; still off by default for single-tenant — enable via
  `config/zydecodb.pods.example.toml`; see [`GUIDE.md`](GUIDE.md#multi-tenant-sharing-model))
- ZDoc-to-client wire (not part of the 1.x freeze)

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

## Aggregation

ZydecoDB supports a **bounded**, **deterministic** aggregation opcode (`Aggregate = 0x2B`) for simple rollups. This is not MongoDB aggregation compatibility.

### Supported pipeline

Exactly:

1. Optional first stage: `$match` (same filter language as `Find`)
2. Required final stage: `$group`

`$group` rules:

- `_id` must be JSON `null` (one global bucket) or a single field reference `"$dotted.path"`
- Accumulator fields may only be:
  - `{"$sum":"$path"}` — missing/non-numeric inputs contribute `0`
  - `{"$count":{}}` — counts every document that reaches `$group`

Hard parser ceilings (not configurable):

- At most two stages
- At most 16 accumulators
- Pipeline JSON ≤ 64 KiB

### Numeric semantics

- Integer-only sums stay checked `i64` until any float input promotes the group to finite `f64`
- Integer overflow or non-finite float output is rejected
- Missing group-key fields map to `null`
- Object/array group keys are rejected
- Result groups are emitted in deterministic scalar/`null` key order

### Resource limits (`[aggregation]`)

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_scan_docs` | `100000` | Candidates inspected (counted before residual filter) |
| `max_groups` | `10000` | Distinct group keys |
| `max_memory_bytes` | `16MiB` | Group key + accumulator state |
| `max_result_bytes` | `4MiB` | Encoded response size (enforced while encoding) |

Aggregation is an authenticated **read**. It is unavailable inside bounded transactions. Tenant prefix and collection-prefix ACL apply like other document reads.

### Explicit non-goals

The following remain unsupported and are rejected by the parser:

- `$lookup`, joins, `$unwind`
- Expression languages, window functions, `$facet`
- Multi-stage pipelines beyond `$match` → `$group`
- Spilling to disk / unbounded group maps

### Client APIs

Official clients expose `aggregate(pipeline)` on collection handles (Python, Go, TypeScript). Codec conformance vectors cover request/response framing.

## Change streams

ZydecoDB change streams (`Watch = 0x2C`) deliver **collection-scoped**, **primary-only** document change notifications with **durable resume tokens** backed by a retained WAL archive.

This is **not** raw WAL replication. Replication/shipping uses the operator-owned ship directory; change streams use a separate local archive under `[change_streams].archive_dir` (default `<data_dir>/change_log`).

### Guarantees

- **Events:** `upsert` (full post-write document JSON) or `delete` (document ID only). WAL cannot truthfully distinguish insert/replace/update.
- **Ordering:** strict `(engine_seq, WAL_op_ordinal)` for fsynced events only.
- **Delivery:** at-least-once across reconnects. Resume is **exclusive** after the last processed token.
- **Non-events:** TTL expiry, compaction, index keys, raw KV, and system metadata do not emit events.
- **Primary-only:** replicas and `replica.from` configurations reject Watch.

### Configuration (`[change_streams]`)

Disabled by default.

| Setting | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Master switch |
| `archive_dir` | `<data_dir>/change_log` | Retained sealed WAL segment archive |
| `retention_secs` | `3600` | Time-based prune of oldest sealed archives |
| `retention_bytes` | `1GiB` | Size-based prune |
| `heartbeat_ms` | `15000` | Idle heartbeat cadence |
| `write_timeout_ms` | `5000` | Slow-consumer socket write deadline |
| `max_subscriptions` | `128` | Global concurrent Watch connections |
| `max_subscriptions_per_tenant` | `8` | Per-tenant cap |

When enabled, a covered WAL segment is unlinked only after archive confirmation. Archive failures retain the source WAL and retry rather than creating a silent history gap.

### Protocol

1. Client opens a **dedicated** connection (not a pooled one-request connection).
2. After auth + collection ACL, client sends `Watch` with `[collection][resume_token]`.
3. Server ACKs, then streams framed Ok payloads: `ACK`, `EVENT`, `HEARTBEAT`.
4. Client cancellation closes the socket. Server shutdown, credential revocation, collection removal, write timeout, retention gap, and peer close all terminate the stream and release capacity.

Resume tokens are opaque, versioned, CRC-checked, and bound to database ID, tenant prefix, and collection ID. Malformed/cross-scope tokens are protocol/forbidden errors; a valid token older than retention returns `Conflict`.

Drivers accept **raw bytes** (or language buffer) on `watch(resume=...)` and
surface **base64 strings** on emitted events. Do not round-trip the base64
string back without decoding.

### Internal formats (1.0)

These formats are internal to the server. Drivers treat resume tokens as opaque.
They are documented here so 1.0.x → 1.0.y upgrades stay honest.

#### Resume token (`TOKEN_VERSION = 1`)

```text
[1]  version (0x01)
[16] database_id
[2]  tenant_prefix_len (u16 BE)
[N]  tenant_prefix
[4]  collection_id (u32 BE)
[8]  seq (u64 BE)
[4]  op_ordinal (u32 BE)
[4]  crc32 of all preceding bytes (u32 BE)
```

Unknown versions fail decode with `ProtocolError`. A token that survives a
patch upgrade is valid if its `(seq, op_ordinal)` is still inside the retained
archive window.

#### Archive `manifest.json` (1.0 shape)

Required fields (no schema-version key today):

| Field | Type | Meaning |
|-------|------|---------|
| `database_id_hex` | string | 32-char hex of the 16-byte database id |
| `segments` | array | Ordered archive entries |

Each segment object: `segment_id`, `min_seq`, `max_seq`, `sealed_unix_ms`,
`size_bytes`, `file_name`. Missing required fields fail closed on load.
Unknown JSON fields are ignored by serde defaults (forward-tolerant within 1.x).
Archived segment files reuse the live WAL segment format
(`WAL_FORMAT_VERSION`).

### Operational limits

- No server-side event queue and no multiplexing: each subscriber pulls one event at a time on its connection.
- Falling behind retention is a terminal gap error — shorten retention only if consumers can keep up.
- Heartbeats keep NAT/load-balancer idle timeouts from killing quiet subscriptions.
- Metrics include active subscriptions, delivered events/heartbeats, disconnect reasons, and archive segment/byte/sequence gauges.

### Client APIs

Python / Go / TypeScript expose `watch(...)` returning a stream/iterator of change events with opaque base64 resume tokens. Tokens are not advanced until the caller receives the event.
