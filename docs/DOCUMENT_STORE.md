# ZydecoDB Document Store — Architecture

How the document layer works today: collections of JSON documents with a
MongoDB-style query/index layer, riding on the durable LSM key-value engine.

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
| Body storage | The document body is stored as **JSON bytes** behind a one-byte `value_kind` discriminator (`VK_RAW = 0x00`). The byte reserves room for a future typed format (e.g. FlatBuffers = `0x01`) without an on-disk migration; only `VK_RAW` is written today |
| Index entry | Order-preserving encoded field value(s) → document id (see [Indexes](#indexes)) |

The engine itself stays byte-opaque: it stores values as bytes and never parses
JSON. All document semantics live in `zydecodb-document`.

---

## Query language

Filters are JSON documents (`filter.rs`):

| Category | Supported |
|----------|-----------|
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` |
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

- Update operators: `$set`, `$inc`, `$unset`, `$push`. Bare (non-`$`) update
  documents are rejected.
- `update_one` / `update_many`, `delete_one` / `delete_many`: candidate ids come
  from a lock-free snapshot, then **each matched document is rewritten in one
  atomic `write_batch`** (body + all of its index keys). Per-document writes are
  atomic; a multi-document update is **not** globally atomic — same model as
  MongoDB.

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
| `0x21` | `DocPut` | Implemented (document upsert) |
| `0x22` | `DocDel` | Implemented (document delete) |
| `0x23` | `Find` | Implemented (filter + sort/projection/pagination) |
| `0x24` | `Update` | Implemented (filter-based partial update) |
| `0x25` | `Delete` | Implemented (filter-based delete) |
| `0x26` | `Count` | Implemented (count / distinct) |
| `0x30` | `IndexDef` | Implemented (index create + backfill) |
| `0x40` | `SessionInit` | Implemented (API-key auth handshake) |
| `0x41` | `SetContext` | Implemented (admin tenant switch) |
| `0xF0` | `Ping` | Implemented |
| `0xF1` | `Stats` | Implemented |
| `0x10`–`0x12` | `Begin`/`Commit`/`Rollback` | Reserved (no multi-doc transactions) |
| `0x31` | `SchemaDef` | Reserved (no enforced schemas) |

Payload codecs are in `zydecodb-document/src/wire.rs`. The Python driver is the
intended product surface; the binary wire sits behind it.

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

The server holds the engine as `Arc<Mutex<Engine>>` and serves each connection on
its own thread (`spawn_tcp_conn` / `spawn_uds_conn`), bounded by
`security.max_connections`. Queries are **two-phase**: the planner takes a
consistent snapshot under the lock, then the scan runs lock-free against that
pinned view, so a long scan does not block other clients' writes. This is why
pagination can be repeatable-read across pages.

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
document layer (`VK_RAW = 0x00` = JSON today). A future typed format can claim
another tag and coexist with existing raw documents without touching the engine's
WAL/SSTable format. JSON is the only body format written today; index performance
claims are about the index keys, not the body encoding.

---

## Not yet

- Aggregation pipeline (`$group` / `$lookup` / `$unwind`)
- `$regex` / `$type` / array operators (`$elemMatch` / `$all`)
- Projection pushdown / covered queries (the body is always fetched)
- Upsert
- Document-level TTL indexes (the engine has per-entry `expires_at`; the document
  layer does not yet map a TTL index onto it)
- MVCC / multi-document transactions (opcodes `0x10`–`0x12` reserved; `seq` is
  ordering only)
- Enforced collection schemas (`SchemaDef`, `0x31`, reserved)
- A typed (non-JSON) document body format

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
9. [`crates/zydecodb/src/server.rs`](../crates/zydecodb/src/server.rs) — `Arc<Mutex<Engine>>` + thread-per-connection

**Tests:** `crates/zydecodb/tests/document_e2e.rs`, `crates/zydecodb-engine/tests/range_scan.rs`.
