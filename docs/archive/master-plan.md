# Parade.dev — Master Plan (v1 Foundation for a Relational-Document Engine)

**Target:** A working Redis-compatible KV store, architected as the v1 substrate of a future relational-document database engine.
**Scope:** ParadeKV (Rust engine) + pkv-api (Go control plane).
**Time:** No fixed deadline. Sequence and correctness over calendar.

---

## Strategic Thesis

You are not building a key-value store with a Redis face. You are building the **structural foundation of an intelligent, embedded, relational-document hybrid engine** — and v1 happens to expose only the KV subset of its capabilities.

Every data structure in v1 must be the v1 form of its v3 self. No throwaway types. No `HashMap` we will replace with `BTreeMap`. No `Vec<u8>` keys we will swap for `InternalKey`. No fixed IPC frame we will rewrite to carry an AST. Build the skeleton of the final system. Leave the muscles unfilled.

The cost of this discipline in v1 is roughly 15–20% extra effort. The cost of *not* doing it is a full rewrite in Sprint 3.

---

## What Exists In v1 vs What Is Planned

The table below is the contract for everything that follows. If something is in **Built in v1**, it ships now. If it is in **Reserved / Stubbed in v1**, the hooks exist (bytes reserved, fields present, types extensible) but the behavior is trivial. If it is in **Sprint N**, it does not exist in any form in v1 — but v1 must not block it.

| Capability | Built in v1 | Reserved / Stubbed in v1 | Deferred |
|---|---|---|---|
| WAL + crash recovery | ✅ Full | — | — |
| Segmented WAL (rolling files) | ✅ Full | — | — |
| LSM memtable + SSTable flush | ✅ Full | — | — |
| Point GET / PUT / DEL | ✅ Full | — | — |
| Range iterators over memtable + SSTable | ✅ Full (used internally) | — | Exposed via query API in Sprint 4 |
| Tombstones as first-class entries | ✅ Full | — | — |
| Sequence number (`seq: u64`) on every entry | ✅ Allocated and persisted | Used only for ordering / shadowing in v1 | Becomes MVCC TxID in Sprint 3 |
| `InternalKey` struct (`user_key + seq + kind`) | ✅ Full | — | `kind` enum expands in Sprints 3–5 |
| Block-based SSTable format with index | ✅ Full | Per-block bloom filters reserved | Bloom filters land Week 4 / Sprint 2 |
| Manifest file (live SSTables, last seq) | ✅ Full | Schema/index metadata sections reserved | Populated in Sprint 4–5 |
| System keyspace (`0x00` prefix) | ✅ Reserved at IPC layer | No system keys written yet | First used in Sprint 4 for schemas |
| User keyspace (`0x01 <tenant> <key>`) | ✅ Full | Tenant ID zero-filled | Multi-tenant auth in Sprint 6 |
| Versioned IPC envelope (proto version + cmd + length) | ✅ Full | Cmd codes reserved for `BEGIN`, `COMMIT`, `QUERY`, `INDEX_*` | Implemented Sprint 3+ |
| Structured error taxonomy (8 codes) | ✅ Full | Codes reserved for `CONFLICT` etc. | Conflict path active Sprint 3 |
| TTL on entries | ✅ Full (Week 4) | — | — |
| RESP2 wire protocol (Go) | ✅ `SET`/`GET`/`DEL`/`PING`/`COMMAND`/`EXPIRE`/`TTL` | Command dispatcher extensible | More Redis verbs as needed |
| Bloom filters | Week 4 if time permits | Per-block layout reserved in SSTable format | Otherwise Sprint 2 |
| Unit + property + fuzz + crash-injection tests | ✅ Full | — | — |
| Structured logging + Prometheus metrics + `STATS` IPC | ✅ Full | — | — |
| TOML configuration | ✅ Full | — | — |
| Advertised operational limits (key/value/WAL/queue) | ✅ Full enforced | — | — |
| MVCC (snapshot isolation, version chains) | — | `seq` already present; read path already seeks `(key, seq DESC)` | **Sprint 3** |
| Multi-statement transactions (`BEGIN`/`COMMIT`) | — | IPC command codes reserved | **Sprint 3** |
| Typed document values (FlatBuffers) | — | ADR locked: FlatBuffers chosen over BSON | **Sprint 4** |
| Schema registry + opt-in validation | — | System keyspace + manifest sections reserved | **Sprint 4** |
| Synchronous secondary indexes | — | System keyspace + atomic batch write path reserved | **Sprint 5** |
| Query AST + execution engine (joins, projections) | — | IPC envelope versioned to carry AST payload | **Sprint 6** |
| Cost-aware planner | — | Manifest tracks per-SSTable size + key range | **Sprint 7** |
| Cloudflare R2 tiering | — | SSTable file abstraction kept behind a trait | **Sprint 7** |
| Compaction worker | — | SSTable format supports multi-file merge | **Sprint 2** |
| Multi-shard key routing (key-level sharding) | — | Tenant ID present in every IPC frame | **Sprint 3+ (post-MVCC)** |
| Replication / clustering | — | `seq` is the logical clock | **Sprint 8+** |
| MongoDB BSON wire protocol | — | RESP2 dispatcher is one of N possible front-ends | **Sprint 9+** |
| Multi-tenant billing / CU governance | — | Tenant ID present in every IPC frame | **Sprint 6+** |
| Authentication (API keys → tenant_id) | — | Tenant ID in IPC; v1 is `0x00 * 16` | **Sprint 6** |
| Backup / restore tooling | — | Documented procedure only (snapshot SSTables + manifest + WAL) | **Sprint 2+** |
| Web dashboard / UI | — | — | Indefinite |

---

## Repository Structure

```
paradekv/                      # Monorepo root — Rust engine lives here at the top level
├── Cargo.toml                 # Rust crate: the ParadeKV storage + (future) query engine
├── rust-toolchain.toml        # Pinned Rust toolchain (glommio is version-sensitive)
├── justfile                   # build / test / run-engine / run-api / bench / fuzz targets
├── .gitignore
├── config/
│   └── paradekv.example.toml  # Default config, shipped in repo
├── docs/
│   ├── master-plan.md         # this document
│   ├── adr/
│   │   ├── 0001-flatbuffers.md
│   │   ├── 0002-mvcc-design.md
│   │   └── 0003-glommio.md
│   ├── STATUS.md              # written end of Week 4
│   ├── BACKLOG.md             # written end of Week 4
│   └── PERF.md                # written end of Week 4
├── src/
│   ├── main.rs
│   ├── config.rs              # TOML/env config loader
│   ├── ipc.rs                 # Unix socket listener, versioned envelope parser
│   ├── frame.rs               # IPC envelope + command payload types
│   ├── keys.rs                # InternalKey, keyspace prefixes, encoding
│   ├── wal.rs                 # io_uring-backed, segmented Write-Ahead Log
│   ├── memtable.rs            # BTreeMap<InternalKey, Entry>
│   ├── sstable.rs             # Block-based SSTable writer + reader
│   ├── manifest.rs            # Engine state of record on disk
│   ├── seq.rs                 # Sequence number allocator (future TxID source)
│   ├── compaction.rs          # Stub in v1, worker in Sprint 2
│   ├── metrics.rs             # Prometheus registry + handlers
│   ├── stats.rs               # STATS IPC payload builder
│   ├── errors.rs              # Error code enum + From impls
│   └── engine.rs              # Orchestrates everything
├── tests/                     # Integration tests (multi-process, real disk)
│   └── crash_injection.rs
├── fuzz/                      # cargo-fuzz targets
│   ├── fuzz_targets/
│   │   ├── wal_parser.rs
│   │   ├── ipc_envelope.rs
│   │   └── sstable_reader.rs
│   └── Cargo.toml
└── pkv-api/                   # Go module — public-facing API / protocol shim
    ├── go.mod                 # module: github.com/<org>/paradekv/pkv-api
    ├── cmd/
    │   └── pkv-api/
    │       └── main.go        # Binary entrypoint
    ├── internal/
    │   ├── config/            # TOML loader (mirrors Rust shape)
    │   ├── resp/              # Hand-rolled RESP2 parser
    │   ├── ipc/               # Unix socket client, envelope builder, conn pool
    │   ├── server/            # TCP listener + command dispatcher
    │   ├── metrics/           # Prometheus handlers
    │   └── log/               # slog setup
    └── test/                  # Integration + crash-injection harness (Go side)
        └── crash_harness/
```

Single monorepo. Two independently built binaries: the Rust engine binary (`paradekv`) and the Go API binary (`pkv-api`).

---

## Day 0 — Pre-Code Checklist

Do not start Step 1.1 until every item below is complete. Total time: roughly half a day.

- [ ] `git init` the repo. `.gitignore` covers: `target/`, `*.sock`, `/var/lib/paradekv/*`, `*.log`, `vendor/`, `.idea/`, `.vscode/`, `coverage/`, `fuzz/corpus/`, `fuzz/artifacts/`.
- [ ] `rust-toolchain.toml` pins the Rust version. glommio is sensitive to nightly vs stable; start with a known-good stable (e.g. `1.78`) and only move if a dependency forces it.
- [ ] `justfile` (or `Makefile`) with targets: `build`, `test`, `lint`, `fuzz`, `run-engine`, `run-api`, `bench`, `clean`.
- [ ] CI configured (GitHub Actions or equivalent). On every push: `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`, `cd pkv-api && go build ./... && go test ./... && go vet ./...`. Red CI blocks merge.
- [ ] `docs/master-plan.md` (this file) committed. ADR stubs in `docs/adr/` committed as empty headings — they will be filled in during Week 4.
- [ ] `config/paradekv.example.toml` committed with documented defaults (see Configuration section).
- [ ] Decide and document the Go module path. Replace `<org>` in `pkv-api/go.mod` with the real GitHub org/user before `go mod init`.
- [ ] Create `/var/lib/paradekv/` and `/var/run/` writable by the dev user (or override paths in `paradekv.dev.toml`).

---

## Core Data Structures (v1 form of their v3 self)

### `InternalKey` — the key type that lives forever

```rust
#[derive(Clone, Eq, PartialEq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,   // includes keyspace prefix byte
    pub seq:      u64,       // monotonic; allocated by SeqAllocator
    pub kind:     EntryKind, // Value | Tombstone (more variants in Sprints 3–5)
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum EntryKind {
    Value     = 0x01,
    Tombstone = 0x02,
    // Sprint 3: SnapshotMarker = 0x10
    // Sprint 4: SchemaDef      = 0x20
    // Sprint 5: IndexEntry     = 0x30
}

// Sort order: user_key ASC, seq DESC.
// This single ordering rule makes:
//   - GET = "seek to (key, u64::MAX), take first entry with matching user_key"
//   - MVCC snapshot read = "seek to (key, snapshot_seq), take first entry ≤ snapshot_seq"
//   - Compaction = "walk sorted; for each user_key keep newest unless shadowed by tombstone"
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key.cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
```

**Why this matters in v1:** Today `seq` only enforces "newer writes shadow older writes" — which the WAL replay order already implies for a single-writer system. It looks like overhead. It is not. The moment MVCC lands in Sprint 3, the read path is already correct; we only add a `snapshot_seq` parameter to the seek. Without `seq` in v1, MVCC means rewriting the memtable, the SSTable format, the WAL format, and every entry already on disk.

### `Entry` — the value payload

```rust
pub struct Entry {
    pub value:      Option<Vec<u8>>,  // None iff kind == Tombstone
    pub expires_at: Option<u64>,      // Unix ms; populated in Week 4
}
```

Values stay opaque `Vec<u8>` in v1. **The schema-aware future (Sprint 4) does not change this struct** — it changes what those bytes *mean* and adds a parallel validation path. The struct is forward-compatible by virtue of being dumb.

### `Memtable` — `BTreeMap`, not `HashMap`

```rust
pub struct Memtable {
    map:        std::collections::BTreeMap<InternalKey, Entry>,
    size_bytes: usize,                  // approximate, for flush threshold
    min_seq:    u64,
    max_seq:    u64,
}
```

`HashMap` would be faster for point lookups in v1 and **catastrophic** for everything after. Range scans, ordered flushes to SSTable, MVCC version walks, secondary index traversal, `ORDER BY`, joins — every relational feature is built on ordered iteration. `BTreeMap` is the lowest-friction v1 choice that does not paint us into a corner. Skiplist (`crossbeam-skiplist`) is the Sprint 3 upgrade when we need lock-free concurrent reads during writes.

### `SeqAllocator` — single source of monotonic sequence numbers

```rust
pub struct SeqAllocator {
    next: std::sync::atomic::AtomicU64,
}

impl SeqAllocator {
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
```

In v1, one allocator per engine. Recovered on startup as `max(seq found in WAL or SSTables) + 1`. In Sprint 3 (MVCC) it becomes the TxID source. In a thread-per-core glommio model, this is the only piece of shared mutable state — and at one atomic increment per write, it is not a bottleneck. When it becomes one (Sprint 3+), it shards by CPU and stitches via hybrid logical clocks. Not our problem today.

### Keyspace prefixes — bake in the relational future today

```rust
pub const KS_SYSTEM: u8 = 0x00;  // schemas, indexes, catalog (unused in v1, reserved)
pub const KS_USER:   u8 = 0x01;  // all user data

// User key on the wire / on disk:
//   [0x01] [16-byte tenant_id] [user-supplied key bytes...]
//
// System key (Sprint 4+):
//   [0x00] [subsystem byte] [subsystem-specific key bytes...]
//     e.g. [0x00] [0x01=schema] [tenant_id] [collection_name]
//          [0x00] [0x02=index]  [tenant_id] [index_name] [indexed_value] [pk]
```

In v1 the engine **rejects any IPC write whose user_key does not begin with `0x01`**. This single guard means Sprint 4 can start writing to `0x00 ...` without worrying that user data has already squatted in that range.

---

## IPC Wire Protocol — Versioned Envelope (Frozen v1 Shape)

The previous fixed 25-byte frame is replaced with a versioned envelope. This is the most consequential change from the original plan, and it costs roughly one extra hour of parser work in v1.

### Envelope (every request and response)

```
[1 byte]  Protocol version       (v1 = 0x01)
[1 byte]  Command code           (see table)
[4 bytes] Payload length (u32 BE)
[N bytes] Command payload
```

### Command codes

```
0x01 PUT       — v1
0x02 GET       — v1
0x03 DEL       — v1
0x10 BEGIN     — reserved, Sprint 3
0x11 COMMIT    — reserved, Sprint 3
0x12 ROLLBACK  — reserved, Sprint 3
0x20 QUERY     — reserved, Sprint 6 (carries serialized AST)
0x30 INDEX_DEF — reserved, Sprint 5
0x31 SCHEMA_DEF — reserved, Sprint 4
0xF0 PING      — v1 (IPC liveness)
0xF1 STATS     — v1 (engine state snapshot; see Observability)
```

### v1 PUT payload

```
[16 bytes] Tenant ID
[ 8 bytes] Reserved for TxID/snapshot_seq (zero in v1)
[ 8 bytes] expires_at (Unix ms; zero = no expiry; wired up in Week 4)
[ 4 bytes] Key length (u32 BE)
[ 4 bytes] Value length (u32 BE)
[ K bytes] Key bytes
[ V bytes] Value bytes
```

### v1 GET payload

```
[16 bytes] Tenant ID
[ 8 bytes] Reserved for snapshot_seq (zero in v1 = "read latest")
[ 4 bytes] Key length (u32 BE)
[ K bytes] Key bytes
```

### v1 DEL payload — same shape as GET

### Response envelope

```
[1 byte]  Protocol version (0x01)
[1 byte]  Status            (see Error Taxonomy below)
[4 bytes] Payload length (u32 BE)
[N bytes] Payload (value bytes for GET; empty for PUT/DEL OK; UTF-8 message for ERROR)
```

### Error Taxonomy (status byte)

All status codes are committed in v1, even ones whose code path is not active yet. Status code numbers are **frozen** — new codes append, existing codes never renumber.

```
0x00 OK                — success; payload depends on command
0x01 NOT_FOUND         — GET/DEL of a missing key
0x02 ERROR             — generic; UTF-8 message in payload; prefer specific codes when possible
0x03 CONFLICT          — reserved, Sprint 3 (MVCC write-write conflict)
0x04 IO_ERROR          — disk full, fsync failed, manifest write failed; engine refuses further writes
0x05 INVALID_KEY       — reserved keyspace violation, key > MAX_KEY_BYTES, empty key
0x06 INVALID_VALUE     — value > MAX_VALUE_BYTES
0x07 ENGINE_BUSY       — flush queue full / backpressure; client should retry with backoff
0x08 PROTOCOL_ERROR    — malformed envelope, unknown command code, unsupported protocol version
```

**Why this matters:** When `BEGIN` lands in Sprint 3, the wire does not break — we add command code `0x10` and a payload format. When `QUERY` lands in Sprint 6, we add `0x20` and serialize an AST inside it. The Go parser dispatches on `(version, command)`. New commands are additive. The protocol version byte is the escape hatch if we ever truly need an incompatible change.

---

## Operational Limits (Committed In v1)

These numbers are **advertised**. They appear in the docs, are enforced at the IPC boundary, and are returned as `INVALID_KEY`/`INVALID_VALUE`/`ENGINE_BUSY` when violated.

| Limit | Value | Rationale |
|---|---|---|
| `MAX_KEY_BYTES` | 64 KB | Redis allows 512MB. That is absurd. Index entries (Sprint 5) embed keys, and bloated keys destroy fanout. |
| `MAX_VALUE_BYTES` | 16 MB | Caps WAL entry size and IPC buffer pressure. Larger blobs are an object-store problem, not a KV problem. |
| `MAX_IN_FLIGHT_WAL_BYTES` | 256 MB | Hard memory ceiling on the WAL write path. Beyond this, PUTs return `ENGINE_BUSY`. |
| `MAX_IMMUTABLE_MEMTABLES` | 4 | Active memtable + up to 4 awaiting flush. When full, PUTs return `ENGINE_BUSY` — backpressure to the client. |
| `MEMTABLE_FLUSH_THRESHOLD` | 64 MB | Standard LSM starting point; revisit with measurements in Week 4. |
| `SSTABLE_BLOCK_SIZE` | 16 KB | Page-cache-friendly; standard LevelDB/RocksDB default. |
| `WAL_SEGMENT_SIZE` | 64 MB | Roll over WAL files at this boundary or whenever a flush completes. |
| `IPC_POOL_SIZE` (Go side) | 32 | Empirical sweet spot for single-engine throughput; revisit with metrics. |
| `MAX_BATCH_KEYS` (DEL) | 1024 | Cap on `DEL k1 k2 ... kN` from a single RESP command. |

These limits exist in `config.rs` as constants but **are not configurable in v1**. They become configurable in Sprint 2 if real workloads demand it. Locking them in v1 forces honesty: every caller learns the limits up front.

---

## On-Disk Formats

### WAL entry

```
[ 1 byte]  Command (0x01=PUT, 0x02=DEL_TOMBSTONE)
[ 8 bytes] seq (u64 BE)              — populated in v1
[16 bytes] tenant_id
[ 8 bytes] expires_at (0 if none)    — populated in v1
[ 4 bytes] key_len (u32 BE)
[ 4 bytes] value_len (u32 BE; 0 for tombstone)
[ K bytes] key (including keyspace prefix byte)
[ V bytes] value
[ 4 bytes] CRC32 of the entry
```

Append-only. `fsync()` (via io_uring) after every entry. Corrupt trailing entry (CRC mismatch) is treated as a torn write — truncate and continue with an `INFO` log.

### WAL segmentation

The WAL is **not a single file**. It is a directory of rolling segments:

```
/var/lib/paradekv/wal/
  wal-00000001.log    (sealed)
  wal-00000002.log    (sealed)
  wal-00000003.log    (active, being appended)
```

Segment rollover triggers:
- Active segment exceeds `WAL_SEGMENT_SIZE` (64 MB)
- A memtable flush completes — the flush task records `last_seq_in_flushed_sstable`, and any segment whose `max_seq <= last_seq_in_flushed_sstable` is eligible for deletion

Truncation is `unlink(segment)`. No file rewriting, no seeking backwards, no holes.

Each segment starts with a tiny header: `[8 bytes] first_seq` so replay can sort segments in O(N) without parsing them.

**Why segmented:** at 48 hours of real traffic with no compaction yet, a single-file WAL becomes huge. Startup replay becomes slow. Prefix truncation is awkward on a single file (rewrite required). Retrofitting segmentation later is painful; baking it in now is trivial.

### Recovery Invariants

**Invariant R1 (idempotent replay):** WAL replay is idempotent with respect to SSTables. An entry whose `seq <= max_seq` of any live SSTable may be safely re-applied (it is a no-op against the memtable). Replay is allowed to skip such entries as an optimization, but must not error on them. This invariant is what makes the WAL/SSTable/manifest handoff race-condition-free.

**Invariant R2 (manifest-first):** No SSTable file is considered "live" until its `SSTABLE_ADD` record is durable in the manifest. An orphan `.sst` file with no matching manifest entry is garbage and must be deleted on startup.

**Invariant R3 (WAL-after-manifest):** WAL segments are eligible for deletion **only after** the manifest record describing the SSTable that covers their entries is durable. The flush sequence is: write SSTable → fsync SSTable → append `SSTABLE_ADD` → fsync manifest → unlink covered WAL segments. A crash at any point leaves the engine recoverable.

These three invariants are tested in `tests/crash_injection.rs` — see Testing Strategy.

### SSTable — block-based, with index

```
File layout:
  [Data block 0]      ~16KB of sorted (InternalKey, Entry) pairs
  [Data block 1]
  ...
  [Data block N]
  [Bloom block]       (reserved in v1; populated in Week 4 or Sprint 2)
  [Index block]       (first_key_of_block → file_offset) for every data block
  [Footer — 40 bytes]
    [ 8] index_offset
    [ 8] index_length
    [ 8] bloom_offset   (0 if absent)
    [ 8] bloom_length   (0 if absent)
    [ 4] magic          (0x50524144 = "PRAD")
    [ 4] format version (0x00000001)

Data block entry:
  [ 1] kind (Value | Tombstone)
  [ 8] seq
  [ 4] key_len
  [ 4] value_len (0 for tombstone)
  [ 8] expires_at
  [ K] key
  [ V] value
  Entries within a block are sorted by InternalKey (user_key ASC, seq DESC).
```

**Why block-based now:** Range scans, partial loads, per-block bloom filters, and (much later) column-group projection all require this format. A single-blob SSTable would force a rewrite when we add any of those. The cost today is one extra day in Week 2 to write the block writer/reader.

### Manifest — engine state of record

```
/var/lib/paradekv/MANIFEST

Append-only log of records:
  [1] record_type
  [4] record_length
  [N] payload

v1 record types:
  0x01 SSTABLE_ADD      { id: u64, level: u8, min_key, max_key, min_seq, max_seq, size_bytes }
  0x02 SSTABLE_REMOVE   { id: u64 }
  0x03 SEQ_CHECKPOINT   { last_durable_seq: u64 }
  0x04 WAL_TRUNCATE     { up_to_segment_id: u64 }

Reserved (no records emitted in v1):
  0x10 SCHEMA_DEF       { tenant_id, collection, schema_blob }       — Sprint 4
  0x11 INDEX_DEF        { tenant_id, index_name, field_path, ... }   — Sprint 5
  0x12 SNAPSHOT_MARKER  { snapshot_id, seq }                         — Sprint 3
```

On startup: replay manifest → know which SSTables are live and the last durable seq → replay WAL segments from there. **Without a manifest, every Sprint 2+ feature (compaction, schemas, indexes, snapshots) requires reinventing engine state on disk.** The cost in v1 is roughly half a day.

---

## Configuration (TOML)

A single TOML file per binary. The Rust engine and the Go API each load their own section of a shared schema. Path resolution: CLI flag `--config` > env `PARADEKV_CONFIG` > `./paradekv.toml` > `/etc/paradekv/paradekv.toml`.

```toml
# config/paradekv.example.toml

[engine]
data_dir         = "/var/lib/paradekv"   # SSTables, manifest live here
wal_dir          = "/var/lib/paradekv/wal"
ipc_socket_path  = "/var/run/paradekv.sock"
ipc_socket_mode  = "0600"                # Unix socket permissions
stats_socket     = true                   # enables 0xF1 STATS command

[engine.logging]
level  = "info"                           # trace | debug | info | warn | error
format = "json"                           # json | text

[engine.metrics]
enabled = true
listen  = "127.0.0.1:9090"                # Prometheus scrape target

[api]
listen_addr      = "127.0.0.1:6379"       # bind to loopback by default; opt-in to 0.0.0.0
ipc_socket_path  = "/var/run/paradekv.sock"
ipc_pool_size    = 32
read_timeout_ms  = 5000
write_timeout_ms = 5000

[api.logging]
level  = "info"
format = "json"

[api.metrics]
enabled = true
listen  = "127.0.0.1:9091"
```

Operational limits (`MAX_KEY_BYTES`, `MAX_VALUE_BYTES`, etc.) are **not** in the config in v1. They are compile-time constants. This is deliberate — see "Operational Limits" above.

---

## Observability

Three layers. All three ship in v1. Each binary owns its own.

### 1. Structured logging

- **Rust:** `tracing` + `tracing-subscriber` with JSON formatter.
- **Go:** stdlib `log/slog` with JSON handler.
- Every WAL write, flush start/end, SSTable open, manifest mutation, recovery decision logs at INFO with `seq` ranges, file paths, and durations.
- Errors log at ERROR with the structured error code from the taxonomy.
- Per-request logs at DEBUG, off by default in production.

### 2. Prometheus metrics

Both binaries expose `/metrics` on a configurable port.

**Engine metrics (`paradekv` on `:9090`):**
- Counters: `paradekv_put_total`, `paradekv_get_total`, `paradekv_del_total`, `paradekv_get_not_found_total`, `paradekv_wal_bytes_written_total`, `paradekv_sstable_flushes_total`, `paradekv_bloom_false_positives_total` (when bloom ships)
- Histograms: `paradekv_ipc_request_duration_seconds{cmd}`, `paradekv_wal_fsync_duration_seconds`, `paradekv_sstable_get_duration_seconds`
- Gauges: `paradekv_memtable_size_bytes`, `paradekv_immutable_memtable_count`, `paradekv_live_sstable_count`, `paradekv_wal_segment_count`, `paradekv_last_durable_seq`

**API metrics (`pkv-api` on `:9091`):**
- Counters: `pkv_resp_commands_total{cmd}`, `pkv_resp_errors_total{code}`
- Histograms: `pkv_resp_request_duration_seconds{cmd}`, `pkv_ipc_roundtrip_seconds`
- Gauges: `pkv_open_connections`, `pkv_ipc_pool_in_use`, `pkv_ipc_pool_available`

### 3. `STATS` IPC command (`0xF0` … wait, `0xF1`)

A debug-only IPC command. Payload-free request. Response payload is a JSON document:

```json
{
  "uptime_s": 12345,
  "last_durable_seq": 9876543,
  "memtable_bytes": 12345678,
  "memtable_entries": 4567,
  "immutable_memtables": 1,
  "sstables": [
    {"id": 1, "size_bytes": 67108864, "min_seq": 1, "max_seq": 100000, "entries": 50000}
  ],
  "wal_segments": [
    {"id": 5, "first_seq": 9000000, "max_seq": 9876543, "size_bytes": 1234567, "active": true}
  ]
}
```

`pkv-api` exposes this as a custom Redis command `PKVSTATS` for operator use; never to end users.

**Without these three layers, the Week 4 48-hour soak is unfalsifiable.** No data → no `PERF.md` → no Sprint 2 prioritization.

---

## Testing Strategy

Three layers, all required, all CI-gated.

### Layer 1 — Unit tests (Rust + Go)

Pure-function tests, no I/O, fast (whole suite under 5 seconds).

**Rust units (minimum):**
- `InternalKey::cmp` — exhaustive ordering cases including equal user_keys with different seqs, tombstones vs values
- `EntryKind` round-trips through `u8`
- WAL entry encode → decode round-trips; truncated entry rejected by CRC
- SSTable block encode → decode round-trips; corrupted footer rejected; magic mismatch rejected
- IPC envelope encode → decode round-trips; oversize payload rejected; unknown command code → `PROTOCOL_ERROR`
- Memtable size accounting matches actual byte total under random insert/delete sequences
- `SeqAllocator` is strictly monotonic under contended access

**Go units (minimum):**
- RESP2 parser: every type, every malformed variant, partial reads, oversized inputs
- IPC envelope builder/parser symmetry
- Connection pool: Get/Put/Close lifecycle, exhaustion blocks, errored conn discarded

### Layer 2 — Property + fuzz tests

**Rust (`fuzz/`):**
- `wal_parser` — feeds random bytes to the WAL replay routine. Must never panic; must either parse cleanly, return a structured error, or report torn-write at the trailing edge.
- `ipc_envelope` — feeds random bytes to the envelope parser. Same invariants.
- `sstable_reader` — feeds random bytes as a candidate SSTable. Must never panic.
- Run nightly in CI for 5 minutes per target; manually for 1 hour before any release.

**Rust (`proptest`):**
- For any sequence of (PUT, DEL, GET) operations, the engine state matches a reference `HashMap<Vec<u8>, Option<Vec<u8>>>`.

**Go:**
- `go test -fuzz=FuzzRESPParser` against the RESP2 parser. Run nightly in CI.

### Layer 3 — Crash injection (the gate that matters)

A reusable harness in `tests/crash_injection.rs` (Rust integration test) **and** `pkv-api/test/crash_harness/` (Go-driven end-to-end). The harness:

1. Starts the engine subprocess.
2. Issues a workload (PUT N keys, sometimes DEL, sometimes mixed).
3. At a random offset, sends `SIGKILL` (-9) to the engine.
4. Restarts the engine.
5. Verifies invariants:
   - Every key whose PUT was acknowledged before the kill is either correct or `NOT_FOUND` (acceptable loss for unflushed WAL tail).
   - Zero wrong values.
   - Zero panics.
   - Engine accepts new writes after restart.
   - Recovery invariants R1, R2, R3 hold (verified by orphan-file scan and seq-monotonicity check).

This harness runs:
- Once at the end of Week 1 (Milestone 1 gate)
- Continuously through Week 2 (the Day 14 stress test gate)
- Once before declaring v1 done

### Coverage target

- Rust engine: **80% line coverage** measured by `cargo-llvm-cov`. CI fails if coverage drops below.
- Go API: no hard target; integration tests cover the surface. Vet with `go test -cover` for transparency.

---

## Rust Dependencies (`Cargo.toml`)

```toml
[dependencies]
glommio          = "0.9"                                   # thread-per-core + io_uring; NOT tokio
bytes            = "1"                                     # zero-copy IPC buffers
crossbeam        = "0.8"                                   # lock-free channels for cross-shard routing
crc32fast        = "1"                                     # WAL + SSTable integrity
xxhash-rust      = { version = "0.8", features = ["xxh3"] } # bloom filter hashing (Week 4 / Sprint 2)
tracing          = "0.1"                                   # structured logs
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
prometheus       = "0.13"                                  # metrics registry
serde            = { version = "1", features = ["derive"] }
serde_json       = "1"                                     # STATS payload
toml             = "0.8"                                   # config loader
thiserror        = "1"                                     # error enum derives

[dev-dependencies]
proptest         = "1"
tempfile         = "3"

# Sprint 4: flatbuffers = "24"
# Sprint 7: reqwest (R2 uploads), behind a `cloud` feature flag

[profile.release]
opt-level     = 3
lto           = true
codegen-units = 1
```

**Why glommio over tokio:** glommio is built exclusively for `io_uring` and thread-per-core shared-nothing. tokio's work-stealing scheduler introduces cross-core locking that destroys the 10,000+ idle-database density target. Tradeoff: smaller community, less documentation — budget extra debugging time. See ADR-0003 for fallback plan (`monoio`).

## Go Dependencies (`pkv-api/go.mod`)

```
module github.com/<org>/paradekv/pkv-api

go 1.22

require (
    github.com/google/uuid v1.6.0
    github.com/prometheus/client_golang v1.19.0
    github.com/BurntSushi/toml v1.3.2
)
```

RESP2 parser is hand-rolled. No third-party Redis library — we will need full control when adding MongoDB protocol later.

---

## Build Sequence

The sequence is the same shape as the original 4-week plan, but every step now includes the substrate work that keeps the v3 door open. Sprint-mode (no fixed calendar). Each "Week" is a coherent block of work; ship when it passes its gate, not when a clock runs out.

---

### Week 1 — The Engine Lives (gate: crash-recovery proof)

**Goal:** End-to-end Go → Unix socket → Rust → response, with WAL durability and replay correctness.

#### Step 1.1 — Project scaffolding + versioned IPC envelope

- Complete Day 0 checklist if not already done.
- Rust: initialize `paradekv` crate. In `frame.rs`, define the envelope parser and command payload types per the spec above. In `ipc.rs`, bind a glommio `UnixListener` on the configured socket path (chmod to `0600`), read envelope, dispatch on command code (PUT/GET/DEL only; everything else returns `PROTOCOL_ERROR "unimplemented"`).
- Rust: wire up `tracing` + Prometheus registry from `metrics.rs`. Every IPC request logs at DEBUG; counters increment.
- Rust: implement `0xF1 STATS` returning an empty-but-valid JSON skeleton.
- Go: initialize `pkv-api` module under `paradekv/pkv-api/`. In `internal/ipc/`, implement envelope builder and connection wrapper. Wire up `cmd/pkv-api/main.go` as a tiny driver that sends a hardcoded PUT(`hello=world`) and prints the response.
- Go: load TOML config, init `slog`, init Prometheus handler.
- **Gate:** Rust logs the parsed command in JSON; Go prints `status=OK`; `curl localhost:9090/metrics` returns Prometheus output; `STATS` IPC returns valid JSON.

#### Step 1.2 — `InternalKey` + `BTreeMap` memtable + keyspace guard + error taxonomy

- Rust: implement `keys.rs` (prefix constants, encoding), `seq.rs` (atomic allocator), `memtable.rs` (`BTreeMap<InternalKey, Entry>` with size tracking), `errors.rs` (full taxonomy with `From` impls).
- Wire `engine.rs` to handle PUT/GET/DEL against the memtable. PUT/DEL allocate a fresh `seq`. DEL writes a `Tombstone` entry — **does not remove from the map**. GET seeks to `(user_key, u64::MAX)` and returns the first matching `user_key`; if its kind is `Tombstone`, return `NOT_FOUND`.
- Enforce keyspace prefix `0x01` on all user keys at the IPC boundary → `INVALID_KEY`.
- Enforce `MAX_KEY_BYTES` and `MAX_VALUE_BYTES` → `INVALID_KEY` / `INVALID_VALUE`.
- Go: implement `Put`, `Get`, `Del` helpers and a smoke test that PUTs 10 keys, GETs them, DELs them, GETs and expects `NOT_FOUND`. Test oversize key/value rejection.
- Unit tests for `InternalKey::cmp`, `Memtable` size accounting, error taxonomy round-trips.
- **Gate:** smoke test passes; all unit tests green; coverage report generated.

#### Step 1.3 — Segmented WAL with `seq` and CRC

- Rust: `wal.rs` manages `/var/lib/paradekv/wal/` as a directory of rolling segments. Active segment is created on startup or rolled when it exceeds `WAL_SEGMENT_SIZE`. Each segment opens via glommio's io_uring file API.
- Every PUT/DEL writes a WAL entry per the format above *before* the memtable is updated. `fsync()` after every write. CRC32 trailer on every entry.
- Backpressure: if total in-flight WAL bytes exceed `MAX_IN_FLIGHT_WAL_BYTES` → `ENGINE_BUSY`.
- Fuzz target `wal_parser` set up and run in CI.
- **Gate:** `xxd` on segment files shows entries with monotonically increasing `seq` values; segment rollover at 64MB observed; `paradekv_wal_bytes_written_total` increments.

#### Step 1.4 — Manifest + recovery (Invariants R1–R3)

- Rust: `manifest.rs` opens/creates `/var/lib/paradekv/MANIFEST`. On startup:
  1. Replay manifest → know which SSTables are live (none yet) and the last `SEQ_CHECKPOINT`.
  2. Delete any orphan `.sst` file with no matching `SSTABLE_ADD` (Invariant R2).
  3. Replay WAL segments in order, skipping entries `seq <= max_seq` of any live SSTable (Invariant R1).
  4. Reconstruct memtable; seed `SeqAllocator` to `max(seq) + 1`.
  5. Only now bind the Unix socket.
- Add `/var/lib/paradekv/` orphan-file scan to recovery.
- Crash-injection test harness in `tests/crash_injection.rs` set up; smoke run.
- **Gate (Milestone 1):**
  1. PUT 100 keys via Go, `kill -9` the Rust process, restart, GET all 100 keys, all correct values.
  2. Torn-write trailer in a WAL segment (simulate by truncating one byte) is recovered cleanly with an `INFO` log, not a crash.
  3. Orphan `.sst` file in data dir is deleted on startup with an `INFO` log.
  4. Crash-injection harness runs 100 iterations without surfacing a wrong value or a panic.

**Do not start Week 2 until Milestone 1 passes.**

---

### Week 2 — Durability Hardens (gate: stress test with kill -9 mid-write)

**Goal:** Bounded RAM, correct SSTable persistence and read path, durable across restarts under load.

#### Step 2.1 — Memtable freeze + flush trigger

- Rust: when memtable `size_bytes` exceeds `MEMTABLE_FLUSH_THRESHOLD` (64MB), atomically swap it for a fresh empty memtable and hand the immutable one to a flush task. Reads must consult: (active memtable) → (immutable memtables being flushed) → (SSTables newest-to-oldest).
- If immutable queue depth reaches `MAX_IMMUTABLE_MEMTABLES` (4) → PUTs return `ENGINE_BUSY`.
- `paradekv_immutable_memtable_count` gauge tracks queue depth.

#### Step 2.2 — Block-based SSTable writer (with Invariants R2 + R3)

- Rust: `sstable.rs` writes the block-based format above. Sort by `InternalKey` (which is already `user_key ASC, seq DESC`). Pack `SSTABLE_BLOCK_SIZE` (16KB) data blocks. Build the index block. Reserve the bloom block (write zero bytes for now). Write footer with `magic = 0x50524144`.
- Atomic-publish sequence (Invariants R2 + R3):
  1. Write to a tempfile in `data_dir/.tmp/`. `fsync` the file.
  2. `rename` to `data_dir/00000001.sst`. `fsync` the directory.
  3. Append `SSTABLE_ADD` to manifest. `fsync` manifest.
  4. Append `WAL_TRUNCATE { up_to_segment_id: X }` to manifest. `fsync` manifest.
  5. `unlink` covered WAL segments.
- File naming: `00000001.sst`, etc., monotonic from a manifest-tracked counter.
- Fuzz target `sstable_reader` set up.

#### Step 2.3 — SSTable reader with block index

- Rust: open SSTable, load footer + index block into RAM (small, sticky). For a GET: bloom check (skipped in v1 — assume "maybe present"; counter `paradekv_bloom_false_positives_total` exists but reads zero), binary-search the index block for the candidate data block, read that single block, scan within it for the target `user_key`. Return the newest non-tombstone entry, or `NOT_FOUND` if the newest entry is a tombstone.
- `paradekv_sstable_get_duration_seconds` histogram populated.

#### Step 2.4 — Multi-SSTable read path correctness

- Rust: GET walks memtable(s) first (newest seq wins), then SSTables newest-to-oldest. **First definitive answer wins** — including tombstones. A tombstone in a newer SSTable hides a value in an older SSTable.
- Iterator API (internal only in v1): expose `memtable.range(...)` and `sstable.scan(start, end)`. Not surfaced over IPC. Used by future compaction, future indexes, future queries.
- Unit tests for tombstone shadowing across (memtable → SSTable), (SSTable → SSTable).

#### Step 2.5 — Stress test (non-optional gate)

- Crash-injection harness expanded: PUT 500,000 keys, random 8-byte keys, 64-byte values. Mid-run, `kill -9` the Rust process at a random offset. Restart. GET every key written before the kill — must be either the correct value or `NOT_FOUND` (an unflushed WAL tail is acceptable loss; a wrong value is not). Then PUT another 100,000 without restart, confirm engine continues. Run 50 iterations with different random seeds.
- Validate Invariants R1–R3 on every restart: no orphan SSTables, no missing manifest entries for live SSTables, seq is monotonic.
- **Gate:** zero wrong values, zero panics, zero hangs, zero invariant violations across 50 iterations. Fix anything that breaks.

---

### Week 3 — The Go Layer (gate: real Redis client compatibility)

**Goal:** Any Redis client running `SET`/`GET`/`DEL` against `localhost:6379` works.

#### Step 3.1 — RESP2 parser from scratch

- Go: `internal/resp/parser.go`. Handle Simple String, Error, Integer, Bulk String, Null Bulk, Array. Public surface: `Parse(r io.Reader) ([][]byte, error)`, `WriteOK`, `WriteError`, `WriteBulk`, `WriteNull`, `WriteInteger`.
- Unit tests with hand-crafted byte slices for every type and edge cases (empty bulk, null bulk, nested arrays).
- Go fuzz target on `Parse`.

#### Step 3.2 — TCP listener + command dispatcher

- Go: listen on configured `listen_addr` (default `127.0.0.1:6379`). Per-connection goroutine reads RESP arrays, dispatches by uppercased command name. v1 commands:
  - `SET key value` → IPC PUT (expires_at=0)
  - `GET key` → IPC GET → Bulk or Null
  - `DEL key [key ...]` → N × IPC DEL → Integer count of successes (cap at `MAX_BATCH_KEYS`)
  - `PING` → `+PONG\r\n` (no IPC)
  - `COMMAND` → `+OK\r\n` (many clients send this on connect)
  - `PKVSTATS` → IPC `0xF1 STATS` → Bulk JSON (operator command)
- Unknown command → `-ERR unknown command 'XYZ'\r\n`.
- IPC error codes map to RESP errors with stable prefixes: `-ERR CONFLICT ...`, `-ERR INVALID_KEY ...`, `-ERR ENGINE_BUSY ...`, etc.
- **Gate:** `redis-cli -p 6379 SET hello world` returns `OK`; `redis-cli -p 6379 GET hello` returns `"world"`; `redis-cli -p 6379 PKVSTATS` returns valid JSON.

#### Step 3.3 — IPC connection pool

- Go: `internal/ipc/pool.go`. Pool size from config (default 32). `Get` blocks if exhausted (with timeout from config). `Put` returns to pool. Errored conns are discarded, replaced lazily on next `Get`.
- Metrics: `pkv_ipc_pool_in_use`, `pkv_ipc_pool_available` populated.
- **Gate:** `redis-benchmark -p 6379 -c 50 -n 10000 -t set,get` completes without errors or data corruption.

#### Step 3.4 — Real-workload smoke

- Point one of your actual Redis-using apps at `localhost:6379`. Log every command that fails. This list drives Week 4.
- Expected failures: `EXPIRE`/`TTL` (added Week 4), `INCR`, list/hash types (Sprint 2+).
- **Gate:** `SET`/`GET`/`DEL` traffic from the real app runs clean for at least one hour with metrics scraped continuously.

---

### Week 4 — Production Hardening (gate: 48h soak on a real workload)

**Goal:** Fix what Week 3 surfaced, add TTL, ship bloom filters if time allows, run real traffic for 48 hours.

#### Step 4.1 — Triage and fix Week 3 failures

Priority order: data correctness > connection handling > missing commands the real app needs > performance.

#### Step 4.2 — TTL end to end

- The IPC PUT payload already has `expires_at`. Wire it up:
  - Rust: on GET, if `entry.expires_at != 0 && now_ms > expires_at` → return `NOT_FOUND` and emit a tombstone (lazy expiry). Background sweeper every 1s scans the active memtable and tombstones any expired entries it finds.
  - Go: parse `SET key value EX seconds` and `SET key value PX milliseconds`. Implement `EXPIRE key seconds` (GET then PUT with new `expires_at`) and `TTL key` (GET entry, compute remaining; return `-2` if missing, `-1` if no expiry).
- **Gate:** `SET foo bar EX 5; sleep 6; GET foo` returns nil.

#### Step 4.3 — Bloom filters (if time permits)

- Rust: at flush time, build a bloom filter for the SSTable (m = 10n bits, k = 7 hash functions via xxh3), write it into the reserved bloom block, update the footer.
- On startup, mmap or load bloom blocks for all live SSTables.
- GET path: bloom-check each SSTable before touching its data blocks. Skip if definitely absent. `paradekv_bloom_false_positives_total` now meaningful.
- **Gate:** a GET for a key that does not exist performs zero data-block reads (verify via instrumentation counter).

If bloom filters do not fit in Week 4, they slip to Sprint 2. The reserved bloom block in the SSTable footer means they remain an additive change.

#### Step 4.4 — 48-hour soak

- Point a real app at Parade. Do not touch the engine for 48 hours. Prometheus scraping both binaries throughout. Monitor:
  - RSS of both processes
  - Panics / restarts (should be zero)
  - Wrong-value reads (should be zero — verify by sampling)
  - `paradekv_wal_bytes_written_total` growth rate
  - `paradekv_live_sstable_count` accumulation rate (no compaction yet — file count will grow; that is expected)
  - p50 / p99 of `pkv_resp_request_duration_seconds{cmd="GET"}` and `{cmd="SET"}`
- Log every anomaly. This is your production-readiness report — it writes itself from Prometheus.

#### Step 4.5 — Audit and write the v1 → v2 handoff docs

- `docs/STATUS.md` — what works, what does not, what is known-broken.
- `docs/BACKLOG.md` — ordered, effort-estimated list of everything not yet built.
- `docs/PERF.md` — `redis-benchmark` numbers, RSS under load, WAL/SSTable growth rates, p50/p99 latencies from soak.
- `docs/adr/0001-flatbuffers.md` — full ADR (skeleton already exists; fill in).
- `docs/adr/0002-mvcc-design.md` — full Sprint 3 MVCC design, written *now* while the substrate is fresh in your head.
- `docs/adr/0003-glommio.md` — codify the runtime choice and the `monoio` fallback plan.

**v1 success bar:**
- `redis-benchmark -p 6379 -c 10 -n 100000 -t set,get` clean
- 48h uptime, zero data corruption
- WAL replay correct after `kill -9` at any point in the crash-injection harness (1000+ iterations cumulative across v1)
- Day-1 data still readable after the soak
- All v1 SLO targets either met or documented as misses in `PERF.md` with explanations

---

## SLO Targets (v1 baseline; revisit each sprint)

| Metric | Target | Notes |
|---|---|---|
| GET p50 | < 100 µs | Single-block SSTable hit or memtable hit |
| GET p99 | < 1 ms | Acceptable until bloom + compaction land |
| PUT p50 | < 200 µs | Dominated by WAL fsync |
| PUT p99 | < 2 ms | |
| Sustained PUT throughput | > 50,000 ops/s single-engine | |
| RSS overhead per stored byte | < 2× value-bytes-on-disk | Memtable + index blocks + bloom blocks |
| Recovery time per 1 GB of WAL | < 5 s | Sequential read + memtable rebuild |

Missing one or more of these in v1 is acceptable; **silently missing them is not**. Every miss is documented in `PERF.md` with a hypothesis for why and a slot in `BACKLOG.md`.

---

## Security / Threat Model (v1)

v1 has **no authentication**. This is documented loudly because someone will deploy it to the public internet otherwise.

**Defenses in v1:**
- Unix IPC socket is `chmod 0600` and owned by the engine user. The Go API runs as a *different* user and is added to a group that can read the socket. This is the v1 of multi-tenant process isolation.
- The Go API binds `127.0.0.1:6379` by default. Opt-in to `0.0.0.0` requires editing the config. Forcing operators to opt into network exposure is a free safety win.
- Operational limits (`MAX_KEY_BYTES`, `MAX_VALUE_BYTES`, `MAX_IN_FLIGHT_WAL_BYTES`, `MAX_IMMUTABLE_MEMTABLES`) prevent a single misbehaving or malicious client from OOMing the engine.
- IPC envelope parser is fuzz-tested. RESP2 parser is fuzz-tested.
- Tenant ID is present in every IPC frame but is `0x00 * 16` in v1 — Sprint 6 wires up API keys → tenant_id mapping with cryptographic auth.

**Explicitly NOT defended against in v1:**
- Authenticated clients abusing each other's data (no multi-tenant enforcement)
- Network-level attacks (no TLS — terminate at a reverse proxy if needed)
- Side-channel timing attacks on the value store

---

## Backup / Disaster Recovery (v1 procedure, automation later)

Documented v1 procedure (no tooling shipped — operators run this manually):

1. While engine is running, take a filesystem snapshot (LVM, ZFS, btrfs, or `cp -r` on a quiesced read-only mount) of `data_dir` + `wal_dir`. **Order matters:** snapshot SSTables first, then manifest, then WAL. A snapshot that captures the manifest after SSTables and the WAL after the manifest is always restorable because of Invariants R1–R3.
2. Restore = stop engine, lay snapshot back down, start engine. WAL replay on startup brings state forward.

Automated backup, point-in-time recovery, and incremental snapshots are **Sprint 2+**.

---

## Glommio Reality Check (operational notes)

Pinned here so they are not surprises:

- Each glommio executor pins to one CPU. Cross-executor communication uses lock-free crossbeam channels. **v1 runs a single executor on one core.** Multi-core is Sprint 3+ alongside MVCC.
- Glommio file I/O requires `O_DIRECT`-capable filesystems. Supported in v1: **ext4, XFS** on local NVMe/SSD. **Not supported:** tmpfs, NFS, many FUSE filesystems. Document this in `STATUS.md`.
- Upstream maintenance has been intermittent. **Fallback plan (ADR-0003):** if glommio becomes unmaintained, port to `monoio` (ByteDance, MIT-licensed). Both speak io_uring; the abstraction layer is `wal.rs` and `ipc.rs` — roughly 500 lines of swap-out work, not a rewrite.
- Multi-shard key routing (when multi-core lands): **hash(tenant_id) mod N executors** is the v1 sharding plan. Tenant always lands on the same shard. Cross-shard queries (Sprint 6+) require explicit fan-out. Connection-level vs key-level sharding decision: **key-level** wins because it preserves locality for the future query engine. This is documented now to lock the answer in.

---

## Architecture Decision Records

ADR skeletons are committed at Day 0. They are filled in during Week 4 while context is fresh.

### ADR-0001 — Typed value format: **FlatBuffers**

**Decision:** When schema-aware values arrive in Sprint 4, the on-the-wire and on-disk value format will be FlatBuffers, not BSON, not Cap'n Proto, not protobuf.

**Why not BSON:** BSON is a Mongo-compat format. Field access is linear-scan. The senior engineer's "lightning-fast bitwise check" cannot be lightning-fast on a format that requires walking the document to find a field. BSON is only justified if Mongo wire-protocol compatibility is a hard product requirement — and even then, we translate at the protocol boundary, not in the storage engine.

**Why not protobuf:** Requires deserialization to access any field. Same problem as BSON, with worse ergonomics.

**Why not Cap'n Proto:** Comparable to FlatBuffers technically. FlatBuffers has better Rust ergonomics today and a more mature schema evolution story. Cap'n Proto's RPC layer is irrelevant to us — we have our own IPC.

**Why FlatBuffers:** Zero-copy field access, schema evolution via field IDs, sub-microsecond field reads, Rust support (`flatbuffers` crate) is production-grade, and the schema definition (`.fbs` files) is human-readable and version-controllable as part of the catalog.

**Implication for v1:** None today. Values stay opaque `Vec<u8>`. But Go's protocol shim must never assume values are text — keep them as `[]byte` end-to-end so the Sprint 4 switch costs nothing on the wire.

### ADR-0002 — MVCC design (target: Sprint 3)

**Decision:** Snapshot isolation via `seq`-tagged entries. Single global `SeqAllocator` until contention demands sharding.

**Mechanism:**
- `BEGIN` → engine returns a `snapshot_seq` (current value of the allocator). All reads in this transaction use this `snapshot_seq`: seek to `(user_key, snapshot_seq)`, take the first entry whose `seq <= snapshot_seq` and whose `user_key` matches.
- Writes inside a transaction buffer in a per-transaction `WriteBatch` (an isolated `BTreeMap<InternalKey, Entry>`).
- `COMMIT` → allocate one `commit_seq`, stamp every entry in the batch with it, write all entries to the WAL as a single contiguous block bracketed by `TX_BEGIN(commit_seq)` and `TX_END(commit_seq, crc)` markers. Recovery treats a missing `TX_END` as "transaction did not commit" and discards the block. Atomic by construction.
- Conflict detection (snapshot isolation, not serializable): on `COMMIT`, check that no key in the write set has been modified by a transaction with `commit_seq > snapshot_seq && commit_seq < my_commit_seq`. If so, return `CONFLICT`. Caller retries.
- Garbage collection: compaction drops versions whose `seq` is older than the oldest live snapshot.

**Why this works on the v1 substrate:** every required piece — `InternalKey` with `seq`, `BTreeMap` memtable, tombstones as entries, manifest tracking `last_durable_seq`, versioned IPC envelope with reserved `BEGIN`/`COMMIT` codes, structured `CONFLICT` error code — already exists in v1. Sprint 3 is purely additive.

### ADR-0003 — Glommio over tokio (with `monoio` fallback)

**Decision:** Use `glommio` for the Rust engine runtime. Abstract io_uring usage in `wal.rs` and `ipc.rs` so a port to `monoio` is a swap-out, not a rewrite.

**Rationale:** glommio is built exclusively for io_uring and thread-per-core shared-nothing. tokio's work-stealing scheduler introduces cross-core locking that destroys the 10,000+ idle-database density target.

**Risk:** Smaller community, intermittent upstream maintenance. Mitigated by keeping the runtime-coupled surface area small (~500 lines).

---

## Sprint Roadmap (Post-v1)

Each sprint listed with its dependency chain so reordering risk is visible.

**Sprint 2 — LSM hygiene**
- Compaction worker (size-tiered or leveled; pick after measuring v1 SSTable growth)
- Bloom filters if not shipped in Week 4
- Per-block compression (LZ4 or Zstd)
- Backup/restore automation
- Configurable operational limits
- Depends on: v1 SSTable format ✓

**Sprint 3 — MVCC + Transactions**
- `BEGIN`/`COMMIT`/`ROLLBACK` over the IPC envelope
- Snapshot reads using `snapshot_seq`
- Atomic multi-key commit blocks in the WAL
- Conflict detection (`CONFLICT` status active)
- Multi-core: N glommio executors, hash(tenant_id) sharding
- Skiplist memtable (`crossbeam-skiplist`) for lock-free concurrent reads
- Depends on: `InternalKey.seq` ✓, manifest ✓, versioned IPC ✓, error taxonomy ✓

**Sprint 4 — Typed values + schema registry**
- FlatBuffers value format negotiated at connection time (legacy opaque-byte mode preserved for Redis)
- Schemas stored as system-keyspace entries (`0x00 0x01 ...`)
- Opt-in validation at PUT time
- Depends on: system keyspace ✓, manifest schema-def records ✓

**Sprint 5 — Synchronous secondary indexes**
- `CREATE INDEX` writes a system-keyspace `INDEX_DEF` entry
- Every PUT atomically writes primary + all matching index entries in one memtable batch
- Index reads are range scans over the index keyspace
- Depends on: typed values (Sprint 4) ✓, range iterators ✓, system keyspace ✓

**Sprint 6 — Query AST + execution engine + auth**
- AST defined in a shared `.fbs` schema (Go encodes, Rust decodes)
- IPC command `0x20 QUERY` carries a serialized AST
- Executor supports: index lookup, range scan, nested-loop join, projection, limit
- Multi-tenant auth enforced at the Go layer; tenant ID is injected into the AST
- Depends on: indexes (Sprint 5) ✓, transactions (Sprint 3) ✓

**Sprint 7 — Cost-aware planner + R2 tiering**
- SSTable storage abstracted behind a `BlobStore` trait (local NVMe and R2 implementations)
- Manifest tracks per-SSTable storage tier and access stats
- Planner consults bloom filters + manifest metadata to estimate cost
- Returns `HighLatencyWarning` to Go for tier-2 reads, enabling CU-based throttling
- Depends on: indexes (Sprint 5) ✓, query engine (Sprint 6) ✓

**Sprint 8+ — Replication, additional wire protocols, billing, dashboard**
- Sequence numbers (`seq`) already in place serve as the logical clock for replication.

---

## Hard Rules

1. **The v1 substrate decisions are non-negotiable.** `BTreeMap` memtable, `InternalKey` with `seq`, segmented WAL, block-based SSTable with footer, manifest file with R1–R3 invariants, versioned IPC envelope, full error taxonomy, keyspace prefix guard, structured logging + Prometheus + STATS, advertised operational limits, fuzz + crash-injection tests. Skip one of these and the rewrite cost in Sprint 3+ exceeds the v1 savings.
2. **Day 0 checklist completes before Step 1.1 starts.** No "I'll set up CI later."
3. **Milestone 1 (Week 1 crash-recovery) gates Week 2.** No exceptions.
4. **Week 2 stress test gates Week 3.** A leaky engine with a Redis facade is not a product.
5. **The IPC envelope is frozen at v1.** Adding new *commands* is fine. Changing the envelope shape requires bumping the version byte and supporting both versions during transition.
6. **Status code numbers are frozen at v1.** New codes append, existing codes never renumber.
7. **No premature optimization.** Correct, then measured, then fast. Bloom filters and compaction are intentionally deferred.
8. **No relational features in v1.** MVCC, indexes, schemas, queries — all Sprint 3+. v1's job is to make those sprints feasible, not to start them.
9. **Write ADR-0001, ADR-0002, ADR-0003 during Week 4**, while the substrate is fresh. Lose this window and Sprint 3 starts with three days of re-deriving decisions.
10. **CI stays green.** A red CI blocks merge, blocks release, blocks the next step.

---

## What v1 Looks Like When You Hold It in Your Hand

A Rust binary and a Go binary. Any Redis client connects, runs `SET`/`GET`/`DEL`/`EXPIRE`/`TTL`, and the data survives `kill -9`, machine reboot, and 48 hours of real traffic. Prometheus scrapes both binaries on `:9090` and `:9091`. `redis-cli PKVSTATS` shows you exactly what the engine is thinking. Operational limits are advertised and enforced. A fuzzer and a crash-injection harness have been beating on the WAL and SSTable parsers for the entire build.

Underneath that boring Redis-shaped surface sits: a sequenced, ordered, block-indexed LSM with a manifest and segmented WAL bound by three named recovery invariants, a versioned binary protocol with reserved opcodes for transactions and queries, a full structured error taxonomy, and a keyspace partitioned for a catalog that does not yet exist.

The KV store is the demo. The substrate is the product.
