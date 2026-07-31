# Internals (maintainers)

*ZydecoDB is built on ZEngine, a Rust LSM storage engine, with ZLattice, the document layer that speaks ZDoc. Four pillars make it fast and unkillable: Straightline evaluation, Caravan commits, Tidewalker compaction, and HotSwap runtime.*

This document details the core architectural pillars that enable ZydecoDB to achieve high throughput, low latency, and zero-downtime operational management.

## Separation of Concerns: ZEngine vs. ZLattice

The architecture is strictly divided into two primary components to ensure modularity and separation of concerns:

1. **ZEngine (`zydecodb-engine`)**: A pure, embedded Key-Value Log-Structured Merge (LSM) tree. It operates entirely on raw bytes, managing the Write-Ahead Log (WAL), Memtables, and SSTables. It has no concept of JSON, documents, or schemas.
2. **ZLattice (`zydecodb-document`)**: A stateless evaluation and indexing layer that sits on top of the engine. It defines the `ZDoc` binary format, executes queries, and manages secondary indexes.

```mermaid
graph TD
    Client[Client Connections] --> Server[Network & Auth Layer]
    Server --> DocLayer["ZLattice (Document Layer)"]
    DocLayer --> EngineLayer["ZEngine (Storage Engine)"]
    EngineLayer --> Disk[(Disk: WAL & SSTables)]
```

---

## Pillar 1: Straightline (Zero-Copy Binary Evaluation)

Most document databases suffer from a "parse tax" during unindexed queries, wasting CPU cycles deserializing JSON strings into memory-heavy DOM trees just to evaluate a filter.

ZydecoDB bypasses this entirely using **Straightline** evaluation. By storing data in the `ZDoc` binary format, the `ValueView` struct navigates the raw byte array by jumping pointers based on field lengths. It skips irrelevant fields in a straight line without allocating memory or parsing strings.

### Performance Impact
In local benchmarks, ZydecoDB performs an unindexed full collection scan of 50,000 complex documents in **~10,215ms** (evaluating roughly **4,894 documents per second**).

*Note: While 5,000 documents evaluate in ~80ms (62,500 docs/sec) when fully cached in L1/L2 CPU cache, the 50,000 document benchmark (10.2s) reflects the true performance when the working set exceeds the CPU cache and requires traversing the 64MB block cache and streaming 10,000 matching JSON objects over the TCP socket to the client.*

### Implementation
The evaluation path defers full materialization until the document is confirmed to match the filter:

```rust
fn check_filter<'a>(stored: &'a [u8], filter: &crate::filter::Filter, doc_id: &[u8]) -> bool {
    let kind = stored[0];
    let payload = crate::store::strip_value_kind(stored);
    
    let view = if kind == crate::store::VK_ZDOC {
        // Zero-copy pointer into the binary payload
        crate::binary::ValueView::new(payload)
    } else {
        // Fallback for legacy JSON
        // ...
    };
    
    filter.matches(view, Some(doc_id))
}
```

---

## Pillar 2: Caravan (Asynchronous Group Commit)

When running with strict synchronous durability (`durability = "sync"`), every write must be `fsync`'d to the Write-Ahead Log (WAL) before acknowledging the client. If every connection locked the database to perform disk I/O, throughput would collapse.

ZydecoDB solves this using **Caravan** commits via the `CommitCoordinator`. The WAL synchronization is decoupled from the main engine lock. Concurrent writes from multiple connection threads are batched together into a single "caravan" and flushed to disk in one `fsync`. This saturates disk IOPS while keeping the engine lock highly available for readers and memtable inserts.

### Performance Impact
With synchronous durability enabled, a single local node achieves **~8,700 durable writes per second**.

```mermaid
graph LR
    C1[Conn 1] -->|Append| Mem[Memtable]
    C2[Conn 2] -->|Append| Mem
    C3[Conn 3] -->|Append| Mem
    Mem --> CC[CommitCoordinator]
    CC -->|Single fsync| WAL[(WAL)]
    WAL -.->|Ack| C1
    WAL -.->|Ack| C2
    WAL -.->|Ack| C3
```

Change streams observe the same durable watermark: subscribers never see writes
before Caravan fsync completes. Retained history is a separate local WAL archive
(not the shipping directory); see [`PROTOCOL.md`](PROTOCOL.md#change-streams).
Bounded aggregation reuses Straightline match visiting with hard scan/group/
memory/result ceilings; see [`PROTOCOL.md`](PROTOCOL.md#aggregation).

---

## Pillar 3: Tidewalker (Dynamic LSM Compaction)

To maintain predictable read amplification under heavy write loads, ZEngine employs a dynamic, leveled compaction strategy (L0 → L1 → L2).

As the Memtable flushes to L0 SSTables, the **Tidewalker** background worker (`CompactionPlanner`) evaluates per-level scores. When a level exceeds its dynamic byte target, the worker performs a k-way merge of overlapping files into the next level. This process continuously garbage-collects deleted, overwritten, and wall-clock-expired data in the background without blocking foreground query execution. Expired newest versions also suppress older versions of the same user key so compaction cannot resurrect a prior live value.

---

## Pillar 4: HotSwap (Wait-Free Configuration Swapping)

In a multi-tenant environment, operational tasks like provisioning new tenants, rotating API keys, or adjusting rate limits must happen without dropping active connections or stalling the accept loop.

ZydecoDB uses **HotSwap** for true zero-downtime, lock-free configuration reloads. When the server receives a `SIGHUP` signal, a dedicated thread loads the new `keys.toml` from disk and performs an atomic pointer swap using `arc-swap`.

Because there is no `RwLock` involved, new connections authenticating against the `KeyStore` never contend for read locks, ensuring the connection initialization path remains entirely wait-free.

```rust
// In the SIGHUP signal handler:
match crate::security::keys::KeyStore::load(&keys_file) {
    Ok(store) => {
        tenant_limits.reload(store.tenant_records());
        // Atomic pointer swap; no read locks blocked
        security_keys.store(Arc::new(store));
        info!("reloaded keys and per-tenant limits on SIGHUP");
    }
    Err(e) => warn!(error = %e, "SIGHUP reload failed"),
}
```

---

## Not shipping (superseded)

| Direction | Replaced by |
|-----------|-------------|
| FlatBuffers typed values | **ZDoc** binary (`VK_ZDOC = 0x01`) in `zydecodb-document` |
| Glommio / io_uring / thread-per-core | **std threads** + `EngineHandle` (write mutex + separate cache/fair/WAL sync domains) |
| RESP2 Redis wire / HTTP REST document API | **Length-prefixed binary** frames on TCP/UDS (`zydecodb-engine::frame`) |

Sources of truth: [`PROTOCOL.md`](PROTOCOL.md), this file, [`GUIDE.md`](GUIDE.md#security).

## Durability / directory fsync

Every create/rename that durability depends on must fsync the **file** and the
**containing directory**. Audit (1.0 Section 2):

| Surface | File sync | Directory sync | Notes |
|---------|-----------|----------------|-------|
| WAL segment create/roll | `sync_all` on segment fd | via data/wal open path | `open_new_wal_segment` |
| SSTable flush / compaction rename | tmp `sync_all` then rename | `fsync_dir(data_dir)` | flush/compaction workers |
| LSM `MANIFEST` | append + `sync_all` | N/A (append-only file) | |
| Change-log archive segment | `sync_all` on dst | before manifest rewrite | `archive_segment` |
| Change-log `manifest.json` | tmp `sync_all` + rename | `archive_dir` `sync_all` | `persist_manifest` |
| Base snapshot | `SNAPMETA` `sync_all` | `fsync_dir(snapshot)` | `snapshot_to` |
| Shipping segment + `shipped.log` | segment + log `sync_all` | `ship_dir` after both | fixed in Section 2 |
| Shipping heartbeat rename | tmp `sync_all` + rename | `ship_dir` | fixed in Section 2 |

Failpoint crash matrix: `cargo test -p zydecodb-engine --features failpoints --test crash_matrix -- --test-threads=1`.

**Fuzz findings:** every crash from fuzz must land as a committed regression
unit test in the owning crate **before** the fix merges.

## Authorization audit matrix

Wire-level proof that every `Command` opcode enforces the same auth/role/tenant
rules. **Test:** `crates/zydecodb/tests/authz_matrix.rs` (also gated in CI).
**Replica mutators:** `crates/zydecodb/tests/replica_write_reject.rs`.

Roles: `read_only` / `read_write` / `admin`. Tenants are 16-byte ids on the key;
prefix ACL is optional per key (`allowed_prefixes`).

| Opcode class | Anon (auth on) | read_only | read_write | admin | Prefix ACL deny |
|--------------|----------------|-----------|------------|-------|-----------------|
| KV/doc **reads** (Get, Find, Query, Count, Aggregate, …) | Unauthorized | Ok\* | Ok\* | Ok\* | Forbidden |
| KV/doc **writes** (Put, Del, DocPut, Update, Delete, DocPutIfMatch, DocUpdateIfMatch, IndexDef) | Unauthorized | Forbidden | Ok\* | Ok\* | Forbidden |
| `Begin` | Unauthorized | Forbidden | Ok | Ok | — |
| `Commit` / `Rollback` | Unauthorized | Commit: no-tx ProtocolError; Rollback: Ok | Ok | Ok | — |
| `Watch` | Unauthorized | Ok\* (primary) | Ok\* | Ok\* | Forbidden |
| `SetContext` / `AdminDropTenant` | Unauthorized | Forbidden | Forbidden | Ok | — |
| `Ping` | Ok (default `allow_unauthenticated_ping`) | Ok | Ok | Ok | — |
| `SchemaDef` | ProtocolError (reserved) | ProtocolError | ProtocolError | ProtocolError | — |

\* Ok / NotFound both mean the authz gate passed (empty tenant or missing doc).

**Replica Forbidden set:** Put, Del, DocPut, DocDel, Update, Delete,
DocPutIfMatch, DocUpdateIfMatch, IndexDef, AdminDropTenant, Begin, Commit,
Rollback, SetContext, Watch. Reads + Ping/Stats/SessionInit remain allowed
(subject to auth).

### Supply chain

CI job `cargo-audit` runs `cargo audit` on every PR/push (includes self-update
deps `ureq` / `flate2` / `tar`). Dependabot weekly for `cargo` + `github-actions`
(`.github/dependabot.yml`). Prefer upgrade over advisory allowlists.

## Reference workloads (published numbers)

Operator-facing numbers live in [`GUIDE.md`](GUIDE.md#performance). This harness
is the **repro source** for those tables (engine soak remains the **stability**
gate).

**Binary:** `crates/zydecodb/src/bin/ref-workloads.rs`  
**Driver:** `scripts/ref-workloads.sh`

| Workload | Setup | Ops measured |
|----------|--------|----------------|
| `point_get` | 10k KV keys preloaded | GET throughput + p50/p99 |
| `find_indexed` | collection + secondary index | filtered Find |
| `find_unindexed` | same shape, no usable index | filtered Find (scan) |
| `upsert` | DocPut seed + Update `upsert=true` | upsert rate + p99 |
| `tx_commit` | Begin → DocPut → Commit | commits/sec + p99 |
| `aggregate` | `$group` over 1k docs | pipeline rate + p99 |
| `watch_fanout` | K Watch subscribers + writers | write + drain lag |

```bash
# Maintainer-class run (release binary, seeded, JSON out)
OPS=2000 OUT=docs/soak-baselines/ref-workloads-local.json scripts/ref-workloads.sh

# Quick local check
OPS=40 cargo run -p zydecodb --bin ref-workloads -- --ops 40
```

Ephemeral server: auth off, `rate_limit_rps=1_000_000`, change streams on
(for `watch_fanout`). Document CPU / RAM / disk / OS in the GUIDE table caption
whenever you refresh published numbers.

### Bench regression (nightly)

Short paced engine put/get mix (~minutes), not Criterion:

**Binary:** `crates/zydecodb-engine/src/bin/bench-regression.rs`  
**Driver:** `scripts/bench-regression.sh`  
**Baseline:** [`soak-baselines/bench-baseline.json`](soak-baselines/bench-baseline.json)

```bash
# Emit JSON only
scripts/bench-regression.sh

# Fail if p99_us or rss_bytes regresses >20% vs committed baseline
COMPARE=1 scripts/bench-regression.sh
```

CI: `.github/workflows/bench-nightly.yml` (schedule + `workflow_dispatch`).
GitHub `ubuntu-latest` is noisy — the gate is coarse: fail if `p99_us` or
`rss_bytes` exceeds `max(baseline×1.2, baseline + floor)` (floor 100 µs /
8 MiB). Not a μs SLA.

## Soak testing

Internal long-running stress harness for release validation. End users do not need this — see the [README](../README.md) for embedding the engine.

The soak answers: is memory stable? Are errors zero? Is compaction healthy? Is throughput drifting down?

**Harness:** `crates/zydecodb-engine/src/bin/engine-soak.rs`  
**Driver:** `scripts/soak.sh`  
**Analyzer:** `scripts/analyze-soak.py`

#### Crash-recovery kill loop

SIGKILL the soak at random intervals, reopen, CRC-scan WAL segments, probe
put/get/flush. CI runs ~35 minutes nightly (`.github/workflows/crash-soak.yml`).
Multi-hour VPS runs use the same script:

```bash
./scripts/crash-soak.sh                         # ~35 min (CI default)
MINUTES=180 OUT_DIR=/var/tmp/crash-soak ./scripts/crash-soak.sh
CYCLES=50 KILL_MIN_MS=200 KILL_MAX_MS=3000 ./scripts/crash-soak.sh
```

Gate: zero failed reopens; integrity must pass every cycle.

#### Multi-tenant isolation (simulated pods)

No real multi-tenant fleet required. Two tenants on one engine — victim vs noisy neighbor — measuring e2e put p99 delta (Busy retries included):

```bash
./scripts/tenant-isolation-soak.sh              # steady + ramp-up (default)
MODE=steady ./scripts/tenant-isolation-soak.sh  # ship bar only (δ ≤ 50ms)
MODE=rampup ./scripts/tenant-isolation-soak.sh  # FairDB reclaim (δ ≤ 350ms)
```

Binary: `crates/zydecodb-engine/src/bin/tenant-isolation-soak.rs`.

- **Steady:** V solo → V|N fair=off → V|N fair=on. Ship gate: fair-on e2e put p99 δ ≤ 50 ms, success ≥ 85%.
- **Ramp-up:** N floods while V is idle, then V bursts to reclaim ~one fair share of the write buffer. Gate: fair-on reclaim p99 δ ≤ 350 ms (paper-like buffer δ). This is the honest hard case — do not confuse it with steady ship.

CI: `.github/workflows/tenant-isolation-soak.yml` runs `MODE=both` on a nightly schedule and `workflow_dispatch` (not every PR). Fast PR coverage for TOML→engine enablement remains `crates/zydecodb/tests/fair_pods_config.rs`.

### Quick commands

```bash
# 6-minute smoke
HOURS=0.1 OPS=3000 OUT_DIR=soak-runs/quick scripts/soak.sh --no-analyze

# 90-minute release gate
HOURS=1.5 OPS=3000 OUT_DIR=soak-runs/phase1-memo6-90m scripts/soak.sh --no-analyze
python3 scripts/analyze-soak.py --mode stability soak-runs/phase1-memo6-90m/metrics.jsonl
python3 scripts/analyze-soak.py --mode perf soak-runs/phase1-memo6-90m/metrics.jsonl   # informational

# 24h uncapped on a clean VPS
export VPS_HOST=your.server.ip
scripts/vps-soak.sh setup    # once
scripts/vps-soak.sh deploy
scripts/vps-soak.sh start    # default: HOURS=24 OPS=0 SAMPLE_EVERY=60
scripts/vps-soak.sh status
scripts/vps-soak.sh analyze  # pull metrics + run analyzer locally
```

### Environment variables (`scripts/soak.sh`)

| Variable | Default | Notes |
|----------|---------|-------|
| `HOURS` | 24 | Duration |
| `OPS` | 1000 | Target ops/sec (`0` = uncapped) |
| `SCAN_PCT` | 0 | Range scan mix |
| `SNAPSHOT_EVERY` | 0 | Owned snapshot interval (seconds) |
| `SAMPLE_EVERY` | 60 | Metrics sample interval |
| `POLL_COMPACTION_MS` | 50 | `poll_compaction` cadence |
| `BLOCK_CACHE_MB` | 640 | Data block cache |
| `RESULT_CACHE_MB` | 0 | Result cache |
| `OUT_DIR` | `soak-runs/<timestamp>/` | Output directory |

Workload: **70% PUT / 25% GET / 5% DEL**, 80% hot keys, values 64–1024 B.

### Output

Under `OUT_DIR/`:

- `metrics.jsonl` — header + per-minute samples + summary
- `stderr.log` — errors
- `data/`, `wal/` — engine state (gitignored; delete after forensics)

Archived baselines: [`soak-baselines/`](soak-baselines/).

### Analyzer modes

```bash
python3 scripts/analyze-soak.py --mode stability  metrics.jsonl
python3 scripts/analyze-soak.py --mode perf       metrics.jsonl
python3 scripts/analyze-soak.py --mode all        metrics.jsonl
```

Steady-state window = samples after first 10% warm-up.

#### Stability gates

| Check | Ceiling | What it catches |
|-------|---------|-----------------|
| `errors` | 0 | Engine or harness failures |
| `compaction_repack_total` | 0 | Whole-level L2 repack storms |
| `compaction_rejected_no_progress` | 0 | Planner tried a no-op compaction |
| `compaction_write_amp` | < 5.0 | Compaction rewriting too much data |
| L2 file count | bytes-derived (`ceil(l2_bytes / 64MB) + 2`) | Fragmentation (paced runs only) |
| RSS max | derived from JSONL header | Memory runaway |
| `space_amplification` | ≤ 3.0 | Disk much larger than live data |

L2 file-count gates are calibrated for paced (~3k ops/s) runs. Uncapped capacity runs may fail them without data loss.

#### Performance mode

Tracked: p99/p999, ops/sec ratio vs target. Informational unless throughput trends down over the run.

Exit code **0** = pass; **2** = breach (mode-dependent).
