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

## Soak testing

Internal long-running stress harness for release validation. End users do not need this — see the [README](../README.md) for embedding the engine.

The soak answers: is memory stable? Are errors zero? Is compaction healthy? Is throughput drifting down?

**Harness:** `crates/engine/src/bin/engine-soak.rs`  
**Driver:** `scripts/soak.sh`  
**Analyzer:** `scripts/analyze-soak.py`

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
