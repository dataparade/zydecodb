# ParadeKV Engine — GA Checklist

> **Note:** This is an aspirational long-term checklist, not the beta ship gate.
> For what actually blocks `0.9.0-beta.1`, see [README § Status](../../README.md#status).

**Target:** Embeddable LSM engine GA (not hosted NoSQL product).  
**Horizon:** MEMO6 — reader-cache memory cap + 90m/24h confirmation.

## Hard blockers (must pass before beta tag)

| Criterion | Verification |
|-----------|--------------|
| Crash / failpoint matrix | `cargo test -p paradekv-engine --features failpoints --test crash_matrix -- --test-threads=1` |
| Disk-full recovery | `cargo test -p paradekv-engine --features failpoints --test resilience_failpoints -- --test-threads=1` |
| Space reclamation | L2 bottommost GC + tombstone drop at max level; integration tests in `compaction.rs` |
| Bounded `space_amplification` | Soak JSONL + `scripts/analyze-soak.py --mode stability` (`TODO_SPACE_AMP_MAX_CEILING`) |
| Bounded RSS | Derived from soak header: caches + `max_open_readers × 1.5 MB` metadata + 256 MB headroom |
| Stability soak | 90m green, then 24h confirmation |
| Documented perf envelope | Hardware, cache budgets, workload mix archived with soak JSONL |
| Stable public API | `Engine`, `EngineConfig`, `SnapshotView`, `SnapshotHandle` |

## Memory model (MEMO6)

- **Data blocks:** `EngineConfig::block_cache_bytes` — LRU data-block cache only (RocksDB default).
- **Metadata:** index and bloom pinned per open reader; bounded by `EngineConfig::max_open_readers` (table cache, default 128).
- **Result cache:** `result_cache_bytes` defaults to **0**. Opt-in only; disabled in GA soak defaults.
- **GA soak defaults:** 640 MB block cache, 0 MB result cache, 128 max open readers.
- **RSS headroom (256 MB):** memtable + engine/WAL/allocator slop. Metadata estimated as `max_open_readers × 1.5 MB` in `analyze-soak.py`. Override via `SOAK_RSS_HEADROOM_MB` / `SOAK_PER_READER_METADATA_MB`.

## Stability gates (`analyze-soak.py --mode stability`)

- `errors == 0`
- `compaction_repack_total == 0`
- `compaction_rejected_no_progress == 0`
- `compaction_write_amp < 5`
- L2 bytes-derived file count
- RSS ≤ derived ceiling from JSONL header (~1088 MB at MEMO6 defaults: 640 + 192 metadata + 256 headroom)
- `space_amplification` ≤ 3.0

## Performance SLOs (tracked, not GA-blocking)

Use `scripts/analyze-soak.py --mode perf` or `--mode all`:

- Throughput ratio vs target (≥ 95%)
- p99 / p999 / max single-op latency

Regression-test perf against the previous release JSONL, not an arbitrary synthetic ceiling.

## Explicit non-goals for engine GA

- MVCC, write batches, secondary indexes
- Hosted-product features (auth, quotas, HTTP — server layer)
- Owned-snapshot backup / PITR API
- Second compaction worker
- Sub-ms p99 at 3k ops/sec in `engine-soak` as graduation bar

## Recommended verification ritual

1. `cargo test -p paradekv-engine`
2. Failpoint suite (CI job `failpoints`)
3. 90m soak: `scripts/soak.sh` with MEMO6 defaults (640 MB block cache, result cache off, 128 readers)
4. `scripts/analyze-soak.py --mode stability soak-runs/.../metrics.jsonl`
5. 24h confirmation: `scripts/run-engine-ga-24h.sh`
6. Tag `paradekv-engine` `0.9.0-beta.1`

## Roadmap status

| Phase | Deliverable |
|-------|-------------|
| MEMO4 | Block cache tuning, space_amp, result cache module, split gates |
| MEMO5 | Cut result cache from defaults; derived RSS gate (metadata-in-LRU reverted in MEMO6) |
| MEMO6 | Reader table cache; metadata pinned off LRU; `optimize_filters_for_hits` on L2 |
| GA ship | 90m + 24h soaks green, beta tag |
