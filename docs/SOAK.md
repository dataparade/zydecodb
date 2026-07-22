# Soak testing (maintainers)

Internal long-running stress harness for release validation. End users do not need this — see the [README](../README.md) for embedding the engine.

The soak answers: is memory stable? Are errors zero? Is compaction healthy? Is throughput drifting down?

**Harness:** `crates/engine/src/bin/engine-soak.rs`  
**Driver:** `scripts/soak.sh`  
**Analyzer:** `scripts/analyze-soak.py`

### Multi-tenant isolation (simulated pods)

No real multi-tenant fleet required. Two tenants on one engine — victim vs noisy neighbor — measuring e2e put p99 delta (Busy retries included):

```bash
./scripts/tenant-isolation-soak.sh              # steady + ramp-up (default)
MODE=steady ./scripts/tenant-isolation-soak.sh  # ship bar only (δ ≤ 50ms)
MODE=rampup ./scripts/tenant-isolation-soak.sh  # FairDB reclaim (δ ≤ 350ms)
```

Binary: `crates/zydecodb-engine/src/bin/tenant-isolation-soak.rs`.

- **Steady:** V solo → V|N fair=off → V|N fair=on. Ship gate: fair-on e2e put p99 δ ≤ 50 ms, success ≥ 85%.
- **Ramp-up:** N floods while V is idle, then V bursts to reclaim ~one fair share of the write buffer. Gate: fair-on reclaim p99 δ ≤ 350 ms (paper-like buffer δ). This is the honest hard case — do not confuse it with steady ship.

## Quick commands

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

## Environment variables (`scripts/soak.sh`)

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

## Output

Under `OUT_DIR/`:

- `metrics.jsonl` — header + per-minute samples + summary
- `stderr.log` — errors
- `data/`, `wal/` — engine state (gitignored; delete after forensics)

Archived baselines: [`soak-baselines/`](soak-baselines/).

## Analyzer modes

```bash
python3 scripts/analyze-soak.py --mode stability  metrics.jsonl
python3 scripts/analyze-soak.py --mode perf       metrics.jsonl
python3 scripts/analyze-soak.py --mode all        metrics.jsonl
```

Steady-state window = samples after first 10% warm-up.

### Stability gates

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

### Performance mode

Tracked: p99/p999, ops/sec ratio vs target. Informational unless throughput trends down over the run.

Exit code **0** = pass; **2** = breach (mode-dependent).
