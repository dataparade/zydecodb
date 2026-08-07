# RC 0.11.0 soak notes

Commit under test: `e80de81` (owned-snapshot tombstone/ceiling fix).

## Uncapped capacity (VPS, OPS=0, 24h)

| | |
|---|---|
| Host | DigitalOcean droplet `ubuntu-s-8vcpu-16gb-nyc1` (`146.190.78.233`) |
| Specs | 8 vCPU, 16 GiB RAM, 309 GiB disk, no swap |
| OS / kernel | Ubuntu 24.04, Linux 6.8.0-124-generic |
| Artifact | `docs/soak-baselines/rc/0.11.0/24h-uncapped.jsonl` |
| Mix | soak.sh defaults under OPS=0 (70% put / 25% get / 5% del, 80% hot, 64–1024B) |

### Results

- **2,824,739,559 ops** in 86400s → **32,694 ops/s average**
- Steady-state mean ~32.4k ops/s (min 27.0k / p99 36.3k after warm-up)
- Peak early rate ~66k ops/s; settles as live set + compaction grow
- Final write amp **3.35** (ceiling 5.0)
- RSS max **1.17 GB**, plateau ~1.0 GB (derived stability ceiling passed)
- Open FDs max **37** (ceiling 48)
- Flush backlog never sustained (immutable mean/max 0)
- Shutdown OK in 0.25s

### Analyzer “breaches” (expected under uncapped)

The stability analyzer is calibrated for **paced** GA soaks (`OPS=3000`, zero
op errors). Applied blindly to uncapped it reports:

1. **Total op errors = 906,067** — all `EngineBusy: compaction backlog`.
   Backpressure shedding under saturation; 12 of 1439 samples, ~0.03% of ops.
   Correct overload behavior, not data-path failure.
2. **L2 file-count / median-size floors** — topology under continuous max write
   pressure differs from paced GA; not a leak or correctness signal.
3. Perf p999 / max-op spikes — apply/fsync stalls under saturation (also warned
   as poll/apply/manifest max µs). Capacity headline remains the sustained mean.

Capacity claim for docs: **~33k ops/s sustained** (GA mix, this hardware) with
graceful `EngineBusy` under compaction backlog.

## Paced endurance (local, OPS=3000, 72h)

| | |
|---|---|
| Artifact | `docs/soak-baselines/rc/0.11.0/72h-paced.jsonl` |
| Mix | 55% put / 25% get / 20% del, 80% hot, 64–1024B, seed `16045690984503098046` |
| Samples | 4314 (~71.9h wall) |

### Results

- **~760M ops**, mean **~2930 ops/s** (target 3000)
- Write amp max **2.90** (plateau ~2.89 from day one — matches `ga-24h-paced`)
- RSS max **1.15 GB**, mean ~1.0 GB (derived stability ceiling passed)
- Open FDs max **33** (ceiling 48)
- Flush backlog never sustained (immutable mean/max 0)
- Stability analyzer: **OK — no ceiling breaches**

Endurance claim: paced GA mix holds a flat RSS/amp/FD plateau across a multi-day
run on the release-candidate tree.
