# Soak baselines

Versioned archives of release-validation and reference-workload metrics.
Reproduce with [`INTERNAL.md`](../INTERNAL.md#soak-testing) and
[`INTERNAL.md`](../INTERNAL.md#reference-workloads-published-numbers).

`soak-runs/` is ephemeral local scratch — never treat it as an archive. Only
promote a finished run into this directory.

## Current gates

| File | Run | Notes |
|------|-----|-------|
| `ga-24h-paced.jsonl` | 24h @ 3k ops | Stability ceilings / 72h compare (amp 2.89, 255M ops, 0 errors) |
| `ga-90m.jsonl` | 90m @ 3k ops | Short stability gate reference (CI / `release-soak.yml`) |
| `bench-baseline.json` | `bench-regression` 50k ops | Nightly `COMPARE=1` baseline |
| `ref-workloads-local.json` | `ref-workloads` OPS=2000 | Source for GUIDE Performance table |

Paced confirmation soak: `scripts/run-engine-ga-24h.sh`.

## RC capacity

Per-version uncapped VPS soaks live under `rc/<version>/`:

| Path | Notes |
|------|-------|
| `rc/0.11.0/24h-uncapped.jsonl` | Capacity archive @ `e80de81`; ~33k ops/s mean, amp 3.35 |
| `rc/0.11.0/notes.md` | Hardware + how to read uncapped analyzer gates |

### Archiving a new RC 24h uncapped soak

```bash
export VPS_HOST=your.server.ip
scripts/vps-soak.sh setup && scripts/vps-soak.sh deploy
HOURS=24 OPS=0 RUN_NAME=rc-<version>-24h-uncapped scripts/vps-soak.sh start
# after completion:
RUN_NAME=rc-<version>-24h-uncapped scripts/vps-soak.sh pull
mkdir -p docs/soak-baselines/rc/<version>
cp soak-runs/rc-<version>-24h-uncapped/metrics.jsonl \
  docs/soak-baselines/rc/<version>/24h-uncapped.jsonl
# write docs/soak-baselines/rc/<version>/notes.md with CPU/RAM/disk/OS
```

## Historical

Older memo-era snapshots that are not gates: [`archive/`](archive/).
