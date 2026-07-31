# Soak baselines

Archived `metrics.jsonl` / JSON summaries from release-validation and reference
workload runs. Maintainer reference — reproduce with [`INTERNAL.md`](../INTERNAL.md#soak-testing)
and [`INTERNAL.md`](../INTERNAL.md#reference-workloads-published-numbers).

| File | Run | Notes |
|------|-----|-------|
| `memo4-90m.jsonl` | 90m @ 3k ops | Earlier perf baseline |
| `memo6-6m-v2.jsonl` | 6m @ 3k ops | Beta-candidate snapshot |
| `memo6-90m.jsonl` | 90m @ 3k ops | Stability gate reference |
| `ref-workloads-local.json` | `ref-workloads` OPS=2000 | Source for GUIDE Performance table |
| `bench-baseline.json` | `bench-regression` 50k ops | Nightly COMPARE=1 baseline |
| `rc-<ver>-24h-uncapped.jsonl` | VPS `OPS=0` 24h | Per-RC archive (add with `notes.md`) |

### Archiving an RC 24h uncapped soak

```bash
export VPS_HOST=your.server.ip
scripts/vps-soak.sh setup && scripts/vps-soak.sh deploy
HOURS=24 OPS=0 scripts/vps-soak.sh start
# after completion:
scripts/vps-soak.sh pull
cp soak-runs/<run>/metrics.jsonl docs/soak-baselines/rc-<version>-24h-uncapped.jsonl
# write docs/soak-baselines/rc-<version>-notes.md with CPU/RAM/disk/OS
```

Paced 24h confirmation (not a substitute for uncapped): `scripts/run-engine-ga-24h.sh`.
