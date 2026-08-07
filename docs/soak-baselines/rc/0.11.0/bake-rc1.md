# 1.0.0-rc.1 bake log

| | |
|---|---|
| Tag | `v1.0.0-rc.1` / `clients/go/v1.0.0-rc.1` |
| Commit | `7da1a5d` |
| Bake start | 2026-08-07 |
| Earliest final tag | 2026-08-14 (≥1 week) |

## Drills (2026-08-07)

- Restore: `restore_equivalence`, `pitr_restore`, `admin_snapshot_crash` (failpoints) — pass
- Failover: `promote_under_load` (promote, retention gap, fenced ex-primary) — pass
- `cargo audit` — clean (2 allowed warnings only)
- Conformance vectors — fresh / no diff

## 24h paced soak

- Out: `soak-runs/rc-bake-24h-20260807T020449Z/`
- Config: `HOURS=24 OPS=3000` GA mix (55/25 put/get, 80% hot)
- Gate: `python3 scripts/analyze-soak.py --mode stability …/metrics.jsonl` exit 0
- On green: optionally promote to `docs/soak-baselines/rc/1.0.0-rc.1/24h-paced.jsonl`
