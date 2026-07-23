# Multi-tenant pods hosting

One-page runbook for hosting **many tenants in one ZydecoDB process** with
δ-fair isolation. For local single-tenant use, prefer `zydecodb serve` (no
config) or [`config/zydecodb.dev.toml`](../config/zydecodb.dev.toml) — fair stays
**off** there by design.

Multi-tenant namespaces only need `legacy_single_tenant = false` and per-tenant
API keys. This page is for hosts that also want noisy-neighbor controls
(`[fair] enabled = true`).

## 1. Config

```bash
cp config/zydecodb.pods.example.toml /etc/zydecodb/config.toml
# Edit data_dir, wal_dir, keys_file, listen / listen_unix as needed.
```

The pods example sets:

- `legacy_single_tenant = false` — every key is prefixed with a 16-byte tenant
- `[fair] enabled = true` — δ-fair memtable/cache/stall isolation
- optional `[runtime] profile = "low_footprint"` — smaller RSS (does **not** enable fair by itself)

## 2. Keys and tenants

```bash
zydecodb admin keys create --id app-a --role read_write \
  --tenant <32-hex-tenant-a> --keys-file /etc/zydecodb/keys.toml
zydecodb admin keys create --id app-b --role read_write \
  --tenant <32-hex-tenant-b> --keys-file /etc/zydecodb/keys.toml
# Optional admin key for SetContext / live drop-tenant:
zydecodb admin keys create --id ops --role admin \
  --tenant 00000000000000000000000000000000 --keys-file /etc/zydecodb/keys.toml
```

Point `security.keys_file` at that file. See [`SECURITY.md`](SECURITY.md) for
roles, prefix ACLs, and `[[tenant]]` byte/RPS caps.

## 3. Serve

```bash
zydecodb serve --config /etc/zydecodb/config.toml
```

Optional Unix socket for local control-plane traffic (auth still applies):

```toml
listen_unix = "/var/run/zydecodb/zydecodb.sock"
```

## 4. Per-tenant limits

```bash
zydecodb admin tenant set-limit --tenant <hex> --max-bytes N --rate-rps R \
  --keys-file /etc/zydecodb/keys.toml
# Reload live limits without restart:
kill -HUP "$(pidof zydecodb)"
zydecodb admin tenant list --keys-file /etc/zydecodb/keys.toml
```

## 5. Prove isolation before claiming δ

Ship gates (separate claims):

| Mode | Gate |
|------|------|
| Steady | fair-on victim put p99 δ ≤ 50 ms, success ≥ 85% |
| Ramp-up reclaim | δ ≤ 350 ms, success ≥ 85% |

```bash
./scripts/tenant-isolation-soak.sh                 # MODE=both (default)
MODE=steady ./scripts/tenant-isolation-soak.sh
MODE=rampup ./scripts/tenant-isolation-soak.sh
```

CI: `.github/workflows/tenant-isolation-soak.yml` runs nightly and on
`workflow_dispatch` (ubuntu-latest). **Re-prove on your hardware** before
marketing numbers — simulated soak is not a fleet SLA.

Last proven locally (developer workstation, `MODE=steady`, 10s/phase): fair-on
steady δ cleared the ≤ 50 ms ship gate. Re-run `MODE=both` on your box before
claiming both steady and ramp-up numbers.

Fast PR coverage (enable path only): `crates/zydecodb/tests/fair_pods_config.rs`.

## 6. Offboard a tenant

```bash
# Offline (node stopped):
zydecodb admin drop-tenant --config /etc/zydecodb/config.toml --tenant <hex> [--compact]

# Live (admin API key; prefers listen_unix):
export ZYDECODB_API_KEY="zdk_..."
zydecodb admin drop-tenant --live --tenant <hex> [--compact]
```

## 7. Security baseline

- Require auth; do not expose `:9470` to the internet without TLS + keys.
- Metrics stay on loopback unless you opt into remote + bearer token.
- Full checklist: [`SECURITY.md`](SECURITY.md).

## See also

- Sharing model and δ claims: [`SECURITY.md#multi-tenant-sharing-model`](SECURITY.md#multi-tenant-sharing-model)
- Soak harness details: [`SOAK.md`](SOAK.md)
- Example TOML: [`config/zydecodb.pods.example.toml`](../config/zydecodb.pods.example.toml)
