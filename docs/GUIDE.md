# Operator Guide

This guide covers how to run ZydecoDB securely and operate it in production: auth and tenants, multi-tenant pods hosting, WAL shipping and read replicas, failover, snapshot/restore, and version upgrades. Follow the security baseline before exposing a port beyond loopback. Use the pods and soak sections when hosting many tenants in one process.

## Security

ZydecoDB secures the **wire and key namespace**. Your application secures **humans** (passwords, sessions, business rules).

Reference implementation: [`examples/user_backend/`](../examples/user_backend/) — Flask handles users; ZydecoDB holds bytes behind an API key.

### Threat model

| Layer | Defends |
|-------|---------|
| **Your HTTP API** | User login, passwords, OAuth, "who can edit what" |
| **Network** | Firewall, private VPC, do not expose `:9470` publicly |
| **ZydecoDB** | API keys, tenant isolation, TLS, rate limits, quotas, audit logs |

### Deployment modes

#### Localhost dev (default)

```toml
listen = "127.0.0.1:9470"

[security]
require_auth = false
```

Use [`config/zydecodb.dev.toml`](../config/zydecodb.dev.toml) for writable `/tmp` paths. Auth is optional on loopback.

#### App behind API (recommended)

```text
Internet → Your Flask/Go API → ZydecoDB (127.0.0.1 or private IP)
```

- Run ZydecoDB on loopback or a private subnet.
- Your API holds `ZYDECODB_API_KEY`.
- End users never touch the database port.

#### LAN / private network

```toml
listen = "0.0.0.0:9470"

[security]
require_auth = true
keys_file = "/etc/zydecodb/keys.toml"
```

With `require_auth = "auto"` (default), auth is required whenever `listen` is not loopback.

### Admin CLI — API keys

```bash
# Create (prints secret once; only hash stored on disk)
zydecodb admin keys create \
  --id backend \
  --role read_write \
  --keys-file /etc/zydecodb/keys.toml

# List key ids
zydecodb admin keys list --keys-file /etc/zydecodb/keys.toml

# Revoke
zydecodb admin keys revoke --id backend --keys-file /etc/zydecodb/keys.toml
```

Roles:

| Role | PUT / DEL | GET | Notes |
|------|-----------|-----|-------|
| `read_only` | denied | allowed | Analytics, replicas |
| `read_write` | allowed | allowed | Default for app backends |
| `admin` | allowed | allowed | Can send `SetContext` to switch tenant |

Optional prefix ACL per key (enforced on **both** raw KV keys and document collection names):

```toml
allowed_prefixes = ["events:", "metrics:"]
```

- **KV:** the client key must start with one of the prefixes.
- **Documents:** the collection name must start with a prefix, or equal the prefix with a trailing `:` stripped (so `events:` allows collection `events`).
- **Aggregate** is an authenticated read and uses the same collection-prefix ACL.
- **Watch** (change streams) is primary-only, uses the same collection-prefix ACL, and is subject to `[change_streams]` subscription caps. Read-only replicas reject Watch. SIGHUP key revocation terminates active streams.

Dev-only bootstrap: set `ZYDECODB_BOOTSTRAP_KEY` instead of a keys file. The server **refuses to start** if this env var is set and `listen` is not loopback. Use a real `keys.toml` for any networked bind.

Fail-closed startup guards:

- **Auth required + empty keys file + no bootstrap** → the server refuses to start (a server that can never authenticate anyone is a misconfiguration, not a service).
- **`legacy_single_tenant = true` + any key with a non-zero tenant** → refused; the two key layouts must not be mixed. Set `legacy_single_tenant = false` for multi-tenant deployments.
- **Group/world-readable `keys_file` or shipping/replica `hmac_key_file`** (Unix mode `& 0o077 != 0`) → refused; `chmod 600` the file.
- **`data_dir` / `wal_dir` equal to `shipping.ship_dir` or `replica.from`** → refused (paths compared after canonicalize when possible).
- **`ship_dir` / `replica.from` without `hmac_key_file`** → refused with an actionable generate/`chmod 600` message.
- **Auth required + `max_bytes_per_tenant = 0`** → starts, but logs a warning to set a non-zero quota.

Key verification is O(1): `admin keys create` stores a `secret_lookup` (sha256 of the secret) used as an index into the keystore, so auth performs exactly one argon2 verify regardless of how many keys exist. Keys minted before this field existed still verify via a linear scan — reissue them to get the fast path (the server logs a warning at startup when such keys are present).

### Connection handshake

When auth is required, the **first** message must be `SessionInit` (`0x40`) with the full API key as UTF-8 bytes:

```text
Client → SessionInit(api_key)
Server → OK or Unauthorized (0x0B)

Client → PUT / GET / DEL / ...
```

Python ([`examples/zydecodb_client.py`](../examples/zydecodb_client.py)):

```python
ZydecoDBClient("127.0.0.1", 9470, api_key="zdk_...")
# or: export ZYDECODB_API_KEY=...
```

`Ping` (`0xF0`) may be allowed before auth when `allow_unauthenticated_ping = true` (health checks).

### Key file format

See [`config/zydecodb.keys.example.toml`](../config/zydecodb.keys.example.toml). Only **argon2id hashes** are stored — never plaintext secrets.

| Field | Meaning |
|-------|---------|
| `id` | Label in audit logs |
| `secret_hash` | argon2id hash of the full `zdk_...` secret |
| `secret_lookup` | sha256 of the secret; O(1) keystore index (written by `admin keys create`) |
| `role` | `read_only`, `read_write`, or `admin` |
| `tenant` | 32 hex chars → 16-byte namespace |
| `allowed_prefixes` | Optional; empty = entire tenant. Applies to KV key prefixes and document collection names |

### Tenant isolation

Stored engine keys use layout `0x01 | tenant(16) | your_key`. Each API key is scoped to one tenant.

- `legacy_single_tenant = true` (default): when tenant is all zeros, uses old layout `0x01 | your_key` for backward compatibility.
- `legacy_single_tenant = false`: always prefix with tenant (multi-tenant hosted).

Upgrade note: keep `legacy_single_tenant = true` until your existing data is migrated — flipping it orphans keys written under the old layout. The server refuses to start when `legacy_single_tenant = true` is combined with any non-zero-tenant key, so a legacy volume can never be half-migrated by accident. Greenfield deployments (including the Docker config) should use `false`.

Admins can switch tenant mid-connection with `SetContext` (`0x41`) and a 16-byte tenant payload.

### TLS

```toml
[tls]
enabled = true
cert = "/etc/zydecodb/tls.crt"
key  = "/etc/zydecodb/tls.key"
```

#### TLS posture (1.0)

- Implemented with **rustls** ([`security/tls.rs`](../crates/zydecodb/src/security/tls.rs)):
  TLS **1.2+**, library default cipher suites, **no client-certificate (mTLS)** auth.
- PEMs must be owner-readable only (`chmod 600`); serve refuses world-readable
  secret files the same way as `keys.toml` / HMAC keys.
- **Cert reload = process restart.** SIGHUP reloads API keys and tenant limits,
  not TLS material. Rotate certs by installing new PEMs and restarting `serve`.
- Prefer terminating TLS at a reverse proxy only when ZydecoDB stays on loopback
  behind that proxy; otherwise enable `[tls]` on the data port.

Dev self-signed cert:

```bash
openssl req -x509 -newkey rsa:2048 -keyout tls.key -out tls.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

Official drivers speak TLS when configured:

| Driver | Option |
|--------|--------|
| Go | `WithTLS(nil)` or `WithTLS(&tls.Config{...})` |
| TypeScript | `{ tls: true }` or `{ tls: { rejectUnauthorized: false, /* ... */ } }` |
| Python | `tls=True` or `tls=ssl_context` |

Alternative: terminate TLS at nginx or stunnel; keep plain TCP to ZydecoDB on `127.0.0.1`.

#### Unix-domain socket (local transport)

For local control-plane or co-located traffic, listen on a Unix-domain socket in
addition to TCP:

```toml
listen_unix = "/run/zydecodb/zydecodb.sock"
```

TLS is **TCP-only** — the UDS trust boundary is the socket file's filesystem
permissions. The server chmods the socket to `0600` at bind, so only the
server's own user can connect by default; widen deliberately (e.g. a shared
group directory) if co-located services need it. API-key auth still applies on
the socket exactly as it does over TCP.

#### Metrics endpoint

The `[metrics]` HTTP endpoint binds loopback by default. A non-loopback bind is
**refused** unless `allow_remote = true`, and remote binds require a bearer
`token`; `/metrics` then demands `Authorization: Bearer <token>` (constant-time
compared) while `/healthz` and `/readyz` stay open for probes.

#### WAL shipping integrity (HMAC)

When `[shipping] ship_dir` is set, `hmac_key_file` is **required**: each
`shipped.log` entry carries an HMAC-SHA256 over `<id> <seq> <sha256>` so an
attacker with write access to the ship path cannot forge a segment plus a
matching manifest line. A replica (`[replica] from`) requires the same key and
refuses entries without a valid HMAC. See [WAL shipping and restore](#wal-shipping-and-restore).

### Rate limits and quotas

```toml
[security]
max_connections = 256        # drop new TCP connections when full
rate_limit_rps = 1000        # per-connection token bucket
auth_burst_limit = 10        # failed SessionInit per IP per minute
max_sort_buffer = 10000      # max docs buffered per query sort / multi-write select

[security.quotas]
max_bytes_per_tenant = 0     # 0 = unlimited; else write cap per tenant
```

`max_sort_buffer` bounds authenticated memory abuse: one sorted `find` or
filtered `update_many`/`delete_many` can buffer at most this many documents
before the request is rejected with `BadFilter` (add an index or a tighter
filter). The Docker config additionally lowers `rate_limit_rps` to 200 and
`max_connections` to 128 — raise them deliberately if your workload needs it.

Exceeded rate → `EngineBusy` (`0x07`). Exceeded quota → `PolicyRejected` (`0x09`).

#### Multi-tenant sharing model

One ZydecoDB process can host many tenants. What is isolated **today**:

| Isolated | Mechanism |
|----------|-----------|
| Key namespace | `KS_USER \| tenant[16] \| …` prefix |
| Auth / ACL | API keys scoped to a tenant; optional prefix ACLs |
| Admission | Per-tenant byte caps and RPS; global connection limits |
| Offboard | `admin drop-tenant` (offline) or `--live` / `AdminDropTenant` |

What is still **shared** (noisy-neighbor risk) when δ-fair is disabled (default):

| Shared | Effect |
|--------|--------|
| Engine mutex domain | Writers serialize; compaction slowdown was moved off-lock but admission is still global |
| WAL + memtable + block cache | One tenant’s burst can evict or fill shared buffers |
| L0 / compaction backpressure | `EngineBusy` / stalls can affect all tenants |

Product target: well-behaved tenant **steady-state** p99 delay bounded by **δ ≈ 50 ms** under a noisy neighbor. **Measured (simulated soak):** with `[fair]` on, e2e victim put p99 δ clears **≤ 50 ms** on a two-tenant write-flood + cache-thrash harness; fair-off remains much worse. Treat **ramp-up / fair-share reclaim** separately (**≤ 350 ms**) — do not market one number for both.

**Mechanisms when `[fair]` is on:** cache floors; memtable reserved/global pools (`f/4` reserve floor when the ρ formula is 0); per-tenant stall / L0 token attribution + over-share pacing; fair soft flush-queue skips. Optional **Fork B** (`fork_b_l0_domains`) stalls a tenant on its own L0 file debt instead of global L0 `EngineBusy` — off by default; enable only if 5a+5b still miss δ after tuning.

**Lock domains:** the server shares [`EngineHandle`](../crates/zydecodb-engine/src/engine_handle.rs) — write mutex for memtable/WAL append/SST publish; block cache, fair-share state, and WAL group-commit use separate interior locks so cache inserts and fsync do not take the write mutex. Never `thread::sleep` while holding the write lock.

**Enable under pods:** follow the one-page runbook [Multi-tenant pods](#multi-tenant-pods). Start from [`config/zydecodb.pods.example.toml`](../config/zydecodb.pods.example.toml) (`[fair] enabled = true`, `legacy_single_tenant = false`, optional `[runtime] profile = "low_footprint"`). Local single-tenant `zydecodb serve` keeps fair **off** by default — do not wire fair into the low-footprint profile.

**Prove it (simulated pods — no fleet required):**

```bash
./scripts/tenant-isolation-soak.sh                 # steady (≤50ms) + ramp-up reclaim (≤350ms)
MODE=steady ./scripts/tenant-isolation-soak.sh     # ship bar only
MODE=rampup ./scripts/tenant-isolation-soak.sh     # FairDB idle→reclaim hard case
```

Harness: `tenant-isolation-soak` — steady V solo / V\|N fair=off / V\|N fair=on, plus ramp-up (N floods while V idle, then V reclaim burst ≈ fair-share bytes). Steady δ ≤ 50 ms and ramp-up δ ≤ 350 ms are **separate** claims. CI gates these thresholds on `ubuntu-latest` (nightly / `workflow_dispatch` — see `.github/workflows/tenant-isolation-soak.yml`); re-prove on your hardware before claiming numbers. Driver notes: [`INTERNAL.md`](INTERNAL.md#soak-testing).

#### Per-tenant limits

`rate_limit_rps` and `max_connections` above are per-connection/global. For
multi-tenant hosting you can also cap a **specific tenant** — a stored-byte
ceiling and a request-rate ceiling shared across all of that tenant's
connections. These live as `[[tenant]]` tables in the keys file:

```toml
[[tenant]]
tenant = "0123456789abcdef0123456789abcdef"   # 32 hex chars
max_bytes = 1073741824                          # 1 GiB stored-byte cap (omit = unlimited)
rate_rps = 500                                  # requests/sec across this tenant (omit = unlimited)
```

Manage them with the admin CLI instead of editing by hand:

```bash
zydecodb admin tenant set-limit --tenant 0123...cdef --max-bytes 1073741824 --rate-rps 500 \
  --keys-file /etc/zydecodb/keys.toml
zydecodb admin tenant list --keys-file /etc/zydecodb/keys.toml
```

A running server applies limit changes on `SIGHUP` (no restart). A tenant byte
cap falls back to the global `max_bytes_per_tenant` when no `[[tenant]]` override
exists. Exceeding a per-tenant rate ceiling returns `EngineBusy` (`0x07`); the
byte cap returns `PolicyRejected` (`0x09`).

### Audit logging

```toml
[security.audit]
enabled = true
log_client_key = false   # never enable in production without good reason
```

Emits structured `tracing` events: `tenant`, `key_id`, `cmd`, `client_key_len`, `status`, `duration_us`. Secrets and values are never logged.

With `log_client_key = true`, each line also carries `client_key_prefix` — a hex dump of at most the **first 8 bytes** of the client's KV key. The full key is never logged even when enabled; leave it off unless you are actively debugging access patterns.

### Wire status codes (security-related)

| Byte | Name | When |
|------|------|------|
| `0x0B` | Unauthorized | Missing/invalid API key, or command before auth |
| `0x0C` | Forbidden | Valid key but read-only or prefix ACL denied |
| `0x07` | EngineBusy | Rate limit or auth burst limit |
| `0x09` | PolicyRejected | Tenant byte quota exceeded |

### What ZydecoDB does not do

- End-user authentication (use your API — see [`examples/user_backend/`](../examples/user_backend/))
- SQL injection protection (no SQL)
- Encryption at rest (use disk/filesystem encryption)

### Docker

[`config/zydecodb.docker.toml`](../config/zydecodb.docker.toml) and [`docker-compose.yml`](../docker-compose.yml):

- `require_auth = true` and `listen = "0.0.0.0:9470"`
- Metrics bind `127.0.0.1:9471` inside the container (not published)
- Process runs as non-root `zydeco` (uid 1000); Compose drops all capabilities (`cap_drop: [ALL]`), sets `no-new-privileges`, and mounts `/tmp` as tmpfs
- `legacy_single_tenant = false`, `rate_limit_rps = 200`, `max_connections = 128`
- Data/WAL volumes must be writable by uid 1000

Create keys before the first start (host binary or a one-shot container):

```bash
zydecodb admin keys create --id docker --role admin --keys-file config/keys.toml
docker compose up -d
```

`config/keys.toml` is gitignored. Do not set `ZYDECODB_BOOTSTRAP_KEY` in Compose — non-loopback listen rejects it.

### Operations checklist (networked deployments)

Before exposing a ZydecoDB port beyond loopback:

- [ ] `require_auth = true` (or `"auto"`, which enforces it off-loopback)
- [ ] Real keys file created with `admin keys create` (server refuses to start with auth on and zero keys)
- [ ] `ZYDECODB_BOOTSTRAP_KEY` **not** set (refused off-loopback anyway)
- [ ] `[tls] enabled = true` with a real cert/key — see [`config/zydecodb.tls.example.toml`](../config/zydecodb.tls.example.toml)
- [ ] Metrics on loopback, or `allow_remote = true` **with** a bearer `token`
- [ ] Shipping/replication configured with `hmac_key_file` (required when enabled)
- [ ] `legacy_single_tenant = false` unless migrating an old volume
- [ ] Firewall: `:9470` reachable only from app subnets; metrics port never published
- [ ] Rate caps sized for your workload (`rate_limit_rps`, `max_connections`, `max_sort_buffer`, per-tenant quotas)
- [ ] (Multi-party / external production) Seek an **external review** of this
      threat model before others depend on it — process step, not a code gate.
      Maintainer authz proof: [`INTERNAL.md`](INTERNAL.md#authorization-audit-matrix).

### Never do this

- Bind `0.0.0.0:9470` without `require_auth` on the public internet
- Ship Docker with `require_auth = false`
- Set `ZYDECODB_BOOTSTRAP_KEY` on a non-loopback listen address
- Commit API keys, `keys.toml`, or TLS PEMs to git
- Log full keys or values in audit mode

## Multi-tenant pods

One-page runbook for hosting **many tenants in one ZydecoDB process** with
δ-fair isolation. For local single-tenant use, prefer `zydecodb serve` (no
config) or [`config/zydecodb.dev.toml`](../config/zydecodb.dev.toml) — fair stays
**off** there by design.

Multi-tenant namespaces only need `legacy_single_tenant = false` and per-tenant
API keys. This page is for hosts that also want noisy-neighbor controls
(`[fair] enabled = true`).

### 1. Config

```bash
cp config/zydecodb.pods.example.toml /etc/zydecodb/config.toml
# Edit data_dir, wal_dir, keys_file, listen / listen_unix as needed.
```

The pods example sets:

- `legacy_single_tenant = false` — every key is prefixed with a 16-byte tenant
- `[fair] enabled = true` — δ-fair memtable/cache/stall isolation
- optional `[runtime] profile = "low_footprint"` — smaller RSS (does **not** enable fair by itself)

### 2. Keys and tenants

```bash
zydecodb admin keys create --id app-a --role read_write \
  --tenant <32-hex-tenant-a> --keys-file /etc/zydecodb/keys.toml
zydecodb admin keys create --id app-b --role read_write \
  --tenant <32-hex-tenant-b> --keys-file /etc/zydecodb/keys.toml
# Optional admin key for SetContext / live drop-tenant:
zydecodb admin keys create --id ops --role admin \
  --tenant 00000000000000000000000000000000 --keys-file /etc/zydecodb/keys.toml
```

Point `security.keys_file` at that file. See [Security](#security) for
roles, prefix ACLs, and `[[tenant]]` byte/RPS caps.

### 3. Serve

```bash
zydecodb serve --config /etc/zydecodb/config.toml
```

Optional Unix socket for local control-plane traffic (auth still applies):

```toml
listen_unix = "/var/run/zydecodb/zydecodb.sock"
```

### 4. Per-tenant limits

```bash
zydecodb admin tenant set-limit --tenant <hex> --max-bytes N --rate-rps R \
  --keys-file /etc/zydecodb/keys.toml
# Reload live limits without restart:
kill -HUP "$(pidof zydecodb)"
zydecodb admin tenant list --keys-file /etc/zydecodb/keys.toml
```

### 5. Prove isolation before claiming δ

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

### 6. Offboard a tenant

```bash
# Offline (node stopped):
zydecodb admin drop-tenant --config /etc/zydecodb/config.toml --tenant <hex> [--compact]

# Live (admin API key; prefers listen_unix):
export ZYDECODB_API_KEY="zdk_..."
zydecodb admin drop-tenant --live --tenant <hex> [--compact]
```

### 7. Security baseline

- Require auth; do not expose `:9470` to the internet without TLS + keys.
- Metrics stay on loopback unless you opt into remote + bearer token.
- Full checklist: [Security](#security).

### See also

- Sharing model and δ claims: [Multi-tenant sharing model](#multi-tenant-sharing-model)
- Soak harness details: [`INTERNAL.md`](INTERNAL.md#soak-testing)
- Example TOML: [`config/zydecodb.pods.example.toml`](../config/zydecodb.pods.example.toml)

## Replication and failover

ZydecoDB replicates with **filesystem-first WAL shipping**: the primary copies
each sealed WAL segment off-box, and a read replica replays those segments to
stay caught up. The database does no network I/O itself — an operator-supplied
sidecar (rsync, s5cmd, AWS DataSync, ...) moves bytes between hosts. This keeps
the data path simple and auditable.

```
 primary ──ship──> ship_dir ──sidecar (rsync/s3/...)──> replica_from ──replay──> replica (read-only)
```

### How it works

1. **Primary** seals a WAL segment (on roll, or at clean shutdown) and writes a
   byte-identical copy into `[shipping].ship_dir`, appending one line to
   `shipped.log`:

   ```text
   <segment_id> <seal_seq> <sha256_hex> <hmac_hex>
   ```

   The HMAC field is keyed by `[shipping].hmac_key_file` (required) and
   authenticates the manifest entry end to end.

2. **Sidecar** (yours) transports `ship_dir` to the replica host's
   `[replica].from` directory. Order does not matter; the replica enforces it.

3. **Replica** (`--replica-from <dir>`) polls `from`, and for each segment in
   `shipped.log` not yet applied:
   - verifies the file's SHA-256 matches the recorded digest **and** the
     entry's HMAC under the shared key (a partial, corrupt, or forged transfer
     is refused),
   - installs it into its own WAL directory atomically,
   - reopens the engine to replay the new segment (flushing already-applied data
     to SSTables first, so each catch-up replays only the new bytes).

   The replica serves reads and **rejects every write/DDL command with
   `Forbidden`**.

### Configure the primary (ship WAL)

`config/zydecodb.example.toml`:

```toml
[shipping]
ship_dir = "/var/lib/zydecodb/ship"
mode = "hardlink"   # same filesystem; use "copy" across filesystems
# Required: authenticates every shipped.log entry (share with the replica).
hmac_key_file = "/etc/zydecodb/ship.hmac"
```

Point your sidecar at `ship_dir`. Ship the whole directory, including
`shipped.log`. Never delete a segment from `ship_dir` until the replica (and any
archive) has consumed it — the replica needs the full ordered stream.

### Configure the replica (replay WAL)

The replica is just `serve` with a replication source. Give it its **own**
`data_dir` and `wal_dir` (do not share the primary's):

```bash
zydecodb serve --config /etc/zydecodb/replica.toml \
  --replica-from /var/lib/zydecodb/replica_from \
  --replica-poll-ms 1000
```

or in the config file:

```toml
[replica]
from = "/var/lib/zydecodb/replica_from"
poll_ms = 1000
# Required: must match the primary's [shipping] hmac_key_file.
hmac_key_file = "/etc/zydecodb/ship.hmac"
```

### Liveness: the heartbeat

A primary refreshes a `shipped.heartbeat` file in `ship_dir` on a fixed cadence
(`[shipping].heartbeat_ms`, default 1000ms) **even while idle**, so a replica can
tell a *quiet* primary from a *dead* one. The heartbeat records the primary's
wall-clock time and current write sequence, and rides along in `ship_dir` like
the segments. Disable it with `heartbeat_ms = 0`.

### Check status (lag + primary liveness)

`zydecodb replica status` reads the shipped stream and the replica's persisted
position — no connection to the running server required, so it is safe to poll:

```bash
zydecodb replica status --config /etc/zydecodb/replica.toml
# primary_heartbeat: 1s ago
# primary_seq:       1284
# shipped_high_seq:  1280
# applied_seq:       1280
# seq_lag:           4
# caught_up:         true
# healthy:           true (max_stale=10s)
```

It **exits non-zero when the primary's heartbeat is older than `--max-stale-secs`**
(default 10), so an orchestrator can use it directly as a health probe:

```bash
zydecodb replica status --config replica.toml --json --max-stale-secs 5 \
  || echo "primary looks dead -- consider promotion"
```

A running replica also exports `zydecodb_replica_lag_seqs` and
`zydecodb_replica_heartbeat_age_seconds` on its `/metrics` endpoint.

### Failover / promotion (assisted)

Promotion is **assisted, not autonomous**: an external orchestrator (or you)
decides the primary is truly dead — and is responsible for *hard* fencing it
(stop the host / pull its address) — then ZydecoDB automates the node-side
mechanics and applies a cooperative epoch fence.

1. **Confirm the primary is dead and fence it.** Use `replica status` (stale
   heartbeat) plus whatever your platform provides. Make sure the old primary
   cannot keep taking writes. **This step is yours; the database cannot do it.**

2. **Stop ingest.** Stop the sidecar feeding the replica's `from` directory so no
   further segments arrive mid-promotion.

3. **Promote.** With the replica process stopped:

   ```bash
   zydecodb replica promote --config /etc/zydecodb/replica.toml
   # promoted: drained 3 segment(s), epoch 1 -> 2 (applied_seq 1280)
   # next: restart as primary without a replication source -> ...
   ```

   This drains every delivered segment into the WAL and bumps this node's
   promotion **epoch** (in `data_dir/EPOCH`) past anything seen in the stream.

4. **Restart as a primary.** Remove the `[replica].from` setting (and the
   `--replica-from` flag) and start `serve` against the *same* `data_dir` and
   `wal_dir`. The node now accepts writes. Keep `[shipping]` enabled if this new
   primary should feed a replica; on start it stamps its epoch into the stream's
   `FENCE` file.

5. **Redirect clients** to the promoted node's address.

6. **Rebuild a new replica** from the promoted primary (fresh `data_dir` /
   `wal_dir` + `--replica-from`) to restore redundancy.

#### The epoch fence (cooperative split-brain guard)

Each node carries a monotonic promotion epoch (`data_dir/EPOCH`, absent = 1).
A primary stamps its epoch into the shipped stream's `FENCE` file on start, and
`promote` bumps the epoch past the fence it observes. If an **old primary wakes
up and re-attaches to the same shipped stream**, it sees a higher `FENCE` epoch
than its own and **refuses to start** rather than create a second writer.

This is *best-effort, cooperative* fencing for the shared-stream case — it is not
hardware fencing. If two nodes can write to physically separate stores, only your
orchestrator's hard fencing prevents divergence. **Never deliberately run two
primaries against the same shipped stream.**

#### Promotion time

CI drills (`promote_under_load`, fence timing fixtures) print `promote_ms` /
`promote_timing_ms`. Order of magnitude on CI:

| Step | Typical CI |
|------|------------|
| `replica promote` drain + epoch bump | **milliseconds** (often single-digit to low tens of ms when catch-up is already done) |
| Catch-up RTO (before or after promote) | Time to apply remaining delivered segments (flush + replay under the engine lock) — grows with unshipped/unapplied WAL, not with total DB size |

**Loss window** after a hard primary death = bytes still in the primary's
**active (unsealed) segment**, plus anything the sidecar has not yet delivered.
Watch `zydecodb_wal_unshipped_bytes` on the primary; it resets when a segment
seals and ships. Acks in a sealed, shipped, HMAC-verified segment are in the
promoted replica's durable window; acks only in the open segment can be lost.

#### Notes & limits

- Replication is **asynchronous**: a replica lags the primary by at most one
  un-sealed segment plus transport time. A primary that dies before a segment
  seals can lose the writes in that open segment (the same window as any async
  log-shipping system). Use `durability = "sync"` so acknowledged writes are at
  least locally durable on the primary.
- The replica is **eventually consistent** with the primary and **read-only**.
  Promotion is deliberate; the epoch fence guards the shared-stream case but the
  death decision and hard fencing remain the operator's responsibility.
- **Catch-up path:** after installing new segments, the replica **incrementally
  applies** them into the live engine (`Engine::apply_installed_wal_segment`)
  under the engine lock (flush + replay). A full `Engine::open` reopen is only
  the fallback if incremental apply fails. Catch-up still pauses writers briefly
  while the lock is held; happy-path RTO is bounded by flush+replay of new
  segments, not a cold open of the whole WAL history.
- SSTables are **not** shipped; the replica reconstructs state purely from the
  WAL stream, so the primary must ship every segment and the replica must replay
  them all in order.
- For recovery that does **not** want to replay the full WAL history — or that
  needs a specific point in time — pair a base snapshot with the shipped WAL:
  `zydecodb admin snapshot` captures SSTables + manifest (run it offline or
  against a replica's `data_dir` for zero primary impact), and
  `zydecodb admin restore --base <snap> --wal <ship_dir> --to-seq <N>` lays the
  snapshot down and replays the shipped WAL up to a sequence (or, best-effort,
  `--to-time`). See [WAL shipping and restore](#wal-shipping-and-restore).

## WAL shipping and restore

ZydecoDB writes a byte-identical copy of every **sealed** WAL segment into a
configured directory the moment it rolls. An operator-supplied **sidecar** moves
those files off the box. The engine does no network I/O and ships nothing to an
object store itself — that is deliberately out of scope. The engine's only
promise is:

> The file in `wal_ship_dir` is exactly the sealed segment, and `shipped.log`
> records the segments in seal order with their SHA-256.

This is the cheapest credible answer to "lose the NVMe, lose the data": pair it
with a one-line `rsync`/`s5cmd`/AWS DataSync watcher and you have off-box copies
of the write-ahead log without coupling the engine to any cloud.

### Configuration

```rust
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::shipping::ShipMode;
use std::path::PathBuf;

let mut engine = Engine::open(EngineConfig {
    data_dir: "/var/lib/zydecodb/data".into(),
    wal_dir: "/var/lib/zydecodb/data/wal".into(),
    ..Default::default()
})?
.with_shipping(
    Some(PathBuf::from("/var/lib/zydecodb/ship")),
    ShipMode::Hardlink,  // or ShipMode::Copy
);
```

Pass `None` for `ship_dir` to disable shipping.

- **hardlink** (default): atomic and free — no bytes are copied, the directory
  entry just points at the same inode. Requires `wal_ship_dir` to be on the
  **same filesystem** as `wal_dir`. If it is not, the engine automatically falls
  back to a copy for that segment (cross-device link is impossible).
- **copy**: always copies the bytes. Use this when `wal_ship_dir` is a different
  mount (e.g. a separate volume the sidecar owns).

### What gets shipped, and when

- A segment is shipped the instant it **seals** — i.e. when a write rolls the
  WAL to a new segment. The sealed segment is fsynced first, so the shipped file
  is complete and durable.
- `Engine::shutdown()` (graceful SIGTERM/SIGINT) also syncs and ships the
  currently-active segment, so a clean stop leaves nothing un-shipped.
- The **active** (not-yet-sealed) segment is *not* shipped on every write. The
  bytes sitting in it are your recovery-point-objective (RPO) exposure.

### `shipped.log`

Append-only, one line per shipped segment, written into `wal_ship_dir`:

```text
<segment_id> <seal_seq> <sha256_hex> <hmac_hex>
```

- `segment_id` — the WAL segment number (matches `wal-XXXXXXXX.log`).
- `seal_seq` — the highest durable sequence number at seal time.
- `sha256_hex` — SHA-256 of the shipped file, for end-to-end integrity checks.
- `hmac_hex` — HMAC-SHA256(key, `<segment_id> <seal_seq> <sha256_hex>`), keyed
  by `[shipping] hmac_key_file`. This authenticates the manifest entry: an
  attacker who can write the ship directory cannot forge a segment *and* a
  matching log line without the key.

The server **requires** `hmac_key_file` whenever `ship_dir` is set. Generate a
key with `head -c 32 /dev/urandom > ship.hmac && chmod 600 ship.hmac` and share
it with the replica. Legacy 3-field lines (pre-HMAC) are only accepted by a
consumer with no key configured.

The sidecar should transport files in `segment_id` order and may use the hash to
verify each upload.

### The sidecar contract

You own transport. A minimal example that mirrors the ship dir to S3:

```bash
# Runs on the same box; watches the ship dir and syncs new segments.
while true; do
  s5cmd sync /var/lib/zydecodb/ship/  s3://my-bucket/zydecodb-wal/
  sleep 5
done
```

Rules the sidecar must follow:

1. **Append-only consumption.** Never delete or mutate files the engine wrote
   until they are safely transported. Deleting locally is fine *after* upload.
2. **Order by `shipped.log`.** Restore correctness depends on applying segments
   in seal order.
3. **Verify with the hash.** Compare the uploaded object's SHA-256 against the
   `shipped.log` entry.

### Recovery / restore

To restore on a fresh box after losing the local disk:

1. Stop the engine (if running).
2. Pull the shipped segments from your remote into a clean `wal_dir`, in
   `segment_id` order.
3. Start the engine. Normal WAL replay (seq-ordered, CRC-checked, torn-tail
   tolerant) reconstructs the memtable from the segments. Records already
   covered by an existing SSTable are skipped.

For a faster restore — or to roll back to a specific point in time — pair the
shipped WAL with a base snapshot instead of replaying the full WAL history:

```bash
# Capture a base snapshot (offline, or against a replica's data_dir).
zydecodb admin snapshot --config zydecodb.toml --out /backups/snap-2026-06-14

# Restore base + shipped WAL up to an exact sequence (or best-effort time).
zydecodb admin restore \
  --base /backups/snap-2026-06-14 \
  --wal  /var/lib/zydecodb/ship \
  --to-seq 12840 \
  --out  /var/lib/zydecodb/restored
```

`--to-time <unix_millis>` resolves to a sequence via the shipped time index
(`timeindex.log`, written at heartbeat granularity), so it is coarse; use
`--to-seq` for precise control. See [Replication and failover](#replication-and-failover) for
the shipped-stream layout.

The RPO is bounded by the bytes still in the active segment at the moment of
loss — exported as the Prometheus gauge:

```text
zydecodb_wal_unshipped_bytes
```

Alert on this if it grows beyond your tolerance (it grows until the next segment
seal, then drops). A graceful shutdown drives it to zero.

#### Restore time expectations

CI `restore_equivalence` / restore timing fixtures print `restore_ms`. Order of
magnitude on CI (not a SLA — measure on your hardware):

| Restore shape | Typical CI wall time | What dominates |
|---------------|----------------------|----------------|
| Empty / ~MB hardlink base (`admin restore --base` only, or tiny WAL) | **< 1 s** | Hardlink/copy of SSTables + manifest is mostly metadata |
| Base + shipped WAL | Base time + WAL path | **WAL copy + HMAC/SHA-256 verify + replay** — grows with sealed segment bytes and `--to-seq` distance, not with cold SSTable size when hardlinking |

Hardlink base restore ≈ directory metadata + inode links when snapshot and
target share a filesystem; use copy across mounts (slower, byte-proportional).
For capacity planning, time a restore of a production-sized snapshot + your
typical shipped-WAL lag on the restore host; do not extrapolate only from CI.

### What this does NOT do (scope)

- No object-store client, no async uploader, no encryption — the sidecar owns
  all of that.
- SSTables are not shipped (only the WAL). Full base backups are produced
  on demand by `admin snapshot` (hardlinked SSTables + manifest), and
  point-in-time restore is `admin snapshot` + `admin restore` over the shipped
  WAL — see [Recovery / restore](#recovery--restore).

## Upgrading

This is the operator runbook for moving a `data_dir` across ZydecoDB versions
when the on-disk format changes. It documents the format-version policy and the
exact upgrade procedure.

### On-disk formats and versioning

ZydecoDB 1.0 freezes these on-disk surfaces. The N/N−1 SSTable policy below is a
**permanent 1.x guarantee**, not a temporary migration note.

| Surface  | Version constant            | Current (1.0) | Backward read support              |
| -------- | --------------------------- | ------------- | ---------------------------------- |
| SSTable  | `sstable::FORMAT_VERSION`   | `v2`          | reads `v1` and `v2`                |
| WAL      | `wal::WAL_FORMAT_VERSION`   | `v2`          | current only (WAL is transient)    |
| Manifest | per-record-type tag         | —             | refuses unknown record types       |
| Change-log archive | reuses WAL segment format | `v2` | same as WAL; see below |
| Resume tokens | `change_log::TOKEN_VERSION` | `1` | unknown versions → `ProtocolError` |

#### Supported-range policy (1.x)

- **SSTable** readers accept the **current version and the immediately prior
  version** (`N` and `N-1`). New tables are always written at the current
  version `N`. Older tables are rewritten forward by background compaction (or
  on demand via `admin upgrade`). Before a future `v3` bump, the migration
  window guarantees every reachable `v1` file has been rewritten to `v2`, so a
  reader only ever spans one version gap. This N/N−1 window remains the
  contract for the entire 1.x line.
- **WAL** segments are validated against the current version only. The WAL is a
  transient recovery log, not long-term storage: a clean shutdown (or
  `admin upgrade`, which flushes) drains it into SSTables, so there is nothing
  to migrate. Never copy a WAL across a format boundary — drain it first.
- **Manifest** records carry a type tag. An older binary that meets a
  record type written by a newer binary refuses to open loudly
  (`UnsupportedFormat`) rather than silently truncating catalog state. This is a
  forward-compatibility *guard*, not a migration path: do not downgrade.
- **Change-log archive** (when `[change_streams]` is enabled) stores sealed WAL
  segments under `archive_dir` plus `manifest.json`. Archive segments use the
  same WAL format version. Resume tokens with `TOKEN_VERSION = 1` must remain
  decodable across 1.0.x → 1.0.y upgrades while the token's sequence is still
  within retention. See [`PROTOCOL.md`](PROTOCOL.md#internal-formats-10).

#### Integrity (v2 SSTables)

`v2` adds a per-block CRC32 trailer to every data, index, and bloom block,
verified on read. Silent bit-rot at rest surfaces as an `Io` error
(`sstable: ... block checksum mismatch`) instead of being served as a correct
value or panicking on decode. `v1` files have no trailers and are read without
verification — another reason to migrate them forward.

### Upgrade procedure

ZydecoDB upgrades are **in-place and backward-read-compatible**: a newer binary
opens an older `data_dir` directly. The steps below are the safe sequence.

1. **Back up first.** Capture a base snapshot (offline or against a replica):

   ```bash
   zydecodb admin snapshot --config /etc/zydecodb/config.toml --out /backups/$(date +%F)
   ```

   Keep shipped WAL alongside it if you rely on point-in-time restore (see
   [WAL shipping and restore](#wal-shipping-and-restore) / [Replication and failover](#replication-and-failover)).

2. **Stop the old server.** A graceful stop writes the clean-shutdown marker and
   flushes the WAL into SSTables.

3. **Swap the binary** and start the new version against the same `data_dir`.
   At startup the engine logs the on-disk SSTable format mix. If any
   legacy-format files remain you will see:

   ```
   WARN on-disk SSTables include legacy-format files (readable; run `admin upgrade` to rewrite)
   ```

   The server is fully operational in this state — legacy files are read
   transparently.

4. **(Optional) Rewrite legacy files now.** To force the migration instead of
   waiting for background compaction to reach every file, stop the server and
   run:

   ```bash
   zydecodb admin upgrade --config /etc/zydecodb/config.toml
   ```

   This forces a full compaction (offline, takes the `data_dir` lock) and then
   reports how many SSTables are at the current format vs. still legacy:

   ```
   upgrade complete: 42 SSTable(s) at current format v2, 0 legacy
   ```

   A small number of settled, non-overlapping files may not be picked by the
   compaction planner and are reported as legacy; they remain readable and are
   rewritten organically as future writes touch their key ranges.

### Downgrade

Downgrading is **not supported**. A newer binary may write a newer SSTable
format (and newer manifest record types) that the older binary refuses to read.
If you must roll back, restore from the snapshot taken in step 1 with the old
binary.

### Quick reference

```bash
# Inspect format mix without changing anything: just start the server and read
# the startup log line (INFO/WARN about SSTable format).

# Force-rewrite legacy SSTables forward (offline):
zydecodb admin upgrade --config /etc/zydecodb/config.toml

# Take a backup before any upgrade:
zydecodb admin snapshot --config /etc/zydecodb/config.toml --out /backups/pre-upgrade
```

## Alerting / paging

Scrape `[metrics]` (`/metrics`). Thresholds below are starting points — tune to
your RPO/RTO and disk. Metric names are part of the 1.0 contract (renames are
breaking).

| Metric | Suggested threshold | Meaning | Action |
|--------|---------------------|---------|--------|
| `zydecodb_wal_unshipped_bytes` | > 1 segment size or > your RPO bytes for > 2 seal intervals | Active WAL not yet sealed/shipped; primary-death loss window | Check seal/roll, disk on `ship_dir`, sidecar lag; graceful stop drives to 0 |
| `zydecodb_replica_lag_seqs` | > 0 for > N minutes (N = your catch-up SLO) | Replica behind primary write seq | Check sidecar, `replica.from` disk, replica logs (HMAC/retention gap halts loudly) |
| `zydecodb_replica_heartbeat_age_seconds` | > `--max-stale-secs` (default 10) / probe budget | Primary heartbeat stale (dead vs idle) | Confirm primary; if dead, hard-fence then [promote](#failover--promotion-assisted) |
| `zydecodb_compaction_queue_depth` / `zydecodb_pending_compaction_bytes` | Depth > 0 sustained **and** pending bytes growing | Compaction falling behind; risk of L0 stall / `EngineBusy` | [Diagnose stuck compaction](#diagnose-stuck-compaction) |
| `zydecodb_change_stream_disconnects_total` | Spike vs baseline (by `reason`) | Consumers dropping | Check client backpressure, `write_timeout_ms`, retention gaps → [Fell-behind change stream](#fell-behind-change-stream) |
| Change-stream lag (archive window) | Consumer resume seq < `zydecodb_change_log_earliest_seq` | Consumer behind retention → `Conflict` on resume | Bump retention or re-snapshot consumer |
| `zydecodb_errors_total{code="Unauthorized"}` | Sustained rate > expected (bad clients / key rotation) | Auth failures | Verify keys file + SIGHUP reload; revoke leaked keys; check burst (`EngineBusy`) |
| `zydecodb_wal_fsync_duration_seconds` | p99 ≫ normal disk (e.g. > 100 ms on NVMe) | Durability path slow; write latency and group-commit stall | Check disk health, IO saturation, `durability` mode |

## Day-2 runbooks

Operator index (details live in the linked sections):

| Runbook | Where |
|---------|--------|
| Backup / restore / PITR | [Recovery / restore](#recovery--restore), [Restore time expectations](#restore-time-expectations) |
| Promote / fence | [Failover / promotion](#failover--promotion-assisted), [Promotion time](#promotion-time) |
| Rotate API keys | [Rotate API keys](#rotate-api-keys) (below) |
| Upgrade | [Upgrading](#upgrading) |
| Stuck compaction | [Diagnose stuck compaction](#diagnose-stuck-compaction) (below) |
| Fell-behind change stream | [Fell-behind change stream](#fell-behind-change-stream) (below) |
| Alerting | [Alerting / paging](#alerting--paging) |

### Rotate API keys

1. Create the replacement key (prints the secret **once**):

   ```bash
   zydecodb admin keys create \
     --id backend-v2 --role read_write \
     --keys-file /etc/zydecodb/keys.toml
   ```

2. Deploy the new secret to clients (`ZYDECODB_API_KEY` / driver config). Keep
   the old key valid until cutover completes.

3. Reload the running server without downtime:

   ```bash
   kill -HUP "$(pidof zydecodb)"   # or your unit's reload → SIGHUP
   ```

   SIGHUP reloads `keys.toml` (new keys usable immediately; revoked keys drop
   sessions on the next request). Tenant limit changes apply the same way.

4. **Validate** the new key before revoking the old one: open a fresh client
   session with the new secret (`SessionInit` / driver connect) and run a
   cheap authenticated read (e.g. `Ping` with auth required, or `Get`/`Find`).
   Confirm `admin keys list` shows both ids.

5. Revoke the old key and SIGHUP again:

   ```bash
   zydecodb admin keys revoke --id backend --keys-file /etc/zydecodb/keys.toml
   kill -HUP "$(pidof zydecodb)"
   ```

6. Confirm revoke: a client still holding the old secret gets `Unauthorized`
   (`session revoked` / failed `SessionInit`); audit log shows no successes for
   the old `key_id`; `zydecodb_errors_total{code="Unauthorized"}` is not elevated
   from stragglers past the cutover window.

Keep `keys.toml` at `chmod 600`. Serve refuses group/world-readable key and HMAC
files.

#### Rotate shipping HMAC

Shipping HMAC is **not** hot-reloaded (unlike API keys). Order:

1. Generate a new key file (`head -c 32 /dev/urandom > ship.hmac.new && chmod 600`).
2. Copy the **same** bytes to every replica host that reads the ship stream.
3. Update `hmac_key_file` on primary + all replicas to the new path (or atomically
   replace the file in place on all hosts).
4. **Rolling restart:** restart replicas first (they must accept the new HMAC),
   then the primary (so new `shipped.log` lines use the new key).
5. Validate: replica continues ingesting (`zydecodb_replica_lag_seqs` not stuck;
   no HMAC verify errors in logs). Remove the old key file only after that.

Authz matrix (opcode × role × tenant): [`INTERNAL.md`](INTERNAL.md#authorization-audit-matrix).

### Diagnose stuck compaction

1. **Metrics:** `zydecodb_compaction_queue_depth`, `zydecodb_compaction_worker_busy`,
   `zydecodb_pending_compaction_bytes`, `zydecodb_live_sstables_by_level{level="0"}`,
   `zydecodb_immutable_memtable_count`. Growing pending bytes + L0 file count with
   worker busy stuck at 0 ⇒ worker not running or planner not scheduling.
2. **Writes:** clients seeing `EngineBusy` / stalls often means L0 write-stall
   threshold hit — compaction must drain L0 before write admission recovers.
3. **Offline drain:** stop serve, then force work:

   ```bash
   zydecodb admin upgrade --config /etc/zydecodb/config.toml
   ```

   That flushes memtables and runs compaction to completion (takes the data_dir
   lock). Use when online compaction cannot catch up.
4. **Config:** check `poll_compaction_ms` (maintenance thread cadence), disk
   full on `data_dir`, and under `[fair]` whether one tenant's L0 debt (Fork B)
   is stalling writes while global compaction looks idle.

### Fell-behind change stream

A resume token older than the retained archive returns **`Conflict`** — the
server will not skip the gap.

1. Confirm retention: `[change_streams] retention_secs` / `retention_bytes`, and
   gauges `zydecodb_change_log_earliest_seq` / `zydecodb_change_log_latest_seq` /
   `zydecodb_change_log_archive_bytes`.
2. If consumers routinely lag: **raise archive retention** (time and/or bytes),
   restart or SIGHUP is not enough for retention knobs — update config and
   restart serve so the archive keeps more sealed segments.
3. **Re-snapshot the consumer:** treat the gap as fatal for that token. Take a
   fresh consistent read of the collection (or app-level snapshot), then open
   `Watch` **without** the stale resume token (or with a token at/after
   `earliest_seq`). Do not invent sequences.
4. Disconnect spikes: `zydecodb_change_stream_disconnects_total` by `reason`
   (`write_timeout`, revoke, shutdown, …) — fix slow consumers or raise
   `write_timeout_ms` only if the consumer is healthy but briefly slow.

## Performance

Reference numbers from `scripts/ref-workloads.sh` (release binary). **Hardware
caption (maintainer workstation, 2026-07-27):** AMD Ryzen 5 3600 (6C/12T),
62 GiB RAM, NVMe SSD, Linux 6.8 x86_64. Seed `42`, `OPS=2000` (heavy workloads
use `OPS/4`). Re-run before marketing a new release; do not treat GitHub Actions
runners as this class of hardware.

| Workload | ops/s | p50 (µs) | p99 (µs) | RSS peak (MiB) |
|----------|------:|---------:|---------:|---------------:|
| `point_get` | 15400 | 27 | 146 | 7.3 |
| `find_indexed` | 6900 | 107 | 271 | 10.0 |
| `find_unindexed` | 3140 | 276 | 544 | 10.4 |
| `upsert` | 860 | 188 | 7680 | 15.5 |
| `tx_commit` | 230 | 4200 | 7690 | 16.6 |
| `aggregate` | 2230 | 429 | 861 | 16.6 |
| `watch_fanout` (4 subs) | 65 | 15100 | 21900 | 55.6 |

Raw JSON: [`soak-baselines/ref-workloads-local.json`](soak-baselines/ref-workloads-local.json).
Harness definitions: [`INTERNAL.md`](INTERNAL.md#reference-workloads-published-numbers).

### Engine 90-minute soak (stability headline)

Archived paced run `memo6-90m` (`HOURS=1.5`, `OPS=3000`, block cache 640 MB):
mean ~2666 ops/s, steady p99 ~158 µs (max sample p99 382 µs), RSS max ~971 MiB,
compaction write amp ~1.9. Stability analyzer exit 0. Repro and gates:
[`INTERNAL.md`](INTERNAL.md#soak-testing).

### Release checklist (pre-tag)

Do **not** tag an RC until these are green (local equivalent OK). Tag publish
(`release.yml`) does **not** re-run the 90m soak — gates are pre-tag only.

- [ ] `HOURS=1.5 OPS=3000 ./scripts/soak.sh` then
      `python3 scripts/analyze-soak.py --mode stability <OUT_DIR>/metrics.jsonl`
      exits 0 — or green [`.github/workflows/release-soak.yml`](../.github/workflows/release-soak.yml)
- [ ] `MODE=both ./scripts/tenant-isolation-soak.sh` exits 0 (or green
      `release-soak.yml` / nightly on the RC commit)
- [ ] (RC) 24h **uncapped** VPS soak archived under
      `docs/soak-baselines/rc-<version>-24h-uncapped.jsonl` (+ `notes.md` with HW).
      Use `scripts/vps-soak.sh` (`OPS=0`). `scripts/run-engine-ga-24h.sh` is a
      **paced** (`OPS=3000`) confirmation, not the uncapped bar.
- [ ] Reference workloads refreshed if you claim new GUIDE numbers:
      `OPS=2000 scripts/ref-workloads.sh`

Nightly regression (not a tag gate): `.github/workflows/bench-nightly.yml`
fails if engine microbench `p99_us` or `rss_bytes` exceeds baseline by more than
20% (with absolute floors for GHA noise). See [`INTERNAL.md`](INTERNAL.md#bench-regression-nightly).

## Resource limits defaults

| Knob | Default | Notes |
|------|---------|--------|
| Memtable flush threshold | **64 MiB** | Engine constant; fair mode memtable budget derives from this unless overridden |
| `block_cache_mb` | **256** | 32 under `[runtime] profile = "low_footprint"` if still at default |
| `max_open_readers` | **128** | 32 under low_footprint |
| `max_connections` | **256** | Global; Docker example uses 128 |
| `rate_limit_rps` | **1000** | Per connection; Docker example uses 200 |
| `auth_burst_limit` | **10** | Failed `SessionInit` per IP per minute |
| `max_sort_buffer` | **10000** | Docs buffered per sort / multi-write select |
| `[aggregation] max_scan_docs` | **100000** | |
| `[aggregation] max_groups` | **10000** | |
| `[aggregation] max_memory_bytes` | **16 MiB** | |
| `[aggregation] max_result_bytes` | **4 MiB** | |
| `[change_streams] max_subscriptions` | **128** | Process-wide; feature off until `enabled = true` |
| `[change_streams] max_subscriptions_per_tenant` | **8** | |
| `max_bytes_per_tenant` | **0** (unlimited) | Set non-zero in production when auth is on; see example.toml recommendations |

`require_auth = "auto"` (default) requires auth off-loopback. Startup also refuses
world-readable `keys_file` / HMAC key files, overlapping `data_dir`/`wal_dir` with
`shipping.ship_dir` or `replica.from`, and shipping/replica without `hmac_key_file`.
