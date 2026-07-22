# ZydecoDB Security

ZydecoDB secures the **wire and key namespace**. Your application secures **humans** (passwords, sessions, business rules).

Reference implementation: [`examples/user_backend/`](../examples/user_backend/) — Flask handles users; ZydecoDB holds bytes behind an API key.

## Threat model

| Layer | Defends |
|-------|---------|
| **Your HTTP API** | User login, passwords, OAuth, "who can edit what" |
| **Network** | Firewall, private VPC, do not expose `:9470` publicly |
| **ZydecoDB** | API keys, tenant isolation, TLS, rate limits, quotas, audit logs |

## Deployment modes

### Localhost dev (default)

```toml
listen = "127.0.0.1:9470"

[security]
require_auth = false
```

Use [`config/zydecodb.dev.toml`](../config/zydecodb.dev.toml) for writable `/tmp` paths. Auth is optional on loopback.

### App behind API (recommended)

```text
Internet → Your Flask/Go API → ZydecoDB (127.0.0.1 or private IP)
```

- Run ZydecoDB on loopback or a private subnet.
- Your API holds `ZYDECODB_API_KEY`.
- End users never touch the database port.

### LAN / private network

```toml
listen = "0.0.0.0:9470"

[security]
require_auth = true
keys_file = "/etc/zydecodb/keys.toml"
```

With `require_auth = "auto"` (default), auth is required whenever `listen` is not loopback.

## Admin CLI — API keys

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

Dev-only bootstrap: set `ZYDECODB_BOOTSTRAP_KEY` instead of a keys file. The server **refuses to start** if this env var is set and `listen` is not loopback. Use a real `keys.toml` for any networked bind.

Fail-closed startup guards:

- **Auth required + empty keys file + no bootstrap** → the server refuses to start (a server that can never authenticate anyone is a misconfiguration, not a service).
- **`legacy_single_tenant = true` + any key with a non-zero tenant** → refused; the two key layouts must not be mixed. Set `legacy_single_tenant = false` for multi-tenant deployments.

Key verification is O(1): `admin keys create` stores a `secret_lookup` (sha256 of the secret) used as an index into the keystore, so auth performs exactly one argon2 verify regardless of how many keys exist. Keys minted before this field existed still verify via a linear scan — reissue them to get the fast path (the server logs a warning at startup when such keys are present).

## Connection handshake

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

## Key file format

See [`config/zydecodb.keys.example.toml`](../config/zydecodb.keys.example.toml). Only **argon2id hashes** are stored — never plaintext secrets.

| Field | Meaning |
|-------|---------|
| `id` | Label in audit logs |
| `secret_hash` | argon2id hash of the full `zdk_...` secret |
| `secret_lookup` | sha256 of the secret; O(1) keystore index (written by `admin keys create`) |
| `role` | `read_only`, `read_write`, or `admin` |
| `tenant` | 32 hex chars → 16-byte namespace |
| `allowed_prefixes` | Optional; empty = entire tenant. Applies to KV key prefixes and document collection names |

## Tenant isolation

Stored engine keys use layout `0x01 | tenant(16) | your_key`. Each API key is scoped to one tenant.

- `legacy_single_tenant = true` (default): when tenant is all zeros, uses old layout `0x01 | your_key` for backward compatibility.
- `legacy_single_tenant = false`: always prefix with tenant (multi-tenant hosted).

Upgrade note: keep `legacy_single_tenant = true` until your existing data is migrated — flipping it orphans keys written under the old layout. The server refuses to start when `legacy_single_tenant = true` is combined with any non-zero-tenant key, so a legacy volume can never be half-migrated by accident. Greenfield deployments (including the Docker config) should use `false`.

Admins can switch tenant mid-connection with `SetContext` (`0x41`) and a 16-byte tenant payload.

## TLS

```toml
[tls]
enabled = true
cert = "/etc/zydecodb/tls.crt"
key  = "/etc/zydecodb/tls.key"
```

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

### Unix-domain socket (local transport)

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

### Metrics endpoint

The `[metrics]` HTTP endpoint binds loopback by default. A non-loopback bind is
**refused** unless `allow_remote = true`, and remote binds require a bearer
`token`; `/metrics` then demands `Authorization: Bearer <token>` (constant-time
compared) while `/healthz` and `/readyz` stay open for probes.

### WAL shipping integrity (HMAC)

When `[shipping] ship_dir` is set, `hmac_key_file` is **required**: each
`shipped.log` entry carries an HMAC-SHA256 over `<id> <seq> <sha256>` so an
attacker with write access to the ship path cannot forge a segment plus a
matching manifest line. A replica (`[replica] from`) requires the same key and
refuses entries without a valid HMAC. See [SHIPPING.md](SHIPPING.md).

## Rate limits and quotas

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

### Multi-tenant sharing model

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

**Enable under pods:** set `[fair] enabled = true` in the server TOML (see `config/zydecodb.example.toml`), typically with `legacy_single_tenant = false`. Off by default until you prove the soak on your box.

**Prove it (simulated pods — no fleet required):**

```bash
./scripts/tenant-isolation-soak.sh                 # steady (≤50ms) + ramp-up reclaim (≤350ms)
MODE=steady ./scripts/tenant-isolation-soak.sh     # ship bar only
MODE=rampup ./scripts/tenant-isolation-soak.sh     # FairDB idle→reclaim hard case
```

Harness: `tenant-isolation-soak` — steady V solo / V\|N fair=off / V\|N fair=on, plus ramp-up (N floods while V idle, then V reclaim burst ≈ fair-share bytes). Steady δ ≤ 50 ms and ramp-up δ ≤ 350 ms are **separate** claims. Re-run on your hardware before claiming numbers. Driver notes: [`SOAK.md`](SOAK.md).

### Per-tenant limits

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

## Audit logging

```toml
[security.audit]
enabled = true
log_client_key = false   # never enable in production without good reason
```

Emits structured `tracing` events: `tenant`, `key_id`, `cmd`, `client_key_len`, `status`, `duration_us`. Secrets and values are never logged.

With `log_client_key = true`, each line also carries `client_key_prefix` — a hex dump of at most the **first 8 bytes** of the client's KV key. The full key is never logged even when enabled; leave it off unless you are actively debugging access patterns.

## Wire status codes (security-related)

| Byte | Name | When |
|------|------|------|
| `0x0B` | Unauthorized | Missing/invalid API key, or command before auth |
| `0x0C` | Forbidden | Valid key but read-only or prefix ACL denied |
| `0x07` | EngineBusy | Rate limit or auth burst limit |
| `0x09` | PolicyRejected | Tenant byte quota exceeded |

## What ZydecoDB does not do

- End-user authentication (use your API — see [`examples/user_backend/`](../examples/user_backend/))
- SQL injection protection (no SQL)
- Encryption at rest (use disk/filesystem encryption)

## Docker

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

## Operations checklist (networked deployments)

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

## Never do this

- Bind `0.0.0.0:9470` without `require_auth` on the public internet
- Ship Docker with `require_auth = false`
- Set `ZYDECODB_BOOTSTRAP_KEY` on a non-loopback listen address
- Commit API keys, `keys.toml`, or TLS PEMs to git
- Log full keys or values in audit mode
