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

Optional prefix ACL per key:

```toml
allowed_prefixes = ["events:", "metrics:"]
```

Dev-only bootstrap (logs a warning): set `ZYDECODB_BOOTSTRAP_KEY` env var instead of a keys file.

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
| `role` | `read_only`, `read_write`, or `admin` |
| `tenant` | 32 hex chars → 16-byte namespace |
| `allowed_prefixes` | Optional; empty = entire tenant |

## Tenant isolation

Stored engine keys use layout `0x01 | tenant(16) | your_key`. Each API key is scoped to one tenant.

- `legacy_single_tenant = true` (default): when tenant is all zeros, uses old layout `0x01 | your_key` for backward compatibility.
- `legacy_single_tenant = false`: always prefix with tenant (multi-tenant hosted).

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

Alternative: terminate TLS at nginx or stunnel; keep plain TCP to ZydecoDB on `127.0.0.1`.

### Unix-domain socket (local transport)

For local control-plane or co-located traffic, listen on a Unix-domain socket in
addition to TCP:

```toml
listen_unix = "/run/zydecodb/zydecodb.sock"
```

TLS is **TCP-only** — the UDS trust boundary is the socket file's filesystem
permissions, so restrict the directory to the intended local users. API-key auth
still applies on the socket exactly as it does over TCP.

## Rate limits and quotas

```toml
[security]
max_connections = 256        # drop new TCP connections when full
rate_limit_rps = 1000        # per-connection token bucket
auth_burst_limit = 10        # failed SessionInit per IP per minute

[security.quotas]
max_bytes_per_tenant = 0     # 0 = unlimited; else write cap per tenant
```

Exceeded rate → `EngineBusy` (`0x07`). Exceeded quota → `PolicyRejected` (`0x09`).

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

## Never do this

- Bind `0.0.0.0:9470` without `require_auth` on the public internet
- Commit API keys or plaintext secrets to git
- Log full keys or values in audit mode
