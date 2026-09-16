zydecodb-agent-docs: 1
zydecodb: 1.2.0
topic: ops
next: index, pitfalls

# Run a server (app-in-front)

## Local

```bash
curl -sSL https://zydeco.dev/install.sh | sh
zydecodb serve
# 127.0.0.1:9470 — auth optional on loopback — state in ~/.zydecodb/
```

Custom paths / production: `zydecodb serve --config /path/to.toml`. Start from
`config/zydecodb.dev.toml` or `config/zydecodb.example.toml`.

`zydecodb version` prints the binary version. `zydecodb update` upgrades the
server binary only (not drivers or data).

## API keys

Required whenever `listen` is not loopback (`require_auth = "auto"`).

```bash
zydecodb admin keys create \
  --id backend --role read_write \
  --keys-file /tmp/zydecodb-keys.toml
# prints zdk_... once — export ZYDECODB_API_KEY=...
zydecodb admin keys list --keys-file /tmp/zydecodb-keys.toml
zydecodb admin keys revoke --id backend --keys-file /tmp/zydecodb-keys.toml
```

Roles: `read_only`, `read_write`, `admin`. Optional `--prefix` ACL applies to
KV keys and collection names. Point `keys_file` in the server config at that
TOML. `chmod 600` the file.

## Docker

Auth is required in the Docker config. Create a key first:

```bash
zydecodb admin keys create \
  --id docker --role admin --keys-file config/keys.toml
docker compose up -d --build
```

Compose publishes `:9470` only. Do not publish it to the public internet.

## Layout

```text
Internet → your HTTPS API → ZydecoDB on 127.0.0.1:9470
              passwords / sessions          ZYDECODB_API_KEY
```

This page stops here. Pods, replication, WAL shipping, and PITR are operator
docs in `docs/GUIDE.md`, not this agent surface.
