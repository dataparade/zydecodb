# ZydecoDB examples

Runnable references for building on top of ZydecoDB. Start here before writing your own client.

**Client codecs:** treat the Python client as the reference implementation; Go and
TypeScript track the same wire via [`clients/conformance/`](../clients/conformance/).
Prefer one full `user_backend` language when learning — ports exist for the others.

## What to run first

| Example | What it shows |
|---------|----------------|
| [`user_backend/`](user_backend/) | Full HTTP API — users as documents, login + TTL sessions |

Security model: **ZydecoDB handles connection auth; your app handles human login.** See [`docs/SECURITY.md`](../docs/SECURITY.md).

---

## 1. Start the database

```bash
# Prebuilt binary (or build from source: cargo build --release -p zydecodb)
curl -sSL https://zydecodb.com/install.sh | sh

# No config needed locally: 127.0.0.1:9470, data in ~/.zydecodb
zydecodb serve
```

Default: listens on `127.0.0.1:9470`, auth optional on loopback. For custom
paths or production settings, pass `--config` (start from
[`config/zydecodb.dev.toml`](../config/zydecodb.dev.toml)).

### With API keys (LAN or `require_auth = true`)

```bash
zydecodb admin keys create \
  --id backend \
  --role read_write \
  --keys-file /tmp/zydecodb-keys.toml

export ZYDECODB_API_KEY="zdk_..."   # printed once by the command above
```

Point `keys_file` in your server config at `/tmp/zydecodb-keys.toml`.

---

## 2. Python TCP client

```bash
pip install zydecodb
```

(Source lives in [`clients/python`](../clients/python).)

The `Collection` API:

```python
users = db.collection("users")
users.create_index(["age"])                                       # optional; speeds up age queries

users.insert_one({"name": "Ada", "age": 30, "city": "London"})    # returns the auto _id
users.find({"age": {"$gte": 30}}, sort=[("age", True)], limit=10) # operators + sort, auto-paginated
users.find({"city": "London"}, projection={"name": 1})            # works with no index on city (scan)
users.update_one({"name": "Ada"}, {"$inc": {"age": 1}})           # $set / $inc / $unset / $push
users.count_documents({"age": {"$gte": 30}})
users.distinct("city")
users.delete_many({"age": {"$lt": 18}})
```

The planner uses an index (or `_id` lookup) when one fits and falls back to a collection scan otherwise, so any field is queryable. The client sends `SessionInit` automatically when `--api-key` or `ZYDECODB_API_KEY` is set.

---

## 3. User-management HTTP API

Flask app that stores users as documents (ZydecoDB-maintained indexes for email lookup and listing) with login tokens on the raw KV path for TTL.

```bash
pip install -r examples/user_backend/requirements.txt

# optional if server requires auth:
export ZYDECODB_API_KEY="zdk_..."

python3 examples/user_backend/app.py --seed
```

API: `http://127.0.0.1:8080`

```bash
# Sign up
curl -s -X POST http://127.0.0.1:8080/api/users \
  -H 'Content-Type: application/json' \
  -d '{"email":"margaret.chen@example.com","name":"Margaret Chen","password":"jazzbrunch"}'

# Log in
curl -s -X POST http://127.0.0.1:8080/api/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"margaret.chen@example.com","password":"jazzbrunch"}'

# Current user (use token from login response)
curl -s http://127.0.0.1:8080/api/me -H 'Authorization: Bearer TOKEN'
```

**Data model** (defined in [`user_backend/store.py`](user_backend/store.py)):

```text
collection "users", _id = <uuid>       → user record (JSON), via the Collection API
  index on ["email"]                   → email lookup + login
  index on ["created_at"]              → list users in signup order
session:<token>                        → logged-in user (24h TTL, raw KV)
```

Sessions stay on the raw key-value path because they need a TTL, which the document write path does not expose. End users talk to Flask. Flask talks to ZydecoDB. Never expose `:9470` to the public internet.

---

## Recommended production layout

```text
Internet  →  your-api.example.com (HTTPS)  →  ZydecoDB on 127.0.0.1:9470
                    ↑                              ↑
              user passwords                  ZYDECODB_API_KEY
              session tokens                  (one key per service)
```

---

## Official clients in other languages

Prefer a maintained driver over hand-rolling one. Each ships its own quickstart
and a small HTTP backend example:

| Language | Driver | Examples |
|----------|--------|----------|
| Python | [`clients/python`](../clients/python) | [`zydecodb_client.py`](zydecodb_client.py), [`user_backend/`](user_backend/) |
| Go | [`clients/go`](../clients/go) | [`clients/go/examples`](../clients/go/examples) |
| TypeScript / Node | [`clients/typescript`](../clients/typescript) | [`clients/typescript/examples`](../clients/typescript/examples) |

All three implement the same wire protocol and are verified byte-for-byte
against shared [conformance vectors](../clients/conformance) generated from the
Rust server's own encoders — so the drivers can never silently drift from the
server or from each other.

## Building your own client

Wire format: length-prefixed frames — see `zydecodb-engine::frame` or copy [`zydecodb_client.py`](zydecodb_client.py). If you implement a new client, run its codec against [`clients/conformance/vectors.json`](../clients/conformance) to guarantee byte-level compatibility.

Handshake when auth is enabled:

1. `SessionInit` (`0x40`) — payload is the full API key string (UTF-8)
2. `PUT` / `GET` / `DEL` / `PING` / `STATS` as usual

Document commands (payload codecs in [`zydecodb_client.py`](zydecodb_client.py)):

- `IndexDef` (`0x30`) — define a collection + index
- `DocPut` (`0x21`) / `DocDel` (`0x22`) — write / delete a document by id
- `Query` (`0x20`) — first payload byte selects the mode: `0x00` get-by-id, `0x01` index range (bounds, cursor, limit)
- `Find` (`0x23`) — filter + sort + projection + skip/limit + cursor; returns a page of documents
- `Update` (`0x24`) — filter + update doc (`$`-operators) + multi flag; returns `{matched, modified}`
- `Delete` (`0x25`) — filter + multi flag; returns `{deleted}`
- `Count` (`0x26`) — first byte selects `count` (returns a number) or `distinct` (returns a JSON array)

All fields inside these payloads are length-prefixed (`u32` big-endian). Status bytes include `Unauthorized` (`0x0B`) and `Forbidden` (`0x0C`).
