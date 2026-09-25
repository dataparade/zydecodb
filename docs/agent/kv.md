zydecodb-agent-docs: 1
zydecodb: 1.4.1
topic: kv
next: python, tx, query

# Raw key-value

Use the document `Collection` API for records you query. Use raw KV when you
need a byte key, a TTL, or a session token that should vanish. The
`examples/user_backend` app stores users as documents and sessions as KV.

Keys and values are opaque bytes. `expires_at` / `expiresAt` is absolute unix
milliseconds; `0` means never.

```python
db.put(b"session:abc", b"user-id", expires_at=deadline_ms)
val = db.get(b"session:abc")          # None if missing or expired
db.delete(b"session:abc")             # True if a value was removed
```

```go
db.Put(ctx, []byte("session:abc"), []byte("user-id"), deadlineMs)
val, err := db.Get(ctx, []byte("session:abc")) // (nil, nil) if missing
ok, err := db.Delete(ctx, []byte("session:abc"))
```

```ts
await db.put(Buffer.from("session:abc"), Buffer.from("user-id"), deadlineMs);
const val = await db.get(Buffer.from("session:abc")); // null if missing
await db.delete(Buffer.from("session:abc"));
```

Do not invent `write_batch`, range scans, or snapshots in application code.
Those are engine/admin surfaces, not the official driver product API.

Prefix ACLs apply to raw keys and to collection names. A key
`events:click` needs an `events:` prefix on the API key.
