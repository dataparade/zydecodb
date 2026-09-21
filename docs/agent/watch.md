zydecodb-agent-docs: 1
zydecodb: 1.3.1
topic: watch
next: python, pitfalls

# Change streams

`watch` is collection-scoped and **primary-only**. Replicas reject it. This is
not WAL shipping / replication.

The driver opens a **dedicated** connection (not a pooled request). Idle
timeout must exceed the server heartbeat (`change_streams.heartbeat_ms`,
default 15s). Driver defaults: Python `watch_idle_timeout=45`, Go
`WithWatchIdleTimeout` 45s, TypeScript `watchIdleTimeoutMs=45000`.

## Resume tokens

- **In:** raw bytes (`resume_token` / `[]byte` / `Buffer`).
- **Out:** events expose a **base64** string (`resume_token` / `ResumeToken` /
  `resumeToken`). Decode before passing back into `watch`.

Tokens are opaque. Do not parse them.

## Drivers

```python
with users.watch() as stream:
    for event in stream:
        token = event.resume_token  # base64 str; decode to resume
```

```go
stream, err := users.Watch(ctx, nil)
ev, err := stream.Next()
// ev.ResumeToken is base64; decode to []byte for Watch(ctx, token)
```

```ts
for await (const ev of users.watch()) {
  const token = ev.resumeToken; // base64; Buffer.from(token, "base64") to resume
}
```

Prefix ACL applies to the collection name. Read-only key revocation (SIGHUP)
kills active streams. Caps live under `[change_streams]` — do not assume
unbounded subscribers.
