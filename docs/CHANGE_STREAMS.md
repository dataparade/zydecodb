# Change Streams

ZydecoDB change streams (`Watch = 0x2C`) deliver **collection-scoped**, **primary-only** document change notifications with **durable resume tokens** backed by a retained WAL archive.

This is **not** raw WAL replication. Replication/shipping uses the operator-owned ship directory; change streams use a separate local archive under `[change_streams].archive_dir` (default `<data_dir>/change_log`).

## Guarantees

- **Events:** `upsert` (full post-write document JSON) or `delete` (document ID only). WAL cannot truthfully distinguish insert/replace/update.
- **Ordering:** strict `(engine_seq, WAL_op_ordinal)` for fsynced events only.
- **Delivery:** at-least-once across reconnects. Resume is **exclusive** after the last processed token.
- **Non-events:** TTL expiry, compaction, index keys, raw KV, and system metadata do not emit events.
- **Primary-only:** replicas and `replica.from` configurations reject Watch.

## Configuration (`[change_streams]`)

Disabled by default.

| Setting | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Master switch |
| `archive_dir` | `<data_dir>/change_log` | Retained sealed WAL segment archive |
| `retention_secs` | `3600` | Time-based prune of oldest sealed archives |
| `retention_bytes` | `1GiB` | Size-based prune |
| `heartbeat_ms` | `15000` | Idle heartbeat cadence |
| `write_timeout_ms` | `5000` | Slow-consumer socket write deadline |
| `max_subscriptions` | `128` | Global concurrent Watch connections |
| `max_subscriptions_per_tenant` | `8` | Per-tenant cap |

When enabled, a covered WAL segment is unlinked only after archive confirmation. Archive failures retain the source WAL and retry rather than creating a silent history gap.

## Protocol

1. Client opens a **dedicated** connection (not a pooled one-request connection).
2. After auth + collection ACL, client sends `Watch` with `[collection][resume_token]`.
3. Server ACKs, then streams framed Ok payloads: `ACK`, `EVENT`, `HEARTBEAT`.
4. Client cancellation closes the socket. Server shutdown, credential revocation, collection removal, write timeout, retention gap, and peer close all terminate the stream and release capacity.

Resume tokens are opaque, versioned, CRC-checked, and bound to database ID, tenant prefix, and collection ID. Malformed/cross-scope tokens are protocol/forbidden errors; a valid token older than retention returns `Conflict`.

## Operational limits

- No server-side event queue and no multiplexing: each subscriber pulls one event at a time on its connection.
- Falling behind retention is a terminal gap error — shorten retention only if consumers can keep up.
- Heartbeats keep NAT/load-balancer idle timeouts from killing quiet subscriptions.
- Metrics include active subscriptions, delivered events/heartbeats, disconnect reasons, and archive segment/byte/sequence gauges.

## Client APIs

Python / Go / TypeScript expose `watch(...)` returning a stream/iterator of change events with opaque base64 resume tokens. Tokens are not advanced until the caller receives the event.
