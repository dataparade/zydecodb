zydecodb-agent-docs: 1
zydecodb: 1.4.0
topic: typescript
next: query, kv, tx, pitfalls

# TypeScript / Node driver

```bash
npm install zydecodb
```

Node 20+. Pin a version. `Client` is a process-wide pool (`poolSize` default 8).

```ts
import { Client, generateId } from "zydecodb";

const db = new Client("127.0.0.1:9470", { apiKey: "YOUR_KEY" });
try {
  const users = db.collection("users");
  await users.createIndex(["email"], true);
  const id = await users.insertOne({ email: "ada@example.com", age: 30 });
  await users.updateOne({ _id: id }, { $inc: { age: 1 } });
  console.log(generateId());
} finally {
  db.close();
}
```

TLS: `{ tls: true }` or a `tls.ConnectionOptions`. Also `timeoutMs`,
`watchIdleTimeoutMs` (default 45000; must exceed server `heartbeat_ms`).

## Collection

| Method | Notes |
|--------|--------|
| `createIndex(fields, unique?, expireAfterSeconds?)` | `string[]` or `{ path, ascending }[]` |
| `insertOne(doc, relaxed?, expiresAt?)` | auto `_id` |
| `insertMany` | |
| `replaceOne` / `replaceOneIfMatch` | if-match stale → `ConflictError` |
| `updateOne` / `updateMany` / `updateByIdIfMatch` | `$` operators; upsert on update* |
| `deleteOne` / `deleteMany` | |
| `find` / `findOne` | `{ sort, include, exclude, skip, limit }` |
| `findWithRevision` | `{ doc, revision }[]` |
| `get` / `getWithRevision` | by-id; missing is `null` |
| `countDocuments` / `distinct` | |
| `aggregate(pipeline)` | see `zydecodb --agent aggregate` |
| `watch(resumeToken?)` | async iterable; `zydecodb --agent watch` |

Revisions are opaque `bigint`. `generateId()` builds a time-ordered `_id`.

## Errors

`ConflictError`, `AuthError`, `ServerBusyError`, `InvalidRequestError`,
`ServerError`, `PolicyError`, `UnsupportedFormatError`. Transport:
`ConnectionError`. Lost commit ack: `UnknownCommitError`.

## KV, durability, transactions

- `db.put(key, value, expiresAt)`, `db.get`, `db.delete` (`Buffer` keys).
- `relaxed = true` acks before fsync.
- `await db.withTransaction(async (tx) => { ... })` — `zydecodb --agent tx`.
