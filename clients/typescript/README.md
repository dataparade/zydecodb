# ZydecoDB TypeScript / Node driver

Official TypeScript/Node client for [ZydecoDB](../../README.md). Built on Node's
standard library (`node:net`), no runtime dependencies.

## Install

```bash
npm install zydecodb
```

Requires Node.js 20+. (Working from a checkout of this repo:
`npm install file:clients/typescript`.)

## Quick start

```ts
import { Client } from "zydecodb";

// Plain TCP (localhost). For TLS: { apiKey: "YOUR_KEY", tls: true }
const db = new Client("127.0.0.1:9470", { apiKey: "YOUR_KEY" });
try {
  const users = db.collection("users");
  await users.createIndex(["email"], true);

  const id = await users.insertOne({ email: "ada@example.com", name: "Ada", age: 30 });

  const adults = await users.find({ age: { $gte: 18 } }, { sort: [{ field: "age", ascending: true }] });
  for (const u of adults) console.log(u.name, u.age);

  await users.updateOne({ _id: id }, { $inc: { age: 1 } });
  console.log(await users.countDocuments());
} finally {
  db.close();
}
```

## What you get

- **Connection pooling.** `Client` owns a bounded pool (`poolSize`, default 8)
  and is safe to share across the whole process.
- **Automatic retries with backoff.** Transient transport failures and server
  `EngineBusy` responses are retried (full-jitter exponential backoff) for
  operations that are safe to repeat. Operator updates and deletes are never
  retried automatically.
- **Keepalive.** Idle pooled connections are validated with a `ping` on
  checkout and transparently replaced if dead.
- **Typed error taxonomy.** Non-OK responses throw a specific subclass:
  `ConflictError` (unique-index violation), `AuthError`, `ServerBusyError`,
  `InvalidRequestError`, or the base `ServerError` — each carrying the wire
  `status` byte. Transport problems throw `ConnectionError`.
- **`Collection` API.** `insertOne/Many`, `find`/`findOne`,
  `updateOne/Many`, `deleteOne/Many`, `countDocuments`, `distinct`, and
  `createIndex`, with `$`-operators, sort, projection, and skip/limit.
  Pagination is repeatable-read across pages.
- **Raw KV with TTL.** Side-channel `put` (with `expiresAt`), `get`, and `delete` methods on `Client` for session data that needs a time-to-live.
- **TLS.** Pass `tls: true` for system CA defaults, or a `tls.ConnectionOptions`
  object for custom roots / SNI / `rejectUnauthorized`.

## Optimistic concurrency

```ts
const got = await users.getWithRevision(id);
got!.doc.age = (got!.doc.age as number) + 1;
try {
  await users.replaceOneIfMatch(id, got!.doc, got!.revision);
} catch (err) {
  if (err instanceof ConflictError) {
    // re-read and retry, or merge
  } else throw err;
}
```

Also: `findWithRevision`, `updateByIdIfMatch`. Revisions are opaque `bigint`
values. Stale/missing documents throw `ConflictError`. Against an older server
these methods fail with a protocol error instead of silently becoming
unconditional writes.

## Bounded transactions

```ts
const { seq } = await db.withTransaction(async (tx) => {
  await tx.put(Buffer.from("session"), Buffer.from("active"));
  await tx.putDocument("users", "u1", { n: 1 });
});
```

Uses an exclusively checked-out connection (not multiplexed). No automatic
retries. Collections must already exist. Filter queries/updates and DDL are
rejected inside a transaction. Commit transport failure throws
`UnknownCommitError` — reconcile by re-reading keys. Older servers reject
`Begin` with a protocol error.

## Durability

Writes are durable (fsync-on-commit) by default. For latency-sensitive,
loss-tolerant writes, pass `relaxed = true` on any write to acknowledge before
the fsync.

```ts
await users.insertOne(doc, true);
await users.updateOne({ _id: "ada" }, { $inc: { hits: 1 } }, true);
```

Filtered positional `$set` (exactly one array match) uses the same update APIs
with a path like `items.$[skuId=ABC].qty` — no new client methods.

Directional indexes: pass `{ path, ascending: false }` objects to `createIndex`
for DESC fields (string fields remain all-ascending).

## Examples

- [`examples/quickstart.ts`](examples/quickstart.ts) — end-to-end collection demo.
- [`examples/user_backend.ts`](examples/user_backend.ts) — a small `node:http`
  users API sharing one pooled client across concurrent requests.

With Node 22.18+ you can run the TypeScript directly:

```bash
node examples/quickstart.ts
node examples/user_backend.ts
```

Both read `ZYDECODB_ADDR` (default `127.0.0.1:9470`) and `ZYDECODB_API_KEY`.

## Development

```bash
npm install        # dev deps: typescript, @types/node
npm run typecheck  # tsc --noEmit
npm run build      # emit dist/ (ESM + .d.ts)
npm test           # node --test (native type stripping; no transpiler)
```

The codec is verified byte-for-byte against the shared
[conformance vectors](../conformance) (generated from Rust; Python is the
hand-maintained reference client). No server required. CI job
`wire-conformance` fails the PR on drift.

```bash
npm test test/conformance.test.ts
```

Live integration tests use `ZYDECODB_TEST_HOST` / `ZYDECODB_TEST_PORT` (and
optional `ZYDECODB_TEST_API_KEY`) and are skipped when the server is unreachable.
