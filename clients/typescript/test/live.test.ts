/**
 * Integration tests against a live ZydecoDB server. Set ZYDECODB_TEST_HOST /
 * ZYDECODB_TEST_PORT (and optionally ZYDECODB_TEST_API_KEY) to point at a
 * running server; the suite is skipped when the server is unreachable, so a
 * plain `npm test` stays green offline.
 */
import assert from "node:assert/strict";
import net from "node:net";
import { test } from "node:test";

import { AuthError, Client, ConflictError, type Document } from "../src/index.ts";

const HOST = process.env.ZYDECODB_TEST_HOST ?? "127.0.0.1";
const PORT = Number(process.env.ZYDECODB_TEST_PORT ?? "9470");
const API_KEY = process.env.ZYDECODB_TEST_API_KEY ?? undefined;

function serverUp(): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.connect({ host: HOST, port: PORT });
    const done = (up: boolean): void => {
      socket.destroy();
      resolve(up);
    };
    socket.setTimeout(1000);
    socket.once("connect", () => done(true));
    socket.once("timeout", () => done(false));
    socket.once("error", () => done(false));
  });
}

const skip = !(await serverUp());
const uniqueCollection = (): string => `tstest_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;

function newClient(): Client {
  return new Client(`${HOST}:${PORT}`, API_KEY ? { apiKey: API_KEY } : {});
}

test("ping", { skip }, async () => {
  const db = newClient();
  try {
    await db.ping();
  } finally {
    db.close();
  }
});

test("raw kv", { skip }, async () => {
  const db = newClient();
  try {
    const key = Buffer.from(`testkv_${Date.now()}`);
    const val = Buffer.from("hello kv");

    // Get missing
    let res = await db.get(key);
    assert.equal(res, null);

    // Put
    const seq = await db.put(key, val, 0);
    assert.ok(seq > 0n);

    // Get
    res = await db.get(key);
    assert.ok(res);
    assert.equal(res.toString(), "hello kv");

    // Delete
    let existed = await db.delete(key);
    assert.equal(existed, true);
    existed = await db.delete(key);
    assert.equal(existed, false);

    // Put with TTL (expired)
    const expired = Date.now() - 3600000;
    await db.put(key, val, expired);
    res = await db.get(key);
    assert.equal(res, null);
  } finally {
    db.close();
  }
});

test("insert / find / update / delete", { skip }, async () => {
  const db = newClient();
  const coll = db.collection(uniqueCollection());
  try {
    await coll.createIndex(["age"]);
    const ids = await coll.insertMany([
      { name: "Ada", age: 30, city: "London" },
      { name: "Bo", age: 25, city: "NOLA" },
      { name: "Cy", age: 40, city: "NOLA" },
    ]);
    assert.equal(ids.length, 3);

    const got = await coll.find({ age: { $gte: 30 } }, { sort: [{ field: "age", ascending: true }] });
    assert.deepEqual(
      got.map((d: Document) => d.name),
      ["Ada", "Cy"],
    );

    const res = await coll.updateOne({ name: "Bo" }, { $inc: { age: 10 } });
    assert.equal(res.matched, 1);
    assert.equal(res.modified, 1);

    assert.equal(await coll.countDocuments(), 3);
    const cities = (await coll.distinct("city")) as string[];
    assert.deepEqual(cities.slice().sort(), ["London", "NOLA"]);

    assert.equal(await coll.deleteMany({ city: "NOLA" }), 2);
    assert.equal(await coll.countDocuments(), 1);
  } finally {
    db.close();
  }
});

test("unique index conflict", { skip }, async () => {
  const db = newClient();
  const coll = db.collection(uniqueCollection());
  try {
    await coll.createIndex(["email"], true);
    await coll.insertOne({ email: "a@b.com" });
    await assert.rejects(() => coll.insertOne({ email: "a@b.com" }), ConflictError);
  } finally {
    db.close();
  }
});

test("upsert setOnInsert", { skip }, async () => {
  const db = newClient();
  const coll = db.collection(uniqueCollection());
  try {
    const miss = await coll.updateOne(
      { email: "soi@example.com" },
      { $set: { email: "soi@example.com", n: 1 }, $setOnInsert: { created: true } },
      false,
      true,
    );
    assert.equal(miss.matched, 0);
    assert.equal(miss.modified, 0);
    assert.ok(miss.upserted_id);

    let doc = await coll.findOne({ email: "soi@example.com" });
    assert.equal(doc?.created, true);
    assert.equal(doc?.n, 1);

    const hit = await coll.updateOne(
      { email: "soi@example.com" },
      { $set: { n: 2 }, $setOnInsert: { created: false, extra: 1 } },
      false,
      true,
    );
    assert.equal(hit.matched, 1);
    assert.equal(hit.modified, 1);
    assert.equal(hit.upserted_id, undefined);

    doc = await coll.findOne({ email: "soi@example.com" });
    assert.equal(doc?.n, 2);
    assert.equal(doc?.created, true);
    assert.equal(doc?.extra, undefined);
  } finally {
    db.close();
  }
});

test("optimistic concurrency", { skip }, async () => {
  const db = newClient();
  const coll = db.collection(uniqueCollection());
  try {
    const id = await coll.insertOne({ n: 1 });
    const got = await coll.getWithRevision(id);
    assert.ok(got);
    assert.equal(got!.doc.n, 1);
    assert.ok(got!.revision > 0n);
    const newRev = await coll.replaceOneIfMatch(id, { n: 2 }, got!.revision);
    assert.ok(newRev > got!.revision);
    await assert.rejects(
      () => coll.replaceOneIfMatch(id, { n: 3 }, got!.revision),
      ConflictError,
    );
    const after = await coll.updateByIdIfMatch(id, { $inc: { n: 1 } }, newRev);
    assert.ok(after > newRev);
  } finally {
    db.close();
  }
});

test("bounded transaction", { skip }, async () => {
  const db = newClient();
  const collName = uniqueCollection();
  const coll = db.collection(collName);
  try {
    await coll.insertOne({ _seed: true });
    const key = Buffer.from(`tx-ts-${collName}`);
    const { seq } = await db.withTransaction(async (tx) => {
      await tx.put(key, Buffer.from("v1"));
      await tx.putDocument(collName, "u1", { n: 1 });
      const got = await tx.get(key);
      assert.ok(got);
      assert.equal(got!.toString("utf8"), "v1");
    });
    assert.ok(seq > 0n);
    const committed = await db.get(key);
    assert.equal(committed?.toString("utf8"), "v1");
  } finally {
    db.close();
  }
});

/** Resolve to `fallback` if `p` has not settled within `ms`. Clears its timer. */
function settleWithin<T, F>(p: Promise<T>, ms: number, fallback: F): Promise<T | F> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve(fallback), ms);
    p.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

test("watch idle timeout is decoupled from request timeout", { skip }, async (t) => {
  // A 1s request timeout must not apply to the dedicated Watch connection: an
  // idle stream only carries a server heartbeat every heartbeat_ms (3s in CI),
  // so with the request timeout the pending read below would fail.
  const db = new Client(`${HOST}:${PORT}`, {
    ...(API_KEY ? { apiKey: API_KEY } : {}),
    timeoutMs: 1000,
  });
  const coll = db.collection(uniqueCollection());
  const stream = coll.watch();
  let iterator: AsyncIterator<unknown> | null = null;
  let firstSettled = false;
  try {
    await coll.insertOne({ _seed: true }); // Watch requires an existing collection.
    iterator = stream[Symbol.asyncIterator]();
    // The first next() opens the subscription, then blocks on the stream.
    const first = iterator.next();
    first.then(
      () => (firstSettled = true),
      () => (firstSettled = true), // settled below; never leave it unhandled
    );

    const IDLE = Symbol("idle");
    let early: unknown;
    try {
      // Several request-timeout windows with no events; only heartbeats flow.
      early = await settleWithin(first, 5000, IDLE);
    } catch (e) {
      if (e instanceof AuthError) {
        t.skip(`change streams not enabled on server: ${e.message}`);
        return;
      }
      throw new Error(`idle watch died: ${String(e)}`);
    }
    assert.equal(early, IDLE, "idle watch produced an event before any write");

    const docId = await coll.insertOne({ n: 1 });
    const res = await settleWithin(first, 10_000, null);
    assert.ok(res, "no change event within 10s of the write");
    assert.equal(res.done, false);
    const ev = res.value as { op: string; docId: string; document: Record<string, unknown> | null };
    assert.equal(ev.op, "upsert");
    assert.equal(ev.docId, docId);
    assert.equal(ev.document?.n, 1);
  } finally {
    if (firstSettled) {
      // Generator is parked at a yield; return() runs its cleanup.
      await iterator?.return?.();
    } else {
      // A read is still pending; return() would queue behind it. Drop the
      // socket instead so the failure path does not wait for the idle timeout.
      await stream.close();
    }
    db.close();
  }
});
