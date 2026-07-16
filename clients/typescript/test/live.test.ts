/**
 * Integration tests against a live ZydecoDB server. Set ZYDECODB_TEST_HOST /
 * ZYDECODB_TEST_PORT (and optionally ZYDECODB_TEST_API_KEY) to point at a
 * running server; the suite is skipped when the server is unreachable, so a
 * plain `npm test` stays green offline.
 */
import assert from "node:assert/strict";
import net from "node:net";
import { test } from "node:test";

import { Client, ConflictError, type Document } from "../src/index.ts";

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
