/**
 * Quickstart: connect, index, insert, query, update, and delete documents.
 *
 * Run against a local server (default 127.0.0.1:9470) with Node 22.18+:
 *
 *   node examples/quickstart.ts
 *
 * Override with ZYDECODB_ADDR and ZYDECODB_API_KEY.
 */
import { Client, type Document } from "../src/index.ts";

const addr = process.env.ZYDECODB_ADDR ?? "127.0.0.1:9470";
const apiKey = process.env.ZYDECODB_API_KEY;

const db = new Client(addr, apiKey ? { apiKey } : {});

try {
  await db.ping();

  const users = db.collection(`quickstart_${Date.now()}`);
  await users.createIndex(["age"]);

  const ids = await users.insertMany([
    { name: "Ada", age: 30, city: "London" },
    { name: "Bo", age: 25, city: "NOLA" },
    { name: "Cy", age: 40, city: "NOLA" },
  ]);
  console.log(`inserted ${ids.length} users:`, ids);

  const adults = await users.find(
    { age: { $gte: 30 } },
    { sort: [{ field: "age", ascending: true }] },
  );
  console.log("adults (age >= 30):");
  for (const u of adults as Document[]) {
    console.log(`  ${u.name} (${u.age})`);
  }

  const res = await users.updateMany({ city: "NOLA" }, { $set: { region: "South" } });
  console.log(`tagged ${res.modified} NOLA users as South`);

  console.log("distinct cities:", await users.distinct("city"));

  const deleted = await users.deleteMany({ age: { $lt: 28 } });
  console.log(`deleted ${deleted} users under 28`);
  console.log("remaining users:", await users.countDocuments());
} catch (err) {
  console.error("quickstart failed:", err);
  process.exitCode = 1;
} finally {
  db.close();
}
