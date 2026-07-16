import { Client, Document } from "zydecodb";
import { randomBytes } from "node:crypto";

const ADDR = "127.0.0.1:9470";
  const NUM_DOCS = 5000;

async function main() {
  const db = new Client(ADDR, { timeoutMs: 30000 });
  await db.ping();
  const catalog = db.collection("catalog");

  console.log("Setting up complex schema...");
  
  // Mix of unique and non-unique, compound and single indexes
  await catalog.createIndex(["category"]);
  await catalog.createIndex(["price"]);
  await catalog.createIndex(["status", "category"]); // compound index

  console.log(`Generating ${NUM_DOCS} products...`);
  const categories = ["electronics", "clothing", "home", "books", "toys"];
  const statuses = ["active", "discontinued", "backorder", "preorder"];
  
  // Clear any existing
  await catalog.deleteMany({});
  
  // Also drop old indexes if needed (for clean slate, though we don't have an explicit drop api exposed)
  // Insert in batches of 100
  let inserted = 0;
  const batch: Document[] = [];
  for (let i = 0; i < NUM_DOCS; i++) {
    const isPremium = Math.random() > 0.9;
    batch.push({
      _id: randomBytes(16).toString("hex"),
      sku: `SKU-${randomBytes(8).toString("hex")}`,
      category: categories[Math.floor(Math.random() * categories.length)],
      status: statuses[Math.floor(Math.random() * statuses.length)],
      price: Math.floor(Math.random() * 1000) + (isPremium ? 500 : 10),
      stock: Math.floor(Math.random() * 500),
      rating: Math.round(Math.random() * 50) / 10,
      is_premium: isPremium,
      tags: [`tag_${Math.floor(Math.random() * 20)}`, `tag_${Math.floor(Math.random() * 20)}`],
      metadata: {
        weight: Math.random() * 10,
        color: ["red", "blue", "green", "black", "white"][Math.floor(Math.random() * 5)]
      }
    });
  }
  console.log(`Inserting ${NUM_DOCS} docs in batches...`);
  const t0 = Date.now();
  for (let i = 0; i < batch.length; i += 100) {
    const chunk = batch.slice(i, i + 100);
    await Promise.all(chunk.map(d => catalog.insertOne(d, true)));
  }
  const t1 = Date.now();
  inserted = NUM_DOCS;
  console.log(`\nInserted ${inserted} docs in ${t1 - t0}ms. Starting queries...`);

  // Define some complex query patterns to time
  const queries = [
    {
      name: "Simple equality (indexed)",
      filter: { category: "electronics" },
      sort: [], limit: 0, include: []
    },
    {
      name: "Range + Equality (indexed path)",
      filter: { category: "clothing", price: { $gte: 100, $lte: 200 } },
      sort: [], limit: 0, include: []
    },
    {
      name: "Compound Index Match",
      filter: { status: "active", category: "home" },
      sort: [], limit: 0, include: []
    },
    {
      name: "Deep Nested Field (unindexed, collection scan)",
      filter: { "metadata.color": "red" },
      sort: [], limit: 0, include: []
    },
    {
      name: "Complex Logic ($or with $gt)",
      filter: { 
        $or: [
          { status: "backorder" },
          { stock: 0 }
        ],
        price: { $gt: 500 }
      },
      sort: [], limit: 0, include: []
    },
    {
      name: "Pagination (Sort + Limit + Skip)",
      filter: { status: "active" },
      sort: [{ field: "price", ascending: false }], 
      limit: 10, skip: 20, include: ["sku", "price"]
    }
  ];

  for (const q of queries) {
    const start = Date.now();
    let count = 0;
    
    // Warmup
    await catalog.find(q.filter, { limit: 10 });
    
    // Actual test
    const iterStart = Date.now();
    const results = await catalog.find(q.filter, {
      sort: q.sort as any,
      limit: q.limit,
      skip: q.skip as any,
      include: q.include as any,
    });
    const iterEnd = Date.now();
    
    console.log(`\n--- ${q.name} ---`);
    console.log(`Matched: ${results.length} docs`);
    console.log(`Time:    ${iterEnd - iterStart}ms`);
  }

  // Test partial updates
  console.log(`\n--- Partial Updates ---`);
  const updateStart = Date.now();
  const updateRes = await catalog.updateMany(
    { status: "discontinued", stock: { $gt: 0 } },
    { $set: { status: "clearance" }, $inc: { price: -10 } }
  );
  const updateEnd = Date.now();
  console.log(`Updated discontinued items w/ stock: ${updateRes.modified} docs (${updateEnd - updateStart}ms)`);

  db.close();
}

main().catch(console.error);