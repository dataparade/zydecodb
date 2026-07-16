import { Client } from "zydecodb";
import { execSync } from "node:child_process";
import fs from "node:fs";

import { randomBytes } from "node:crypto";

const ADDR = "127.0.0.1:9470";
const ADMIN_KEY_FILE = "/tmp/zdb/keys.toml"; // Path we used in our server config
const ZDB_CLI = "cargo run --release -p zydecodb --bin zydecodb --";

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Ensure the keys file exists and has an admin key
function ensureAdminEnv() {
  if (!fs.existsSync("/tmp/zdb")) fs.mkdirSync("/tmp/zdb", { recursive: true });
  fs.writeFileSync(ADMIN_KEY_FILE, `
[[keys]]
key = "sk_admin_12345"
tenant_id = "00000000000000000000000000000000"
is_admin = true
`);
  console.log("Wrote admin key to", ADMIN_KEY_FILE);
}

function provisionTenant(name: string): { tenantId: string, apiKey: string } {
  console.log(`Provisioning tenant: ${name}`);
  const tenantId = randomBytes(16).toString("hex");
  const keyId = `key_${name}_1`;
  const out = execSync(`cd ../../ && ${ZDB_CLI} admin keys create --keys-file ${ADMIN_KEY_FILE} --id ${keyId} --tenant ${tenantId} --role read-write`, { encoding: 'utf8' });
  
  const matchKey = out.match(/zdk_[a-zA-Z0-9]+/);
  if (!matchKey) throw new Error(`Failed to parse CLI output:\n${out}`);
  
  return { tenantId, apiKey: matchKey[0] };
}

async function main() {
  ensureAdminEnv();
  console.log("Provisioning tenants via CLI to populate keys file...");

  const t1 = provisionTenant("acme_corp");
  const t2 = provisionTenant("globex");

  console.log("Acme:", t1);
  console.log("Globex:", t2);
  console.log("Sending SIGHUP to DB so it reloads the new keys...");
  
  execSync(`killall -SIGHUP zydecodb || true`);
  
  // Wait for the DB to reload
  await sleep(1000);

  // 2. Connect as Acme and write data
  console.log("\nConnecting as Acme Corp...");
  const acmeClient = new Client(ADDR, { apiKey: t1.apiKey });
  await acmeClient.ping();
  const acmeStore = acmeClient.collection("users");
  
  // ensure the collection exists by touching an index
  await acmeStore.createIndex(["email"], true);
  
  await acmeStore.insertOne({ email: "wile.e@acme.com", role: "admin" });
  await acmeStore.insertOne({ email: "runner@acme.com", role: "user" });
  console.log("Acme inserted 2 users.");

  // 3. Connect as Globex and verify isolation
  console.log("\nConnecting as Globex...");
  const globexClient = new Client(ADDR, { apiKey: t2.apiKey });
  await globexClient.ping();
  const globexStore = globexClient.collection("users");
  
  await globexStore.createIndex(["email"], true);
  
  const globexCount = await globexStore.countDocuments();
  console.log(`Globex sees ${globexCount} users (Expected: 0)`);
  if (globexCount !== 0) throw new Error("Tenant isolation failure!");

  await globexStore.insertOne({ email: "homer@globex.com", role: "safety_inspector" });
  
  // 4. Verify Acme can't see Globex data
  const acmeCount = await acmeStore.countDocuments();
  console.log(`Acme sees ${acmeCount} users (Expected: 2)`);
  if (acmeCount !== 2) throw new Error("Tenant isolation failure!");

  // 5. Test Rate Limiting
  console.log("\nTesting Rate Limiting (Attempting burst of requests as Globex)...");
  
  // Send 1000 fast pings to trip the limit
  let rateLimited = 0;
  let succeeded = 0;
  
  // Try directly using the connection to bypass client-side retry backoff
  try {
    const promises = [];
    for (let i = 0; i < 1500; i++) {
        promises.push(
            globexClient.ping()
                .then(() => succeeded++)
                .catch((e) => {
                    if (e.message && e.message.includes("rate limit exceeded")) {
                        rateLimited++;
                    } else {
                        // Some other error
                    }
                })
        );
    }
    await Promise.all(promises);
  } catch (e) {
    // Ignore pool exhaustion if we hit it
  }
  
  console.log(`Burst results: ${succeeded} succeeded, ${rateLimited} rate limited.`);
  
  // Clean up
  acmeClient.close();
  globexClient.close();
  console.log("Done.");
}

main().catch(console.error);