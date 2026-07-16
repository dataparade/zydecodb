/**
 * A small HTTP backend (a users API) on top of the ZydecoDB client. One shared,
 * pooled Client handles concurrent HTTP requests.
 *
 * Run against a local server (default 127.0.0.1:9470) with Node 22.18+:
 *
 *   node examples/user_backend.ts
 *
 * Then:
 *
 *   curl -s localhost:8080/users -d '{"name":"Ada","email":"ada@x.io","age":30}'
 *   curl -s 'localhost:8080/users?min_age=18'
 *   curl -s localhost:8080/users/<id>
 *   curl -s -X PATCH localhost:8080/users/<id> -d '{"age":31}'
 *   curl -s -X DELETE localhost:8080/users/<id>
 */
import http from "node:http";
import { randomBytes } from "node:crypto";

import { Client, ConflictError, type Document } from "../src/index.ts";

const addr = process.env.ZYDECODB_ADDR ?? "127.0.0.1:9470";
const apiKey = process.env.ZYDECODB_API_KEY;
const COLLECTION = "app_users";

const db = new Client(addr, apiKey ? { apiKey } : {});
const users = db.collection(COLLECTION);

await db.ping();
// A unique email per user — enforced by the database, not the app.
await users.createIndex(["email"], true);

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = typeof body === "string" ? body : JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(payload);
}

function readJson(req: http.IncomingMessage): Promise<Document> {
  return new Promise((resolve, reject) => {
    let raw = "";
    let tooBig = false;
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 1 << 20) {
        tooBig = true;
        req.destroy();
      }
    });
    req.on("end", () => {
      if (tooBig) return reject(new Error("body too large"));
      try {
        resolve(raw ? (JSON.parse(raw) as Document) : {});
      } catch {
        reject(new Error("invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", "http://localhost");
    const parts = url.pathname.split("/").filter(Boolean);

    if (parts[0] === "users" && parts.length === 1) {
      if (req.method === "POST") {
        const doc = await readJson(req);
        if (typeof doc.email !== "string") {
          send(res, 400, { error: "email is required" });
          return;
        }
        try {
          const id = await users.insertOne(doc);
          send(res, 201, { id });
        } catch (err) {
          if (err instanceof ConflictError) send(res, 409, { error: "email already exists" });
          else throw err;
        }
        return;
      }
      if (req.method === "GET") {
        const filter: Document = {};
        const minAge = url.searchParams.get("min_age");
        if (minAge !== null) {
          const age = Number(minAge);
          if (!Number.isInteger(age)) {
            send(res, 400, { error: "min_age must be an integer" });
            return;
          }
          filter.age = { $gte: age };
        }
        const list = await users.find(filter, {
          sort: [{ field: "age", ascending: true }],
          limit: 100,
        });
        send(res, 200, list);
        return;
      }
      send(res, 405, { error: "method not allowed" });
      return;
    }

    // /users/{id}
    if (parts[0] === "users" && parts.length === 2) {
      const id = parts[1]!;
      if (req.method === "GET") {
        const doc = await users.get(id);
        if (!doc) send(res, 404, { error: "not found" });
        else send(res, 200, doc);
        return;
      }
      if (req.method === "PATCH") {
        const fields = await readJson(req);
        delete fields._id;
        if (Object.keys(fields).length === 0) {
          send(res, 400, { error: "no fields to update" });
          return;
        }
        try {
          const result = await users.updateOne({ _id: id }, { $set: fields });
          if (result.matched === 0) send(res, 404, { error: "not found" });
          else send(res, 200, result);
        } catch (err) {
          if (err instanceof ConflictError) send(res, 409, { error: "email already exists" });
          else throw err;
        }
        return;
      }
      if (req.method === "DELETE") {
        const deleted = await users.deleteOne({ _id: id });
        if (deleted === 0) send(res, 404, { error: "not found" });
        else send(res, 204, "");
        return;
      }
      send(res, 405, { error: "method not allowed" });
      return;
    }

    if (parts[0] === "login" && parts.length === 1) {
      if (req.method !== "POST") {
        send(res, 405, { error: "method not allowed" });
        return;
      }
      const doc = await readJson(req);
      const email = typeof doc.email === "string" ? doc.email : "";
      const matches = await users.find({ email }, { limit: 1 });
      if (matches.length === 0) {
        send(res, 401, { error: "invalid email" });
        return;
      }
      const token = randomBytes(32).toString("hex");
      const expiresAt = Date.now() + 24 * 3600 * 1000;
      await db.put(Buffer.from(`session:${token}`), Buffer.from(matches[0]._id as string), expiresAt);
      send(res, 200, { token });
      return;
    }

    if (parts[0] === "me" && parts.length === 1) {
      if (req.method !== "GET") {
        send(res, 405, { error: "method not allowed" });
        return;
      }
      const auth = req.headers.authorization ?? "";
      if (!auth.startsWith("Bearer ")) {
        send(res, 401, { error: "missing bearer token" });
        return;
      }
      const token = auth.slice(7);
      const idBytes = await db.get(Buffer.from(`session:${token}`));
      if (!idBytes) {
        send(res, 401, { error: "invalid or expired token" });
        return;
      }
      const doc = await users.get(idBytes.toString("utf8"));
      if (!doc) {
        send(res, 404, { error: "user not found" });
        return;
      }
      send(res, 200, doc);
      return;
    }

    send(res, 404, { error: "not found" });
  } catch (err) {
    const message = err instanceof Error ? err.message : "internal error";
    const status = message === "invalid JSON body" || message === "body too large" ? 400 : 500;
    send(res, status, { error: status === 500 ? "internal error" : message });
    if (status === 500) console.error("error:", err);
  }
});

const listen = Number(process.env.PORT ?? "8080");
server.listen(listen, () => console.log(`user_backend listening on :${listen} (db ${addr})`));

const shutdown = (): void => {
  server.close();
  db.close();
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
