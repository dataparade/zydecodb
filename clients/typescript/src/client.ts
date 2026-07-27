import { randomBytes } from "node:crypto";

import type { TlsOption } from "./connection.ts";
import { Collection } from "./collection.ts";
import { ConnectionError, fromStatus, ServerBusyError, ZydecoError } from "./errors.ts";
import { ConnectionPool } from "./pool.ts";
import { ChangeStream } from "./watch.ts";
import type { Connection } from "./connection.ts";
import {
  Cmd,
  decodeDocGetRevResponse,
  decodePage,
  decodePageWithRevision,
  decodeAggregateResponse,
  encodeAggregate,
  encodeCount,
  encodeDelete,
  encodeDistinct,
  encodeDocDel,
  encodeDocPut,
  encodeDocPutIfMatch,
  encodeDocUpdateIfMatch,
  encodeFind,
  encodeIndexDef,
  encodeQueryById,
  encodeUpdate,
  encodePut,
  encodeKey,
  Proj,
  Status,
  type Projection,
  type SortKey,
} from "./protocol.ts";
import { Transaction } from "./transaction.ts";

export interface ClientOptions {
  apiKey?: string;
  /** Per-request I/O timeout in ms (default 5000). */
  timeoutMs?: number;
  /** Maximum pooled connections (default 8). */
  poolSize?: number;
  /** Retries for idempotent operations on transient failures (default 2). */
  maxRetries?: number;
  backoffBaseMs?: number;
  backoffCapMs?: number;
  /**
   * Enable TLS before the protocol handshake. `true` uses system CA defaults;
   * pass a `tls.ConnectionOptions` object for custom roots / SNI / etc.
   */
  tls?: TlsOption;
}

interface ExecOptions {
  retryable: boolean;
  notFoundNull?: boolean;
}

export interface FindOptions {
  sort?: SortKey[];
  projection?: Projection;
  skip?: number;
  limit?: number; // 0 = no limit
  pageSize?: number; // default 100
}

export interface UpdateResult {
  matched: number;
  modified: number;
  upserted_id?: string;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * A pooled, retrying ZydecoDB client. Safe to share across the whole process.
 * Transient transport failures and server EngineBusy responses are retried with
 * full-jitter exponential backoff for operations that are safe to repeat;
 * operator updates and deletes are never retried automatically.
 */
export class Client {
  private readonly pool: ConnectionPool;
  private readonly maxRetries: number;
  private readonly backoffBaseMs: number;
  private readonly backoffCapMs: number;

  constructor(address = "127.0.0.1:9470", options: ClientOptions = {}) {
    const { host, port } = parseAddress(address);
    this.pool = new ConnectionPool({
      host,
      port,
      apiKey: options.apiKey ?? null,
      timeoutMs: options.timeoutMs ?? 5000,
      maxSize: options.poolSize ?? 8,
      tls: options.tls ?? null,
    });
    this.maxRetries = Math.max(0, options.maxRetries ?? 2);
    this.backoffBaseMs = options.backoffBaseMs ?? 50;
    this.backoffCapMs = options.backoffCapMs ?? 2000;
  }

  close(): void {
    this.pool.close();
  }

  /**
   * Run `fn` inside a pinned-connection bounded transaction. Commits on
   * success; rolls back on error. Commit transport failures throw
   * `UnknownCommitError`.
   */
  async withTransaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<{ result: T; seq: bigint }> {
    const conn = await this.pool.acquireExclusive();
    const tx = new Transaction(this.pool, conn);
    try {
      await tx.begin();
      const result = await fn(tx);
      const seq = await tx.commit();
      return { result, seq };
    } catch (err) {
      try {
        await tx.rollback();
      } catch {
        // already finished or transport dead
      }
      throw err;
    }
  }

  private backoff(attempt: number): number {
    const ceiling = Math.min(this.backoffCapMs, this.backoffBaseMs * 2 ** attempt);
    return Math.random() * ceiling;
  }

  private async execute(
    command: number,
    payload: Buffer,
    op: string,
    eo: ExecOptions,
  ): Promise<Buffer | null> {
    let lastErr: Error | null = null;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      const conn = await this.pool.acquire();
      let res;
      try {
        res = await conn.request(command, payload);
      } catch (err) {
        this.pool.discard(conn);
        lastErr = err as Error;
        if (eo.retryable && attempt < this.maxRetries) {
          await sleep(this.backoff(attempt));
          continue;
        }
        throw err;
      }
      this.pool.release(conn);

      if (res.status === Status.Ok) return res.body;
      if (eo.notFoundNull && res.status === Status.NotFound) return null;
      if (res.status === Status.EngineBusy && eo.retryable && attempt < this.maxRetries) {
        console.warn(`[Client] EngineBusy on ${op}, attempt ${attempt}`);
        lastErr = new ServerBusyError(op, res.status, "");
        await sleep(this.backoff(attempt));
        continue;
      }
      throw fromStatus(res.status, op, res.body);
    }
    throw lastErr ?? new ZydecoError(`${op}: retries exhausted`);
  }

  // --- health / introspection ---

  async ping(): Promise<void> {
    await this.execute(Cmd.Ping, Buffer.alloc(0), "Ping", { retryable: true });
  }

  async stats(): Promise<Record<string, unknown>> {
    const body = await this.execute(Cmd.Stats, Buffer.alloc(0), "Stats", { retryable: true });
    return JSON.parse(body!.toString("utf8"));
  }

  // --- document layer (raw bytes; Collection adds JSON ergonomics) ---

  async get(key: Buffer): Promise<Buffer | null> {
    return this.execute(
      Cmd.Get,
      encodeKey(key),
      "Get",
      { retryable: true, notFoundNull: true },
    );
  }

  async put(key: Buffer, value: Buffer, expiresAt: number | bigint = 0): Promise<bigint> {
    const out = await this.execute(
      Cmd.Put,
      encodePut(key, value, expiresAt),
      "Put",
      { retryable: true },
    );
    return decodeU64(out, "Put");
  }

  async delete(key: Buffer): Promise<boolean> {
    const out = await this.execute(
      Cmd.Del,
      encodeKey(key),
      "Delete",
      { retryable: false },
    );
    return out !== null && out.length > 0 && out[0] !== 0;
  }

  async defineIndex(
    collection: string,
    index: string,
    fields: string[],
    unique: boolean,
    ifNotExists = true,
    expireAfterSeconds: number | bigint = 0,
    directions?: boolean[],
  ): Promise<boolean> {
    const payload = encodeIndexDef(
      collection,
      index,
      fields,
      unique,
      expireAfterSeconds,
      directions,
    );
    const conn = await this.pool.acquire();
    let res;
    try {
      res = await conn.request(Cmd.IndexDef, payload);
    } catch (err) {
      this.pool.discard(conn);
      throw err;
    }
    this.pool.release(conn);
    if (ifNotExists && res.status === Status.Conflict) return false;
    if (res.status !== Status.Ok) throw fromStatus(res.status, "IndexDef", res.body);
    return true;
  }

  async putDocument(
    collection: string,
    docId: string,
    body: Buffer,
    relaxed: boolean,
    expiresAt: number | bigint = 0,
  ): Promise<bigint> {
    const out = await this.execute(
      Cmd.DocPut,
      encodeDocPut(collection, Buffer.from(docId, "utf8"), body, relaxed, expiresAt),
      "DocPut",
      { retryable: true },
    );
    return decodeU64(out, "DocPut");
  }

  async deleteDocument(collection: string, docId: string): Promise<boolean> {
    const out = await this.execute(
      Cmd.DocDel,
      encodeDocDel(collection, Buffer.from(docId, "utf8")),
      "DocDel",
      { retryable: false },
    );
    return out !== null && out.length > 0 && out[0] !== 0;
  }

  async getDocument(collection: string, docId: string): Promise<Buffer | null> {
    return this.execute(
      Cmd.Query,
      encodeQueryById(collection, Buffer.from(docId, "utf8")),
      "Query",
      { retryable: true, notFoundNull: true },
    );
  }

  /** Fetch one document and its opaque revision, or null if absent. */
  async getDocumentWithRevision(
    collection: string,
    docId: string,
  ): Promise<{ body: Buffer; revision: bigint } | null> {
    const out = await this.execute(
      Cmd.DocGetRev,
      encodeQueryById(collection, Buffer.from(docId, "utf8")),
      "DocGetRev",
      { retryable: true, notFoundNull: true },
    );
    if (out === null) return null;
    return decodeDocGetRevResponse(out);
  }

  /** Conditional replace; stale/missing revision → Conflict. Returns new revision. */
  async putDocumentIfMatch(
    collection: string,
    docId: string,
    body: Buffer,
    relaxed: boolean,
    ifMatch: bigint | number,
    expiresAt: number | bigint = 0,
  ): Promise<bigint> {
    const out = await this.execute(
      Cmd.DocPutIfMatch,
      encodeDocPutIfMatch(
        collection,
        Buffer.from(docId, "utf8"),
        body,
        relaxed,
        ifMatch,
        expiresAt,
      ),
      "DocPutIfMatch",
      { retryable: false },
    );
    return decodeU64(out, "DocPutIfMatch");
  }

  /** Conditional by-id update; returns the new revision. */
  async updateDocumentIfMatch(
    collection: string,
    docId: string,
    update: Buffer,
    relaxed: boolean,
    ifMatch: bigint | number,
  ): Promise<bigint> {
    const out = await this.execute(
      Cmd.DocUpdateIfMatch,
      encodeDocUpdateIfMatch(
        collection,
        Buffer.from(docId, "utf8"),
        update,
        relaxed,
        ifMatch,
      ),
      "DocUpdateIfMatch",
      { retryable: false },
    );
    return decodeU64(out, "DocUpdateIfMatch");
  }

  /** Returns the raw JSON bodies of matching documents, auto-paginating. */
  async find(collection: string, filter: Buffer, opts: FindOptions = {}): Promise<Buffer[]> {
    const rows = await this.findRows(collection, filter, opts, false);
    return rows.map((r) => r.body);
  }

  /** Like find, but each row includes its opaque revision (bigint). */
  async findWithRevision(
    collection: string,
    filter: Buffer,
    opts: FindOptions = {},
  ): Promise<{ body: Buffer; revision: bigint }[]> {
    const rows = await this.findRows(collection, filter, opts, true);
    return rows.map((r) => ({ body: r.body, revision: r.revision! }));
  }

  private async findRows(
    collection: string,
    filter: Buffer,
    opts: FindOptions,
    withRev: boolean,
  ): Promise<{ body: Buffer; revision?: bigint }[]> {
    const pageSize = opts.pageSize && opts.pageSize > 0 ? opts.pageSize : 100;
    const limit = opts.limit ?? 0;
    const projection = opts.projection ?? { mode: Proj.None, fields: [] };
    const sort = opts.sort ?? [];
    let skip = opts.skip ?? 0;
    let cursor: Buffer = Buffer.alloc(0);
    let yielded = 0;
    const results: { body: Buffer; revision?: bigint }[] = [];
    const cmd = withRev ? Cmd.FindRev : Cmd.Find;
    const op = withRev ? "FindRev" : "Find";

    for (;;) {
      let want = pageSize;
      if (limit !== 0) {
        const remaining = limit - yielded;
        if (remaining <= 0) return results;
        want = Math.min(want, remaining);
      }
      const payload = encodeFind(collection, filter, sort, projection, skip, want, cursor);
      const body = await this.execute(cmd, payload, op, { retryable: true });
      const page = withRev ? decodePageWithRevision(body!) : decodePage(body!);
      skip = 0; // applied on the first page; the cursor carries it onward
      for (const row of page.rows) {
        results.push({ body: row.body, revision: row.revision });
        yielded++;
        if (limit !== 0 && yielded >= limit) return results;
      }
      if (page.cursor === null) return results;
      cursor = page.cursor;
    }
  }

  async update(
    collection: string,
    filter: Buffer,
    update: Buffer,
    multi: boolean,
    relaxed: boolean,
    upsert = false,
  ): Promise<UpdateResult> {
    const body = await this.execute(
      Cmd.Update,
      encodeUpdate(collection, filter, update, multi, relaxed, upsert),
      "Update",
      { retryable: false },
    );
    return JSON.parse(body!.toString("utf8")) as UpdateResult;
  }

  async deleteByFilter(
    collection: string,
    filter: Buffer,
    multi: boolean,
    relaxed: boolean,
  ): Promise<number> {
    const body = await this.execute(
      Cmd.Delete,
      encodeDelete(collection, filter, multi, relaxed),
      "Delete",
      { retryable: false },
    );
    return (JSON.parse(body!.toString("utf8")) as { deleted: number }).deleted;
  }

  async count(collection: string, filter: Buffer): Promise<number> {
    const body = await this.execute(Cmd.Count, encodeCount(collection, filter), "Count", {
      retryable: true,
    });
    return JSON.parse(body!.toString("utf8")) as number;
  }

  async distinct(collection: string, field: string, filter: Buffer): Promise<unknown[]> {
    const body = await this.execute(
      Cmd.Count,
      encodeDistinct(collection, filter, field),
      "Distinct",
      { retryable: true },
    );
    return JSON.parse(body!.toString("utf8")) as unknown[];
  }

  async aggregate(collection: string, pipeline: Buffer): Promise<Buffer[]> {
    const body = await this.execute(
      Cmd.Aggregate,
      encodeAggregate(collection, pipeline),
      "Aggregate",
      { retryable: true },
    );
    return decodeAggregateResponse(body!);
  }

  collection(name: string): Collection {
    return new Collection(this, name);
  }

  /** Open a dedicated (non-pooled) connection for streaming APIs. */
  async openDedicatedConnection(): Promise<Connection> {
    return this.pool.acquireExclusive();
  }

  /** Release a dedicated connection opened via {@link openDedicatedConnection}. */
  releaseDedicatedConnection(conn: Connection): void {
    this.pool.discard(conn);
  }

  watch(
    collection: string,
    resumeToken?: Buffer,
    signal?: AbortSignal,
  ): ChangeStream {
    return new ChangeStream(this, collection, resumeToken ?? Buffer.alloc(0), signal);
  }
}

/**
 * A time-ordered id (UUIDv7-style): a 48-bit millisecond timestamp followed by
 * 80 random bits, hex-encoded. Sorts lexicographically by creation time.
 */
export function generateId(): string {
  const buf = Buffer.alloc(16);
  const ms = BigInt(Date.now()) & ((1n << 48n) - 1n);
  buf.writeUIntBE(Number(ms), 0, 6);
  randomBytes(10).copy(buf, 6);
  return buf.toString("hex");
}

function parseAddress(address: string): { host: string; port: number } {
  const idx = address.lastIndexOf(":");
  if (idx < 0) throw new ZydecoError(`invalid address ${address}: expected host:port`);
  const host = address.slice(0, idx) || "127.0.0.1";
  const port = Number(address.slice(idx + 1));
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new ZydecoError(`invalid port in address ${address}`);
  }
  return { host, port };
}

function decodeU64(body: Buffer | null, op: string): bigint {
  if (!body || body.length !== 8) {
    throw new ZydecoError(`${op}: expected 8-byte sequence, got ${body?.length ?? 0}`);
  }
  return body.readBigUInt64BE(0);
}
