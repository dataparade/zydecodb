import type { Connection } from "./connection.ts";
import { fromStatus, ZydecoError } from "./errors.ts";
import type { ConnectionPool } from "./pool.ts";
import {
  Cmd,
  decodeBeginResponse,
  decodeCommitResponse,
  decodeDocGetRevResponse,
  encodeDocDel,
  encodeDocPut,
  encodeDocPutIfMatch,
  encodeDocUpdateIfMatch,
  encodeKey,
  encodePut,
  encodeQueryById,
  Status,
} from "./protocol.ts";

/** Commit may have succeeded; transport failed before the ack was received. */
export class UnknownCommitError extends ZydecoError {
  constructor(message: string) {
    super(message);
    this.name = "UnknownCommitError";
  }
}

/**
 * A pinned-connection bounded transaction. Not reusable after commit/rollback.
 * No automatic retries.
 */
export class Transaction {
  private done = false;
  private readonly pool: ConnectionPool;
  private conn: Connection | null;
  txId = 0n;
  snapshotSeq = 0n;

  constructor(pool: ConnectionPool, conn: Connection | null) {
    this.pool = pool;
    this.conn = conn;
  }

  private ensureOpen(): void {
    if (this.done || !this.conn) {
      throw new ZydecoError("transaction already finished");
    }
  }

  private async request(
    command: number,
    payload: Buffer,
    op: string,
    opts: { notFoundNull?: boolean } = {},
  ): Promise<Buffer | null> {
    this.ensureOpen();
    const conn = this.conn!;
    let status: number;
    let body: Buffer;
    try {
      ({ status, body } = await conn.request(command, payload));
    } catch (err) {
      this.pool.discard(conn);
      this.conn = null;
      this.done = true;
      throw err;
    }
    if (status === Status.Ok) return body;
    if (opts.notFoundNull && status === Status.NotFound) return null;
    throw fromStatus(status, op, body);
  }

  async begin(): Promise<void> {
    const body = await this.request(Cmd.Begin, Buffer.alloc(0), "Begin");
    const decoded = decodeBeginResponse(body!);
    this.txId = decoded.txId;
    this.snapshotSeq = decoded.snapshotSeq;
  }

  async commit(): Promise<bigint> {
    this.ensureOpen();
    const conn = this.conn!;
    let status: number;
    let body: Buffer;
    try {
      ({ status, body } = await conn.request(Cmd.Commit, Buffer.alloc(0)));
    } catch (err) {
      this.pool.discard(conn);
      this.conn = null;
      this.done = true;
      throw new UnknownCommitError(
        `Commit: transport failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
    this.pool.releaseExclusive(conn);
    this.conn = null;
    this.done = true;
    if (status !== Status.Ok) throw fromStatus(status, "Commit", body);
    return decodeCommitResponse(body);
  }

  async rollback(): Promise<void> {
    if (this.done) return;
    if (!this.conn) {
      this.done = true;
      return;
    }
    const conn = this.conn;
    let status: number;
    let body: Buffer;
    try {
      ({ status, body } = await conn.request(Cmd.Rollback, Buffer.alloc(0)));
    } catch (err) {
      this.pool.discard(conn);
      this.conn = null;
      this.done = true;
      throw err;
    }
    this.pool.releaseExclusive(conn);
    this.conn = null;
    this.done = true;
    if (status !== Status.Ok) throw fromStatus(status, "Rollback", body);
  }

  async put(key: Buffer, value: Buffer, expiresAt = 0): Promise<void> {
    await this.request(Cmd.Put, encodePut(key, value, expiresAt), "Put");
  }

  async get(key: Buffer): Promise<Buffer | null> {
    return this.request(Cmd.Get, encodeKey(key), "Get", { notFoundNull: true });
  }

  async delete(key: Buffer): Promise<void> {
    await this.request(Cmd.Del, encodeKey(key), "Del");
  }

  async putDocument(collection: string, docId: string, doc: unknown): Promise<void> {
    const body = Buffer.from(JSON.stringify(doc), "utf8");
    await this.request(
      Cmd.DocPut,
      encodeDocPut(collection, Buffer.from(docId, "utf8"), body, false, 0),
      "DocPut",
    );
  }

  async putDocumentIfMatch(
    collection: string,
    docId: string,
    doc: unknown,
    ifMatch: bigint,
  ): Promise<void> {
    const body = Buffer.from(JSON.stringify(doc), "utf8");
    await this.request(
      Cmd.DocPutIfMatch,
      encodeDocPutIfMatch(collection, Buffer.from(docId, "utf8"), body, false, ifMatch, 0),
      "DocPutIfMatch",
    );
  }

  async deleteDocument(collection: string, docId: string): Promise<void> {
    await this.request(
      Cmd.DocDel,
      encodeDocDel(collection, Buffer.from(docId, "utf8")),
      "DocDel",
    );
  }

  async getDocumentWithRevision(
    collection: string,
    docId: string,
  ): Promise<{ body: unknown; revision: bigint } | null> {
    const out = await this.request(
      Cmd.DocGetRev,
      encodeQueryById(collection, Buffer.from(docId, "utf8")),
      "DocGetRev",
      { notFoundNull: true },
    );
    if (out === null) return null;
    const decoded = decodeDocGetRevResponse(out);
    return {
      body: JSON.parse(decoded.body.toString("utf8")),
      revision: decoded.revision,
    };
  }

  async updateDocumentIfMatch(
    collection: string,
    docId: string,
    update: unknown,
    ifMatch: bigint,
  ): Promise<void> {
    const body = Buffer.from(JSON.stringify(update), "utf8");
    await this.request(
      Cmd.DocUpdateIfMatch,
      encodeDocUpdateIfMatch(collection, Buffer.from(docId, "utf8"), body, false, ifMatch),
      "DocUpdateIfMatch",
    );
  }
}
