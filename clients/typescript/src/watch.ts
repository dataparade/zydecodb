import type { Client } from "./client.ts";
import type { Connection } from "./connection.ts";
import { ConnectionError, fromStatus } from "./errors.ts";
import {
  Cmd,
  decodeWatchFrame,
  encodeWatch,
  Status,
  WatchOp,
} from "./protocol.ts";

/** One durable change from a Watch subscription. */
export interface ChangeEvent {
  op: "upsert" | "delete";
  docId: string;
  document: Record<string, unknown> | null;
  /** Base64-encoded opaque resume token from this frame. */
  resumeToken: string;
}

/** Async iterable change stream over a dedicated connection. */
export class ChangeStream implements AsyncIterable<ChangeEvent> {
  private readonly client: Client;
  private readonly collection: string;
  private readonly resumeToken: Buffer;
  private readonly signal: AbortSignal | undefined;
  private conn: Connection | null = null;
  private opened = false;

  constructor(
    client: Client,
    collection: string,
    resumeToken: Buffer = Buffer.alloc(0),
    signal?: AbortSignal,
  ) {
    this.client = client;
    this.collection = collection;
    this.resumeToken = resumeToken;
    this.signal = signal;
  }

  async close(): Promise<void> {
    if (this.conn) {
      this.client.releaseDedicatedConnection(this.conn);
      this.conn = null;
    }
  }

  private throwIfAborted(): void {
    if (this.signal?.aborted) {
      throw this.signal.reason ?? new ConnectionError("watch aborted");
    }
  }

  private async open(): Promise<void> {
    if (this.opened) return;
    this.throwIfAborted();
    this.conn = await this.client.openDedicatedConnection();
    const res = await this.conn.request(Cmd.Watch, encodeWatch(this.collection, this.resumeToken));
    if (res.status !== Status.Ok) {
      await this.close();
      throw fromStatus(res.status, "Watch", res.body);
    }
    const frame = decodeWatchFrame(res.body);
    if (frame.kind !== "ack") {
      await this.close();
      throw fromStatus(Status.ProtocolError, "Watch", Buffer.from("expected initial ack"));
    }
    this.opened = true;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<ChangeEvent, void, undefined> {
    try {
      await this.open();
      while (true) {
        this.throwIfAborted();
        const res = await this.conn!.waitResponse();
        if (res.status !== Status.Ok) {
          throw fromStatus(res.status, "Watch", res.body);
        }
        const frame = decodeWatchFrame(res.body);
        if (frame.kind === "ack" || frame.kind === "heartbeat") {
          continue;
        }
        if (frame.kind !== "event" || frame.op === undefined || !frame.docId) {
          throw fromStatus(Status.ProtocolError, "Watch", Buffer.from("invalid event frame"));
        }
        const token = frame.resumeToken.toString("base64");
        const docId = frame.docId.toString("utf8");
        if (frame.op === WatchOp.Upsert) {
          const document =
            frame.body && frame.body.length > 0
              ? (JSON.parse(frame.body.toString("utf8")) as Record<string, unknown>)
              : {};
          yield { op: "upsert", docId, document, resumeToken: token };
        } else if (frame.op === WatchOp.Delete) {
          yield { op: "delete", docId, document: null, resumeToken: token };
        } else {
          throw fromStatus(
            Status.ProtocolError,
            "Watch",
            Buffer.from(`unknown op 0x${frame.op.toString(16)}`),
          );
        }
      }
    } finally {
      await this.close();
    }
  }
}
