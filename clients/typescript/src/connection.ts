import net from "node:net";
import tls from "node:tls";
import { once } from "node:events";

import { ConnectionError, fromStatus } from "./errors.ts";
import { encodeHeader, HEADER_LEN, PROTO_VERSION, Status, Cmd } from "./protocol.ts";

/** A decoded response: a status byte and (possibly empty) payload. */
export interface Response {
  status: number;
  body: Buffer;
}

/** TLS options passed through to `tls.connect`. `true` enables system defaults. */
export type TlsOption = boolean | tls.ConnectionOptions;

interface Pending {
  resolve: (r: Response) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

interface WriteTask {
  frame: Buffer;
  reject: (err: Error) => void;
}

/**
 * A single TCP connection to a ZydecoDB server. Safe for concurrent use:
 * requests are pipelined. The writer queue respects OS backpressure (drain),
 * and the reader loop continuously consumes frames to resolve promises in FIFO order.
 */
export class Connection {
  private socket: net.Socket | null = null;
  private inFlight: Pending[] = [];
  private writeQueue: WriteTask[] = [];
  private writing = false;
  private dead = false;
  lastUsed = 0;

  private readonly host: string;
  private readonly port: number;
  private readonly timeoutMs: number;
  private readonly apiKey: string | null;
  private readonly tlsOpt: TlsOption | null;

  constructor(
    host: string,
    port: number,
    timeoutMs: number,
    apiKey: string | null,
    tlsOpt: TlsOption | null = null,
  ) {
    this.host = host;
    this.port = port;
    this.timeoutMs = timeoutMs;
    this.apiKey = apiKey;
    this.tlsOpt = tlsOpt;
  }

  get connected(): boolean {
    return this.socket !== null && !this.dead;
  }

  connect(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onError = (err: Error): void => {
        this.fail(new ConnectionError(`connect to ${this.host}:${this.port} failed`, { cause: err }));
        reject(this.lastError(err));
      };

      const onReady = (socket: net.Socket): void => {
        socket.removeListener("error", onError);
        socket.setNoDelay(true); // requests are small and latency-sensitive
        this.socket = socket;
        this.lastUsed = Date.now();

        // Start the async reader loop
        this.readLoop(socket).catch((err) => {
          this.fail(new ConnectionError("read loop failed", { cause: err }));
        });

        socket.on("error", (err) => this.fail(new ConnectionError("socket error", { cause: err })));
        socket.on("close", () => this.fail(new ConnectionError("connection closed")));

        if (this.apiKey) {
          this.sessionInit(this.apiKey).then(resolve, reject);
        } else {
          resolve();
        }
      };

      if (this.tlsOpt) {
        const opts: tls.ConnectionOptions =
          this.tlsOpt === true
            ? { host: this.host, port: this.port, servername: this.host }
            : { host: this.host, port: this.port, servername: this.host, ...this.tlsOpt };
        const socket = tls.connect(opts);
        socket.once("error", onError);
        socket.once("secureConnect", () => onReady(socket));
      } else {
        const socket = net.connect({ host: this.host, port: this.port });
        socket.once("error", onError);
        socket.once("connect", () => onReady(socket));
      }
    });
  }

  private lastError(cause: unknown): ConnectionError {
    return new ConnectionError("connection error", { cause });
  }

  private async sessionInit(apiKey: string): Promise<void> {
    const res = await this.request(Cmd.SessionInit, Buffer.from(apiKey, "utf8"));
    if (res.status !== Status.Ok) {
      throw fromStatus(res.status, "SessionInit", res.body);
    }
  }

  /** Send one framed request and resolve with the framed response. */
  request(command: number, payload: Buffer = Buffer.alloc(0)): Promise<Response> {
    return new Promise<Response>((resolve, reject) => {
      if (!this.socket || this.dead) {
        reject(new ConnectionError("not connected"));
        return;
      }
      const timer = setTimeout(() => {
        this.fail(new ConnectionError("request timed out"));
      }, this.timeoutMs);

      this.inFlight.push({ resolve, reject, timer });
      const frame = Buffer.concat([encodeHeader(command, payload.length), payload]);

      this.writeQueue.push({ frame, reject });
      this.pumpWrites();
    });
  }

  private async pumpWrites(): Promise<void> {
    if (this.writing || !this.socket || this.dead) return;
    this.writing = true;

    try {
      this.socket.cork();
      while (this.writeQueue.length > 0) {
        if (this.dead || !this.socket) break;
        const task = this.writeQueue.shift()!;

        const canContinue = this.socket.write(task.frame, (err) => {
          if (err) {
            task.reject(new ConnectionError("write failed", { cause: err }));
            this.fail(new ConnectionError("write failed", { cause: err }));
          }
        });

        // Respect backpressure
        if (!canContinue && !this.dead && this.socket) {
          this.socket.uncork();
          await once(this.socket, "drain");
          if (!this.dead && this.socket) this.socket.cork();
        }
      }
      if (!this.dead && this.socket) this.socket.uncork();
    } catch (err) {
      this.fail(new ConnectionError("write loop error", { cause: err }));
    } finally {
      this.writing = false;
    }
  }

  private async readLoop(socket: net.Socket): Promise<void> {
    let inbox = Buffer.alloc(0);

    for await (const chunk of socket) {
      inbox = inbox.length === 0 ? chunk : Buffer.concat([inbox, chunk]);

      while (inbox.length >= HEADER_LEN) {
        const version = inbox.readUInt8(0);
        if (version !== PROTO_VERSION) {
          throw new ConnectionError(`unexpected protocol version 0x${version.toString(16)}`);
        }

        const status = inbox.readUInt8(1);
        const length = inbox.readUInt32BE(2);

        if (inbox.length < HEADER_LEN + length) break; // wait for more chunks

        const body = inbox.subarray(HEADER_LEN, HEADER_LEN + length);
        inbox = inbox.subarray(HEADER_LEN + length);

        const pending = this.inFlight.shift();
        if (!pending) {
          throw new ConnectionError("received response with no pending request");
        }

        clearTimeout(pending.timer);
        this.lastUsed = Date.now();
        pending.resolve({ status, body: Buffer.from(body) });
      }
    }
  }

  /** Tear down the connection and reject any in-flight requests. */
  private fail(err: ConnectionError): void {
    if (this.dead) return;
    this.dead = true;

    const queue = this.inFlight;
    this.inFlight = [];

    for (const pending of queue) {
      clearTimeout(pending.timer);
      pending.reject(err);
    }

    this.close();
  }

  close(): void {
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
  }

  /** Send a keepalive; resolves true if the server answered OK. */
  async ping(): Promise<boolean> {
    try {
      const res = await this.request(Cmd.Ping);
      return res.status === Status.Ok;
    } catch {
      return false;
    }
  }
}
