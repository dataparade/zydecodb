import { Connection } from "./connection.ts";
import { ConnectionError } from "./errors.ts";

export interface PoolOptions {
  host: string;
  port: number;
  apiKey: string | null;
  timeoutMs: number;
  maxSize: number;
  acquireTimeoutMs?: number;
  keepaliveIdleMs?: number;
}

/**
 * A multiplexing connection pool. Because the server executes requests serially
 * per connection but processes pipelined frames without requiring a round-trip 
 * between them, we can safely and efficiently multiplex many concurrent requests 
 * across a small pool of open connections. This avoids artificial queueing
 * timeouts and allows high-throughput `Promise.all` patterns.
 */
export class ConnectionPool {
  private readonly connections: Connection[] = [];
  private total = 0;
  private nextIdx = 0;
  private closed = false;
  private connecting: Promise<Connection> | null = null;

  private readonly keepaliveIdleMs: number;
  private readonly opts: PoolOptions;

  constructor(opts: PoolOptions) {
    this.opts = opts;
    this.keepaliveIdleMs = opts.keepaliveIdleMs ?? 30_000;
  }

  private newConnection(): Connection {
    return new Connection(this.opts.host, this.opts.port, this.opts.timeoutMs, this.opts.apiKey);
  }

  async acquire(): Promise<Connection> {
    if (this.closed) throw new ConnectionError("pool is closed");

    // Clean out dead connections
    for (let i = this.connections.length - 1; i >= 0; i--) {
      const c = this.connections[i];
      if (c && !c.connected) {
        this.connections.splice(i, 1);
        this.total -= 1;
      }
    }

    // Lazily grow the pool up to maxSize if we're under capacity.
    if (this.total < this.opts.maxSize) {
      if (!this.connecting) {
        this.connecting = (async () => {
          try {
            const fresh = this.newConnection();
            await fresh.connect();
            this.connections.push(fresh);
            this.total += 1;
            return fresh;
          } finally {
            this.connecting = null;
          }
        })();
      }
      
      // If we literally have 0 active connections, we MUST wait for this first one.
      if (this.connections.length === 0) {
        return this.connecting;
      }
      
      // Otherwise, fall through and multiplex on the existing ones 
      // while the new connection builds in the background!
    }

    // Multiplex round-robin
    const conn = this.connections[this.nextIdx % this.connections.length]!;
    this.nextIdx++;
    
    // Idle validation
    if (Date.now() - conn.lastUsed > this.keepaliveIdleMs) {
      if (!(await conn.ping())) {
        this.discard(conn);
        return this.acquire(); // Try again
      }
    }
    
    return conn;
  }

  /** Release is a no-op in a multiplexing pool, but we validate it is alive. */
  release(conn: Connection): void {
    if (this.closed || !conn.connected) {
      this.discard(conn);
    }
  }

  /** Permanently drop a (presumed-broken) connection, freeing its slot. */
  discard(conn: Connection): void {
    conn.close();
    const idx = this.connections.indexOf(conn);
    if (idx >= 0) {
      this.connections.splice(idx, 1);
      this.total -= 1;
    }
  }

  close(): void {
    this.closed = true;
    for (const conn of this.connections) conn.close();
    this.total = 0;
    this.connections.length = 0;
  }
}
