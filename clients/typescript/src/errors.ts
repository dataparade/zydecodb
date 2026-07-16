import { Status, statusName } from "./protocol.ts";

/** Base class for every error thrown by this driver. */
export class ZydecoError extends Error {}

/**
 * A transport-level failure (connect/write/read). Safe to retry for idempotent
 * operations; the client does this automatically.
 */
export class ConnectionError extends ZydecoError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ConnectionError";
  }
}

/**
 * A non-OK response from the server. `status` is the wire status byte so callers
 * can branch on the failure class without string-matching messages.
 */
export class ServerError extends ZydecoError {
  readonly status: number;
  readonly op: string;

  constructor(op: string, status: number, detail: string) {
    const base = `${op} failed: ${statusName(status)}`;
    super(detail ? `${base} (${detail})` : base);
    this.name = "ServerError";
    this.op = op;
    this.status = status;
  }
}

/** A constraint conflict, e.g. a unique-index violation (status 0x03). */
export class ConflictError extends ServerError {}

/** Unauthorized or forbidden (status 0x0B / 0x0C). */
export class AuthError extends ServerError {}

/** The server is shedding load (status 0x07). Retried automatically. */
export class ServerBusyError extends ServerError {}

/** The server rejected the request as malformed or invalid. */
export class InvalidRequestError extends ServerError {}

/** Build the most specific ServerError for a non-OK response. */
export function fromStatus(status: number, op: string, payload: Buffer): ServerError {
  const detail = payload.length ? payload.toString("utf8") : "";
  switch (status) {
    case Status.Conflict:
      return new ConflictError(op, status, detail);
    case Status.Unauthorized:
    case Status.Forbidden:
      return new AuthError(op, status, detail);
    case Status.EngineBusy:
      return new ServerBusyError(op, status, detail);
    case Status.ProtocolError:
    case Status.InvalidKey:
    case Status.InvalidValue:
      return new InvalidRequestError(op, status, detail);
    default:
      return new ServerError(op, status, detail);
  }
}
