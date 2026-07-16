/**
 * Binary wire protocol: command/status codes and the encode/decode of payload
 * bodies, with no I/O. Mirrors the Rust definitions in
 * crates/zydecodb-engine/src/frame.rs and crates/zydecodb-document/src/wire.rs
 * and is verified byte-for-byte against clients/conformance/vectors.json.
 */

export const PROTO_VERSION = 0x01;
export const HEADER_LEN = 6;

// Command codes (envelope byte 1).
export const Cmd = {
  Put: 0x01,
  Get: 0x02,
  Del: 0x03,
  Query: 0x20,
  DocPut: 0x21,
  DocDel: 0x22,
  Find: 0x23,
  Update: 0x24,
  Delete: 0x25,
  Count: 0x26,
  IndexDef: 0x30,
  SessionInit: 0x40,
  Ping: 0xf0,
  Stats: 0xf1,
} as const;

// Query / count sub-command discriminators (first payload byte).
const QUERY_BY_ID = 0x00;
const QUERY_INDEX_RANGE = 0x01;
const COUNT_MODE_COUNT = 0x00;
const COUNT_MODE_DISTINCT = 0x01;

// Projection modes for Find.
export const Proj = {
  None: 0x00,
  Include: 0x01,
  Exclude: 0x02,
} as const;

// Bit 0 of the optional trailing flags byte on write payloads.
const FLAG_RELAXED = 0x01;

// Status codes (response envelope byte 1).
export const Status = {
  Ok: 0x00,
  NotFound: 0x01,
  Error: 0x02,
  Conflict: 0x03,
  IoError: 0x04,
  InvalidKey: 0x05,
  InvalidValue: 0x06,
  EngineBusy: 0x07,
  ProtocolError: 0x08,
  Unauthorized: 0x0b,
  Forbidden: 0x0c,
} as const;

const STATUS_NAMES: Record<number, string> = {
  [Status.Ok]: "Ok",
  [Status.NotFound]: "NotFound",
  [Status.Error]: "Error",
  [Status.Conflict]: "Conflict",
  [Status.IoError]: "IoError",
  [Status.InvalidKey]: "InvalidKey",
  [Status.InvalidValue]: "InvalidValue",
  [Status.EngineBusy]: "EngineBusy",
  [Status.ProtocolError]: "ProtocolError",
  [Status.Unauthorized]: "Unauthorized",
  [Status.Forbidden]: "Forbidden",
};

export function statusName(status: number): string {
  return STATUS_NAMES[status] ?? `0x${status.toString(16).padStart(2, "0")}`;
}

/** Build the 6-byte request envelope header. */
export function encodeHeader(command: number, payloadLen: number): Buffer {
  const h = Buffer.allocUnsafe(HEADER_LEN);
  h.writeUInt8(PROTO_VERSION, 0);
  h.writeUInt8(command, 1);
  h.writeUInt32BE(payloadLen >>> 0, 2);
  return h;
}

/** Build a Put payload. */
export function encodePut(key: Buffer, value: Buffer, expiresAt: number | bigint = 0): Buffer {
  const buf = Buffer.allocUnsafe(16 + 8 + 8 + 4 + 4 + key.length + value.length);
  buf.fill(0, 0, 16); // routing key
  buf.writeBigUInt64BE(0n, 16); // txid
  buf.writeBigUInt64BE(BigInt(expiresAt), 24);
  buf.writeUInt32BE(key.length, 32);
  buf.writeUInt32BE(value.length, 36);
  key.copy(buf, 40);
  value.copy(buf, 40 + key.length);
  return buf;
}

/** Build a Key payload for Get/Del. */
export function encodeKey(key: Buffer): Buffer {
  const buf = Buffer.allocUnsafe(16 + 8 + 4 + key.length);
  buf.fill(0, 0, 16); // routing key
  buf.writeBigUInt64BE(0n, 16); // snapshot_seq
  buf.writeUInt32BE(key.length, 24);
  key.copy(buf, 28);
  return buf;
}

function lp(b: Buffer): Buffer {
  const len = Buffer.allocUnsafe(4);
  len.writeUInt32BE(b.length >>> 0, 0);
  return Buffer.concat([len, b]);
}

function u32(value: number): Buffer {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32BE(value >>> 0, 0);
  return b;
}

function flag(relaxed: boolean): Buffer {
  return Buffer.from([relaxed ? FLAG_RELAXED : 0]);
}

function boolByte(v: boolean): Buffer {
  return Buffer.from([v ? 1 : 0]);
}

function utf8(s: string): Buffer {
  return Buffer.from(s, "utf8");
}

/**
 * DocPut payload: [collection][doc_id][body][flags]. `body` is already-
 * serialized JSON (the codec treats it as opaque bytes).
 */
export function encodeDocPut(
  collection: string,
  docId: Buffer,
  body: Buffer,
  relaxed: boolean,
): Buffer {
  return Buffer.concat([lp(utf8(collection)), lp(docId), lp(body), flag(relaxed)]);
}

/** DocDel payload: [collection][doc_id]. */
export function encodeDocDel(collection: string, docId: Buffer): Buffer {
  return Buffer.concat([lp(utf8(collection)), lp(docId)]);
}

/** IndexDef payload: [collection][index][unique u8][field_count u32]{[field]}. */
export function encodeIndexDef(
  collection: string,
  index: string,
  fields: string[],
  unique: boolean,
): Buffer {
  const parts = [lp(utf8(collection)), lp(utf8(index)), boolByte(unique), u32(fields.length)];
  for (const f of fields) parts.push(lp(utf8(f)));
  return Buffer.concat(parts);
}

/** By-id Query payload: [mode][collection][doc_id]. */
export function encodeQueryById(collection: string, docId: Buffer): Buffer {
  return Buffer.concat([Buffer.from([QUERY_BY_ID]), lp(utf8(collection)), lp(docId)]);
}

/**
 * Index-range Query payload: [mode][collection][index][limit u32][lo][hi][cursor].
 * `lo`/`hi` are JSON-array bound bytes (empty = unbounded); `cursor` is an
 * opaque page token (empty = first page).
 */
export function encodeQueryIndexRange(
  collection: string,
  index: string,
  lo: Buffer,
  hi: Buffer,
  cursor: Buffer,
  limit: number,
): Buffer {
  return Buffer.concat([
    Buffer.from([QUERY_INDEX_RANGE]),
    lp(utf8(collection)),
    lp(utf8(index)),
    u32(limit),
    lp(lo),
    lp(hi),
    lp(cursor),
  ]);
}

/** One ordering term: a dotted field path and its direction. */
export interface SortKey {
  field: string;
  ascending: boolean;
}

/** Field selection. `mode` is Proj.None/Include/Exclude. */
export interface Projection {
  mode: number;
  fields: string[];
}

/** Find payload. `filter` is opaque JSON bytes (empty = match all). */
export function encodeFind(
  collection: string,
  filter: Buffer,
  sort: SortKey[],
  projection: Projection,
  skip: number,
  limit: number,
  cursor: Buffer,
): Buffer {
  const parts = [lp(utf8(collection)), lp(filter), u32(sort.length)];
  for (const s of sort) {
    parts.push(lp(utf8(s.field)), boolByte(s.ascending));
  }
  if (projection.mode === Proj.Include || projection.mode === Proj.Exclude) {
    parts.push(Buffer.from([projection.mode]), u32(projection.fields.length));
    for (const f of projection.fields) parts.push(lp(utf8(f)));
  } else {
    parts.push(Buffer.from([Proj.None]));
  }
  parts.push(u32(skip), u32(limit), lp(cursor));
  return Buffer.concat(parts);
}

/** Update payload: [collection][filter][update][multi u8][flags]. */
export function encodeUpdate(
  collection: string,
  filter: Buffer,
  update: Buffer,
  multi: boolean,
  relaxed: boolean,
): Buffer {
  return Buffer.concat([
    lp(utf8(collection)),
    lp(filter),
    lp(update),
    boolByte(multi),
    flag(relaxed),
  ]);
}

/** Filter-based Delete payload: [collection][filter][multi u8][flags]. */
export function encodeDelete(
  collection: string,
  filter: Buffer,
  multi: boolean,
  relaxed: boolean,
): Buffer {
  return Buffer.concat([lp(utf8(collection)), lp(filter), boolByte(multi), flag(relaxed)]);
}

/** Count payload: [mode][collection][filter]. */
export function encodeCount(collection: string, filter: Buffer): Buffer {
  return Buffer.concat([Buffer.from([COUNT_MODE_COUNT]), lp(utf8(collection)), lp(filter)]);
}

/** Distinct payload: [mode][collection][filter][field]. */
export function encodeDistinct(collection: string, filter: Buffer, field: string): Buffer {
  return Buffer.concat([
    Buffer.from([COUNT_MODE_DISTINCT]),
    lp(utf8(collection)),
    lp(filter),
    lp(utf8(field)),
  ]);
}

/** One decoded row from a query/find response page. */
export interface Row {
  docId: Buffer;
  body: Buffer;
}

/** A decoded response page. `cursor` is null when there are no more pages. */
export interface Page {
  rows: Row[];
  cursor: Buffer | null;
}

/** Decode a response page: [row_count u32]{[doc_id][body]}[cursor]. */
export function decodePage(buf: Buffer): Page {
  let off = 0;
  const need = (n: number): void => {
    if (off + n > buf.length) throw new Error("zydecodb: payload truncated");
  };
  need(4);
  const count = buf.readUInt32BE(off);
  off += 4;
  const rows: Row[] = [];
  for (let i = 0; i < count; i++) {
    need(4);
    const klen = buf.readUInt32BE(off);
    off += 4;
    need(klen);
    const docId = buf.subarray(off, off + klen);
    off += klen;
    need(4);
    const blen = buf.readUInt32BE(off);
    off += 4;
    need(blen);
    const body = buf.subarray(off, off + blen);
    off += blen;
    rows.push({ docId, body });
  }
  need(4);
  const clen = buf.readUInt32BE(off);
  off += 4;
  need(clen);
  const cursorBytes = buf.subarray(off, off + clen);
  return { rows, cursor: cursorBytes.length === 0 ? null : cursorBytes };
}
