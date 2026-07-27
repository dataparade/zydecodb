/**
 * Conformance: the TS codec must match the shared wire vectors byte-for-byte.
 * The vectors are generated from the Rust server encoders (the protocol
 * authority), so running the TS codec against them proves it cannot silently
 * drift. See clients/conformance/README.md.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import {
  Cmd,
  decodeDocGetRevResponse,
  decodeBeginResponse,
  decodeCommitResponse,
  decodeStageAck,
  decodeAggregateResponse,
  decodeWatchFrame,
  decodePage,
  decodePageWithRevision,
  encodeAggregate,
  encodeWatch,
  encodeCount,
  encodeDelete,
  encodeDistinct,
  encodeDocDel,
  encodeDocPut,
  encodeDocPutIfMatch,
  encodeDocUpdateIfMatch,
  encodeFind,
  encodeHeader,
  encodeIndexDef,
  encodeQueryById,
  encodeQueryIndexRange,
  encodeUpdate,
  encodePut,
  encodeKey,
  PROTO_VERSION,
  Proj,
  Status,
  type Projection,
  type SortKey,
} from "../src/protocol.ts";

interface ReqVector {
  name: string;
  kind: string;
  command: number;
  input: Record<string, unknown>;
  payload_hex: string;
  envelope_hex: string;
}

interface RespVector {
  name: string;
  kind: string;
  bytes_hex: string;
  decoded: {
    rows?: { doc_id: string; body_json: string; revision?: number }[];
    next_cursor_hex?: string | null;
    body_json?: string;
    revision?: number;
    tx_id?: number;
    snapshot_seq?: number;
    seq?: number;
    logical_ops?: number;
    estimated_keys?: number;
    rows_json?: string[];
    resume_token_hex?: string;
    op?: string;
    doc_id?: string;
    status?: number;
    status_name?: string;
    detail?: string;
  };
}

interface VectorFile {
  proto_version: number;
  commands: Record<string, number>;
  statuses: Record<string, number>;
  requests: ReqVector[];
  responses: RespVector[];
}

const vectorsPath = fileURLToPath(new URL("../../conformance/vectors.json", import.meta.url));
const vectors = JSON.parse(readFileSync(vectorsPath, "utf8")) as VectorFile;

// An opaque "*_json" field -> the bytes the codec must accept verbatim.
const optBytes = (s: string): Buffer => (s === "" ? Buffer.alloc(0) : Buffer.from(s, "utf8"));
const fromHex = (s: string): Buffer => Buffer.from(s, "hex");

function encodeRequest(v: ReqVector): Buffer {
  const i = v.input as Record<string, never>;
  switch (v.kind) {
    case "Put":
      return encodePut(fromHex(s(i, "key_hex")), fromHex(s(i, "value_hex")), n(i, "expires_at"));
    case "Get":
      return encodeKey(fromHex(s(i, "key_hex")));
    case "Del":
      return encodeKey(fromHex(s(i, "key_hex")));
    case "DocPut":
      return encodeDocPut(
        s(i, "collection"),
        Buffer.from(s(i, "doc_id"), "utf8"),
        optBytes(s(i, "body_json")),
        b(i, "relaxed"),
        (i["expires_at"] as number | undefined) ?? 0,
      );
    case "DocDel":
      return encodeDocDel(s(i, "collection"), Buffer.from(s(i, "doc_id"), "utf8"));
    case "IndexDef": {
      const dirs = i["directions"];
      return encodeIndexDef(
        s(i, "collection"),
        s(i, "index_name"),
        arr(i, "fields"),
        b(i, "unique"),
        (i["expire_after_seconds"] as number | undefined) ?? 0,
        Array.isArray(dirs) ? (dirs as boolean[]) : undefined,
      );
    }
    case "QueryById":
      return encodeQueryById(s(i, "collection"), Buffer.from(s(i, "doc_id"), "utf8"));
    case "QueryIndexRange":
      return encodeQueryIndexRange(
        s(i, "collection"),
        s(i, "index_name"),
        optBytes(s(i, "lo_json")),
        optBytes(s(i, "hi_json")),
        fromHex(s(i, "cursor_hex")),
        n(i, "limit"),
        i["include_bodies"] === undefined ? true : Boolean(i["include_bodies"]),
      );
    case "Find":
    case "FindRev": {
      const proj = i["projection"] as { mode: string; fields: string[] };
      const projection: Projection =
        proj.mode === "include"
          ? { mode: Proj.Include, fields: proj.fields }
          : proj.mode === "exclude"
            ? { mode: Proj.Exclude, fields: proj.fields }
            : { mode: Proj.None, fields: [] };
      const sort: SortKey[] = (i["sort"] as [string, boolean][]).map(([field, ascending]) => ({
        field,
        ascending,
      }));
      return encodeFind(
        s(i, "collection"),
        optBytes(s(i, "filter_json")),
        sort,
        projection,
        n(i, "skip"),
        n(i, "limit"),
        fromHex(s(i, "cursor_hex")),
      );
    }
    case "DocGetRev":
      return encodeQueryById(s(i, "collection"), Buffer.from(s(i, "doc_id"), "utf8"));
    case "DocPutIfMatch":
      return encodeDocPutIfMatch(
        s(i, "collection"),
        Buffer.from(s(i, "doc_id"), "utf8"),
        optBytes(s(i, "body_json")),
        b(i, "relaxed"),
        BigInt(n(i, "if_match")),
        (i["expires_at"] as number | undefined) ?? 0,
      );
    case "DocUpdateIfMatch":
      return encodeDocUpdateIfMatch(
        s(i, "collection"),
        Buffer.from(s(i, "doc_id"), "utf8"),
        optBytes(s(i, "update_json")),
        b(i, "relaxed"),
        BigInt(n(i, "if_match")),
      );
    case "Begin":
    case "Commit":
    case "Rollback":
      return Buffer.alloc(0);
    case "Update":
      return encodeUpdate(
        s(i, "collection"),
        optBytes(s(i, "filter_json")),
        optBytes(s(i, "update_json")),
        b(i, "multi"),
        b(i, "relaxed"),
        Boolean(i["upsert"]),
      );
    case "Delete":
      return encodeDelete(s(i, "collection"), optBytes(s(i, "filter_json")), b(i, "multi"), b(i, "relaxed"));
    case "Count":
      return encodeCount(s(i, "collection"), optBytes(s(i, "filter_json")));
    case "Distinct":
      return encodeDistinct(s(i, "collection"), optBytes(s(i, "filter_json")), s(i, "field"));
    case "Aggregate":
      return encodeAggregate(s(i, "collection"), optBytes(s(i, "pipeline_json")));
    case "Watch":
      return encodeWatch(s(i, "collection"), fromHex(s(i, "resume_token_hex")));
    case "SessionInit":
      return Buffer.from(s(i, "api_key"), "utf8");
    case "Ping":
    case "Stats":
    case "SchemaDef":
      return Buffer.alloc(0);
    case "SetContext":
      return fromHex(s(i, "tenant_hex"));
    case "AdminDropTenant": {
      const tenant = fromHex(s(i, "tenant_hex"));
      return Buffer.concat([tenant, Buffer.from([i["compact"] ? 1 : 0])]);
    }
    default:
      throw new Error(`unhandled request kind: ${v.kind}`);
  }
}

// Typed accessors for the language-neutral input objects.
const s = (o: Record<string, never>, k: string): string => o[k] as unknown as string;
const n = (o: Record<string, never>, k: string): number => o[k] as unknown as number;
const b = (o: Record<string, never>, k: string): boolean => o[k] as unknown as boolean;
const arr = (o: Record<string, never>, k: string): string[] => o[k] as unknown as string[];

test("proto version matches", () => {
  assert.equal(vectors.proto_version, PROTO_VERSION);
});

for (const v of vectors.requests) {
  test(`request: ${v.name}`, () => {
    const payload = encodeRequest(v);
    assert.equal(payload.toString("hex"), v.payload_hex, "payload");
    const envelope = Buffer.concat([encodeHeader(v.command, payload.length), payload]);
    assert.equal(envelope.toString("hex"), v.envelope_hex, "envelope");
  });
}

for (const v of vectors.responses) {
  test(`response: ${v.name}`, () => {
    if (v.kind === "QueryPage") {
      const page = decodePage(fromHex(v.bytes_hex));
      assert.equal(page.rows.length, v.decoded.rows!.length);
      page.rows.forEach((row, idx) => {
        const exp = v.decoded.rows![idx]!;
        assert.equal(row.docId.toString("utf8"), exp.doc_id);
        assert.equal(row.body.toString("utf8"), exp.body_json);
      });
      if (v.decoded.next_cursor_hex === null) {
        assert.equal(page.cursor, null);
      } else {
        assert.equal(page.cursor?.toString("hex"), v.decoded.next_cursor_hex);
      }
      return;
    }
    if (v.kind === "QueryPageRev") {
      const page = decodePageWithRevision(fromHex(v.bytes_hex));
      assert.equal(page.rows.length, v.decoded.rows!.length);
      page.rows.forEach((row, idx) => {
        const exp = v.decoded.rows![idx]!;
        assert.equal(row.docId.toString("utf8"), exp.doc_id);
        assert.equal(row.body.toString("utf8"), exp.body_json);
        assert.equal(row.revision, BigInt(exp.revision!));
      });
      assert.equal(page.cursor, null);
      return;
    }
    if (v.kind === "DocGetRevResponse") {
      const got = decodeDocGetRevResponse(fromHex(v.bytes_hex));
      assert.equal(got.body.toString("utf8"), v.decoded.body_json);
      assert.equal(got.revision, BigInt(v.decoded.revision!));
      return;
    }
    if (v.kind === "BeginResponse") {
      const got = decodeBeginResponse(fromHex(v.bytes_hex));
      assert.equal(got.txId, BigInt(v.decoded.tx_id!));
      assert.equal(got.snapshotSeq, BigInt(v.decoded.snapshot_seq!));
      return;
    }
    if (v.kind === "CommitResponse") {
      const seq = decodeCommitResponse(fromHex(v.bytes_hex));
      assert.equal(seq, BigInt(v.decoded.seq!));
      return;
    }
    if (v.kind === "StageAck") {
      const got = decodeStageAck(fromHex(v.bytes_hex));
      assert.equal(got.logicalOps, v.decoded.logical_ops);
      assert.equal(got.estimatedKeys, v.decoded.estimated_keys);
      return;
    }
    if (v.kind === "AggregateResponse") {
      const rows = decodeAggregateResponse(fromHex(v.bytes_hex));
      assert.equal(rows.length, v.decoded.rows_json!.length);
      rows.forEach((row, idx) => {
        assert.equal(row.toString("utf8"), v.decoded.rows_json![idx]);
      });
      return;
    }
    if (v.kind === "WatchFrameAck" || v.kind === "WatchFrameHeartbeat") {
      const frame = decodeWatchFrame(fromHex(v.bytes_hex));
      const wantKind = v.kind === "WatchFrameAck" ? "ack" : "heartbeat";
      assert.equal(frame.kind, wantKind);
      assert.equal(frame.resumeToken.toString("hex"), v.decoded.resume_token_hex);
      return;
    }
    if (v.kind === "WatchFrameEvent") {
      const frame = decodeWatchFrame(fromHex(v.bytes_hex));
      assert.equal(frame.kind, "event");
      assert.equal(frame.resumeToken.toString("hex"), v.decoded.resume_token_hex);
      assert.equal(frame.docId?.toString("utf8"), v.decoded.doc_id);
      assert.equal(frame.body?.toString("utf8") ?? "", v.decoded.body_json);
      const wantOp = v.decoded.op === "upsert" ? 0x01 : 0x02;
      assert.equal(frame.op, wantOp);
      return;
    }
    if (v.kind === "StatusResponse") {
      const raw = fromHex(v.bytes_hex);
      assert.equal(raw[0], PROTO_VERSION);
      const status = raw[1]!;
      const plen = raw.readUInt32BE(2);
      const detail = raw.subarray(6, 6 + plen).toString("utf8");
      assert.equal(status, v.decoded.status);
      assert.equal(detail, v.decoded.detail ?? "");
      return;
    }
    throw new Error(`unhandled response kind: ${v.kind}`);
  });
}

test("command and status codes match vectors", () => {
  assert.equal(vectors.commands["DocPut"], Cmd.DocPut);
  assert.equal(vectors.commands["Find"], Cmd.Find);
  assert.equal(vectors.commands["Update"], Cmd.Update);
  assert.equal(vectors.commands["Delete"], Cmd.Delete);
  assert.equal(vectors.commands["Count"], Cmd.Count);
  assert.equal(vectors.commands["DocGetRev"], Cmd.DocGetRev);
  assert.equal(vectors.commands["FindRev"], Cmd.FindRev);
  assert.equal(vectors.commands["DocPutIfMatch"], Cmd.DocPutIfMatch);
  assert.equal(vectors.commands["DocUpdateIfMatch"], Cmd.DocUpdateIfMatch);
  assert.equal(vectors.commands["Aggregate"], Cmd.Aggregate);
  assert.equal(vectors.commands["Watch"], Cmd.Watch);
  assert.equal(vectors.commands["Begin"], Cmd.Begin);
  assert.equal(vectors.commands["Commit"], Cmd.Commit);
  assert.equal(vectors.commands["Rollback"], Cmd.Rollback);
  assert.equal(vectors.commands["IndexDef"], Cmd.IndexDef);
  assert.equal(vectors.commands["SessionInit"], Cmd.SessionInit);
  assert.equal(vectors.statuses["Ok"], Status.Ok);
  assert.equal(vectors.statuses["EngineBusy"], Status.EngineBusy);
  assert.equal(vectors.statuses["PolicyRejected"], Status.PolicyRejected);
  assert.equal(vectors.statuses["UnsupportedFormat"], Status.UnsupportedFormat);
  assert.equal(vectors.statuses["Unauthorized"], Status.Unauthorized);
  assert.equal(vectors.statuses["Forbidden"], Status.Forbidden);
});
