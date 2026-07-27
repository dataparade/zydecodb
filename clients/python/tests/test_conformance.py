"""Conformance: the Python codec must match the shared wire vectors byte-for-byte.

The vectors in `clients/conformance/vectors.json` are generated from the Rust
server encoders (the protocol authority). Running the Python codec against them
proves it cannot silently drift from the server. See `clients/conformance/README.md`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from zydecodb import _protocol as proto

VECTORS_PATH = Path(__file__).resolve().parents[2] / "conformance" / "vectors.json"


def _load():
    with VECTORS_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


VECTORS = _load()


def _json_field(s: str):
    """An opaque pre-serialized JSON field -> the object Python re-serializes to
    the same bytes (empty string means "absent")."""
    return None if s == "" else json.loads(s)


def _encode_request(kind: str, inp: dict) -> bytes:
    if kind == "Put":
        return proto.encode_put(
            bytes.fromhex(inp["key_hex"]),
            bytes.fromhex(inp["value_hex"]),
            expires_at=inp["expires_at"],
        )
    if kind == "Get":
        return proto.encode_key(bytes.fromhex(inp["key_hex"]))
    if kind == "Del":
        return proto.encode_key(bytes.fromhex(inp["key_hex"]))
    if kind == "DocPut":
        return proto.encode_doc_put(
            inp["collection"], inp["doc_id"], _json_field(inp["body_json"]),
            relaxed=inp["relaxed"],
            expires_at=inp.get("expires_at", 0),
        )
    if kind == "DocDel":
        return proto.encode_doc_del(inp["collection"], inp["doc_id"])
    if kind == "IndexDef":
        return proto.encode_index_def(
            inp["collection"], inp["index_name"], inp["fields"], unique=inp["unique"],
            expire_after_seconds=inp.get("expire_after_seconds", 0),
            directions=inp.get("directions"),
        )
    if kind == "QueryById":
        return proto.encode_query_by_id(inp["collection"], inp["doc_id"])
    if kind == "QueryIndexRange":
        return proto.encode_query_index_range(
            inp["collection"], inp["index_name"],
            lo=_json_field(inp["lo_json"]), hi=_json_field(inp["hi_json"]),
            page_size=inp["limit"], cursor=bytes.fromhex(inp["cursor_hex"]),
            include_bodies=inp.get("include_bodies", True),
        )
    if kind in ("Find", "FindRev"):
        proj = inp["projection"]
        mode = {"none": None, "include": proto.PROJ_INCLUDE, "exclude": proto.PROJ_EXCLUDE}[proj["mode"]]
        projection = None if mode is None else (mode, proj["fields"])
        return proto.encode_find(
            inp["collection"], _json_field(inp["filter_json"]),
            [tuple(s) for s in inp["sort"]], projection,
            inp["skip"], inp["limit"], bytes.fromhex(inp["cursor_hex"]),
        )
    if kind == "DocGetRev":
        return proto.encode_query_by_id(inp["collection"], inp["doc_id"])
    if kind == "DocPutIfMatch":
        return proto.encode_doc_put_if_match(
            inp["collection"],
            inp["doc_id"],
            _json_field(inp["body_json"]),
            relaxed=inp["relaxed"],
            if_match=inp["if_match"],
            expires_at=inp.get("expires_at", 0),
        )
    if kind == "DocUpdateIfMatch":
        return proto.encode_doc_update_if_match(
            inp["collection"],
            inp["doc_id"],
            _json_field(inp["update_json"]),
            relaxed=inp["relaxed"],
            if_match=inp["if_match"],
        )
    if kind in ("Begin", "Commit", "Rollback"):
        return b""
    if kind == "Update":
        return proto.encode_update(
            inp["collection"], _json_field(inp["filter_json"]),
            _json_field(inp["update_json"]), multi=inp["multi"], relaxed=inp["relaxed"],
            upsert=inp.get("upsert", False),
        )
    if kind == "Delete":
        return proto.encode_delete(
            inp["collection"], _json_field(inp["filter_json"]),
            multi=inp["multi"], relaxed=inp["relaxed"],
        )
    if kind == "Count":
        return proto.encode_count(inp["collection"], _json_field(inp["filter_json"]))
    if kind == "Distinct":
        return proto.encode_distinct(
            inp["collection"], inp["field"], _json_field(inp["filter_json"])
        )
    if kind == "Aggregate":
        return proto.encode_aggregate(
            inp["collection"], json.loads(inp["pipeline_json"])
        )
    if kind == "Watch":
        token_hex = inp.get("resume_token_hex", "")
        resume = bytes.fromhex(token_hex) if token_hex else b""
        return proto.encode_watch(inp["collection"], resume)
    if kind == "SessionInit":
        return inp["api_key"].encode("utf-8")
    if kind in ("Ping", "Stats", "SchemaDef", "Begin", "Commit", "Rollback"):
        return b""
    if kind == "SetContext":
        return bytes.fromhex(inp["tenant_hex"])
    if kind == "AdminDropTenant":
        return bytes.fromhex(inp["tenant_hex"]) + bytes([1 if inp.get("compact") else 0])
    raise AssertionError(f"unhandled request kind: {kind}")


@pytest.mark.parametrize("vec", VECTORS["requests"], ids=lambda v: v["name"])
def test_request_payload_matches(vec):
    payload = _encode_request(vec["kind"], vec["input"])
    assert payload.hex() == vec["payload_hex"], vec["name"]
    envelope = proto.encode_header(vec["command"], len(payload)) + payload
    assert envelope.hex() == vec["envelope_hex"], vec["name"]


@pytest.mark.parametrize("vec", VECTORS["responses"], ids=lambda v: v["name"])
def test_response_decode_matches(vec):
    kind = vec["kind"]
    if kind == "QueryPage":
        rows, cursor = proto.decode_page(bytes.fromhex(vec["bytes_hex"]))
        expected_rows = vec["decoded"]["rows"]
        assert len(rows) == len(expected_rows), vec["name"]
        for (doc_id, body), exp in zip(rows, expected_rows):
            assert doc_id.decode("utf-8") == exp["doc_id"]
            assert body.decode("utf-8") == exp["body_json"]
        expected_cursor = vec["decoded"]["next_cursor_hex"]
        if expected_cursor is None:
            assert cursor == b""
        else:
            assert cursor.hex() == expected_cursor
        return
    if kind == "QueryPageRev":
        rows, cursor = proto.decode_page_with_revision(
            bytes.fromhex(vec["bytes_hex"]), with_revision=True
        )
        expected_rows = vec["decoded"]["rows"]
        assert len(rows) == len(expected_rows), vec["name"]
        for (doc_id, body, rev), exp in zip(rows, expected_rows):
            assert doc_id.decode("utf-8") == exp["doc_id"]
            assert body.decode("utf-8") == exp["body_json"]
            assert rev == exp["revision"]
        assert cursor == b""
        return
    if kind == "DocGetRevResponse":
        body, rev = proto.decode_doc_get_rev_response(bytes.fromhex(vec["bytes_hex"]))
        assert body.decode("utf-8") == vec["decoded"]["body_json"]
        assert rev == vec["decoded"]["revision"]
        return
    if kind == "BeginResponse":
        tx_id, snap = proto.decode_begin_response(bytes.fromhex(vec["bytes_hex"]))
        assert tx_id == vec["decoded"]["tx_id"]
        assert snap == vec["decoded"]["snapshot_seq"]
        return
    if kind == "CommitResponse":
        seq = proto.decode_commit_response(bytes.fromhex(vec["bytes_hex"]))
        assert seq == vec["decoded"]["seq"]
        return
    if kind == "StageAck":
        ops, keys = proto.decode_stage_ack(bytes.fromhex(vec["bytes_hex"]))
        assert ops == vec["decoded"]["logical_ops"]
        assert keys == vec["decoded"]["estimated_keys"]
        return
    if kind == "AggregateResponse":
        rows = proto.decode_aggregate_response(bytes.fromhex(vec["bytes_hex"]))
        expected = vec["decoded"]["rows_json"]
        assert len(rows) == len(expected), vec["name"]
        for got, exp in zip(rows, expected):
            assert got.decode("utf-8") == exp
        return
    if kind in ("WatchFrameAck", "WatchFrameHeartbeat"):
        kind_name, token, op, doc_id, body = proto.decode_watch_frame(
            bytes.fromhex(vec["bytes_hex"])
        )
        expected_kind = {
            "WatchFrameAck": "ack",
            "WatchFrameHeartbeat": "heartbeat",
        }[kind]
        assert kind_name == expected_kind, vec["name"]
        assert token.hex() == vec["decoded"]["resume_token_hex"]
        assert op is None and doc_id is None and body is None
        return
    if kind == "WatchFrameEvent":
        kind_name, token, op, doc_id, body = proto.decode_watch_frame(
            bytes.fromhex(vec["bytes_hex"])
        )
        assert kind_name == "event", vec["name"]
        assert token.hex() == vec["decoded"]["resume_token_hex"]
        expected_op = {
            "upsert": proto.WATCH_OP_UPSERT,
            "delete": proto.WATCH_OP_DELETE,
        }[vec["decoded"]["op"]]
        assert op == expected_op
        assert doc_id.decode("utf-8") == vec["decoded"]["doc_id"]
        assert (body or b"").decode("utf-8") == vec["decoded"]["body_json"]
        return
    if kind == "StatusResponse":
        raw = bytes.fromhex(vec["bytes_hex"])
        assert raw[0] == proto.PROTO_VERSION
        status = raw[1]
        plen = int.from_bytes(raw[2:6], "big")
        detail = raw[6 : 6 + plen].decode("utf-8")
        assert status == vec["decoded"]["status"], vec["name"]
        assert detail == vec["decoded"]["detail"], vec["name"]
        return
    raise AssertionError(f"unhandled response kind: {kind}")


def test_command_codes_match_vectors():
    cmds = VECTORS["commands"]
    assert proto.CMD_DOC_PUT == cmds["DocPut"]
    assert proto.CMD_FIND == cmds["Find"]
    assert proto.CMD_UPDATE == cmds["Update"]
    assert proto.CMD_DELETE == cmds["Delete"]
    assert proto.CMD_COUNT == cmds["Count"]
    assert proto.CMD_DOC_GET_REV == cmds["DocGetRev"]
    assert proto.CMD_FIND_REV == cmds["FindRev"]
    assert proto.CMD_DOC_PUT_IF_MATCH == cmds["DocPutIfMatch"]
    assert proto.CMD_DOC_UPDATE_IF_MATCH == cmds["DocUpdateIfMatch"]
    assert proto.CMD_AGGREGATE == cmds["Aggregate"]
    assert proto.CMD_WATCH == cmds["Watch"]
    assert proto.CMD_BEGIN == cmds["Begin"]
    assert proto.CMD_COMMIT == cmds["Commit"]
    assert proto.CMD_ROLLBACK == cmds["Rollback"]
    assert proto.CMD_INDEX_DEF == cmds["IndexDef"]
    assert proto.CMD_SESSION_INIT == cmds["SessionInit"]


def test_status_codes_match_vectors():
    st = VECTORS["statuses"]
    assert proto.STATUS_OK == st["Ok"]
    assert proto.STATUS_ENGINE_BUSY == st["EngineBusy"]
    assert proto.STATUS_POLICY_REJECTED == st["PolicyRejected"]
    assert proto.STATUS_UNSUPPORTED_FORMAT == st["UnsupportedFormat"]
    assert proto.STATUS_UNAUTHORIZED == st["Unauthorized"]
    assert proto.STATUS_FORBIDDEN == st["Forbidden"]
