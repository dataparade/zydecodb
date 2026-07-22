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
        )
    if kind == "DocDel":
        return proto.encode_doc_del(inp["collection"], inp["doc_id"])
    if kind == "IndexDef":
        return proto.encode_index_def(
            inp["collection"], inp["index_name"], inp["fields"], unique=inp["unique"]
        )
    if kind == "QueryById":
        return proto.encode_query_by_id(inp["collection"], inp["doc_id"])
    if kind == "QueryIndexRange":
        return proto.encode_query_index_range(
            inp["collection"], inp["index_name"],
            lo=_json_field(inp["lo_json"]), hi=_json_field(inp["hi_json"]),
            page_size=inp["limit"], cursor=bytes.fromhex(inp["cursor_hex"]),
        )
    if kind == "Find":
        proj = inp["projection"]
        mode = {"none": None, "include": proto.PROJ_INCLUDE, "exclude": proto.PROJ_EXCLUDE}[proj["mode"]]
        projection = None if mode is None else (mode, proj["fields"])
        return proto.encode_find(
            inp["collection"], _json_field(inp["filter_json"]),
            [tuple(s) for s in inp["sort"]], projection,
            inp["skip"], inp["limit"], bytes.fromhex(inp["cursor_hex"]),
        )
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
    if kind == "SessionInit":
        return inp["api_key"].encode("utf-8")
    if kind == "Ping":
        return b""
    raise AssertionError(f"unhandled request kind: {kind}")


@pytest.mark.parametrize("vec", VECTORS["requests"], ids=lambda v: v["name"])
def test_request_payload_matches(vec):
    payload = _encode_request(vec["kind"], vec["input"])
    assert payload.hex() == vec["payload_hex"], vec["name"]
    envelope = proto.encode_header(vec["command"], len(payload)) + payload
    assert envelope.hex() == vec["envelope_hex"], vec["name"]


@pytest.mark.parametrize("vec", VECTORS["responses"], ids=lambda v: v["name"])
def test_response_decode_matches(vec):
    assert vec["kind"] == "QueryPage"
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


def test_command_codes_match_vectors():
    cmds = VECTORS["commands"]
    assert proto.CMD_DOC_PUT == cmds["DocPut"]
    assert proto.CMD_FIND == cmds["Find"]
    assert proto.CMD_UPDATE == cmds["Update"]
    assert proto.CMD_DELETE == cmds["Delete"]
    assert proto.CMD_COUNT == cmds["Count"]
    assert proto.CMD_INDEX_DEF == cmds["IndexDef"]
    assert proto.CMD_SESSION_INIT == cmds["SessionInit"]


def test_status_codes_match_vectors():
    st = VECTORS["statuses"]
    assert proto.STATUS_OK == st["Ok"]
    assert proto.STATUS_ENGINE_BUSY == st["EngineBusy"]
    assert proto.STATUS_UNAUTHORIZED == st["Unauthorized"]
    assert proto.STATUS_FORBIDDEN == st["Forbidden"]
