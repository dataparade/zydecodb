"""Server-independent unit tests for the wire codecs and error taxonomy."""

import struct

import pytest

from zydecodb import (
    AuthError,
    ConflictError,
    InvalidRequestError,
    PolicyError,
    ServerBusyError,
    ServerError,
    UnsupportedFormatError,
)
from zydecodb import _protocol as proto
from zydecodb.collection import _encode_projection
from zydecodb.errors import from_status


def _encode_page(rows, cursor=b""):
    out = struct.pack(">I", len(rows))
    for doc_id, body in rows:
        out += struct.pack(">I", len(doc_id)) + doc_id
        out += struct.pack(">I", len(body)) + body
    out += struct.pack(">I", len(cursor)) + cursor
    return out


def test_decode_page_roundtrip():
    rows = [(b"u1", b'{"a":1}'), (b"u2", b"")]
    buf = _encode_page(rows, cursor=b"NEXT")
    decoded_rows, cursor = proto.decode_page(buf)
    assert decoded_rows == rows
    assert cursor == b"NEXT"


def test_decode_empty_page():
    decoded_rows, cursor = proto.decode_page(_encode_page([]))
    assert decoded_rows == []
    assert cursor == b""


def test_doc_put_carries_relaxed_flag():
    durable = proto.encode_doc_put("c", "id", {"x": 1}, relaxed=False)
    relaxed = proto.encode_doc_put("c", "id", {"x": 1}, relaxed=True)
    assert durable[-1] == 0
    assert relaxed[-1] == proto.FLAG_RELAXED


def test_update_and_delete_trailing_flags():
    upd = proto.encode_update("c", {"_id": "x"}, {"$set": {"n": 1}}, multi=True, relaxed=True)
    # ...multi byte, then relaxed flag byte.
    assert upd[-2] == 1 and upd[-1] == proto.FLAG_RELAXED
    dele = proto.encode_delete("c", {"a": 1}, multi=False, relaxed=False)
    assert dele[-2] == 0 and dele[-1] == 0


@pytest.mark.parametrize(
    "status,exc",
    [
        (proto.STATUS_CONFLICT, ConflictError),
        (proto.STATUS_UNAUTHORIZED, AuthError),
        (proto.STATUS_FORBIDDEN, AuthError),
        (proto.STATUS_ENGINE_BUSY, ServerBusyError),
        (proto.STATUS_PROTOCOL_ERROR, InvalidRequestError),
        (proto.STATUS_INVALID_KEY, InvalidRequestError),
        (proto.STATUS_POLICY_REJECTED, PolicyError),
        (proto.STATUS_UNSUPPORTED_FORMAT, UnsupportedFormatError),
        (proto.STATUS_ERROR, ServerError),
    ],
)
def test_status_maps_to_exception(status, exc):
    err = from_status(status, "Op", b"detail")
    assert isinstance(err, exc)
    assert err.status == status
    assert "Op failed" in str(err)


def test_projection_encoding():
    assert _encode_projection(None) is None
    assert _encode_projection({"name": 1}) == (proto.PROJ_INCLUDE, ["name"])
    assert _encode_projection({"secret": 0}) == (proto.PROJ_EXCLUDE, ["secret"])


def test_projection_rejects_mixed():
    with pytest.raises(Exception):
        _encode_projection({"a": 1, "b": 0})
