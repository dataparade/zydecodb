"""Binary wire protocol: command codes, status codes, and payload codecs.

This module is pure encoding/decoding with no I/O, so it can be unit-tested in
isolation and reused by the connection layer. It mirrors the Rust definitions in
`crates/zydecodb-engine/src/frame.rs` and `crates/zydecodb-document/src/wire.rs`.
"""

from __future__ import annotations

import json
import struct
from typing import Any, List, Optional, Tuple

PROTO_VERSION = 0x01
HEADER_LEN = 6

# --- commands ---
CMD_PUT = 0x01
CMD_GET = 0x02
CMD_DEL = 0x03
CMD_BEGIN = 0x10
CMD_COMMIT = 0x11
CMD_ROLLBACK = 0x12
CMD_QUERY = 0x20
CMD_DOC_PUT = 0x21
CMD_DOC_DEL = 0x22
CMD_FIND = 0x23
CMD_UPDATE = 0x24
CMD_DELETE = 0x25
CMD_COUNT = 0x26
CMD_DOC_GET_REV = 0x27
CMD_FIND_REV = 0x28
CMD_DOC_PUT_IF_MATCH = 0x29
CMD_DOC_UPDATE_IF_MATCH = 0x2A
CMD_AGGREGATE = 0x2B
CMD_WATCH = 0x2C
CMD_INDEX_DEF = 0x30
CMD_SESSION_INIT = 0x40
CMD_PING = 0xF0
CMD_STATS = 0xF1

# --- query / count sub-commands ---
QUERY_BY_ID = 0x00
QUERY_INDEX_RANGE = 0x01
COUNT_MODE_COUNT = 0x00
COUNT_MODE_DISTINCT = 0x01

# --- projection modes ---
PROJ_NONE = 0x00
PROJ_INCLUDE = 0x01
PROJ_EXCLUDE = 0x02

# Bit 0 of the optional trailing flags byte on write payloads.
FLAG_RELAXED = 0x01
FLAG_UPSERT = 0x02

# --- watch stream frame kinds (first byte of Ok payloads) ---
WATCH_FRAME_ACK = 0x01
WATCH_FRAME_EVENT = 0x02
WATCH_FRAME_HEARTBEAT = 0x03

WATCH_OP_UPSERT = 0x01
WATCH_OP_DELETE = 0x02
STATUS_OK = 0x00
STATUS_NOT_FOUND = 0x01
STATUS_ERROR = 0x02
STATUS_CONFLICT = 0x03
STATUS_IO_ERROR = 0x04
STATUS_INVALID_KEY = 0x05
STATUS_INVALID_VALUE = 0x06
STATUS_ENGINE_BUSY = 0x07
STATUS_PROTOCOL_ERROR = 0x08
STATUS_POLICY_REJECTED = 0x09
STATUS_UNSUPPORTED_FORMAT = 0x0A
STATUS_UNAUTHORIZED = 0x0B
STATUS_FORBIDDEN = 0x0C

STATUS_NAMES = {
    STATUS_OK: "Ok",
    STATUS_NOT_FOUND: "NotFound",
    STATUS_ERROR: "Error",
    STATUS_CONFLICT: "Conflict",
    STATUS_IO_ERROR: "IoError",
    STATUS_INVALID_KEY: "InvalidKey",
    STATUS_INVALID_VALUE: "InvalidValue",
    STATUS_ENGINE_BUSY: "EngineBusy",
    STATUS_PROTOCOL_ERROR: "ProtocolError",
    STATUS_POLICY_REJECTED: "PolicyRejected",
    STATUS_UNSUPPORTED_FORMAT: "UnsupportedFormat",
    STATUS_UNAUTHORIZED: "Unauthorized",
    STATUS_FORBIDDEN: "Forbidden",
}


def encode_header(command: int, payload_len: int) -> bytes:
    return struct.pack(">BBI", PROTO_VERSION, command, payload_len)


def _lp(b: bytes) -> bytes:
    """Length-prefix a byte string with a u32 big-endian length."""
    return struct.pack(">I", len(b)) + b


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def encode_put(
    key: bytes,
    value: bytes,
    *,
    routing_key: bytes = b"\x00" * 16,
    txid: int = 0,
    expires_at: int = 0,
) -> bytes:
    return (
        routing_key[:16].ljust(16, b"\x00")
        + struct.pack(">QQII", txid, expires_at, len(key), len(value))
        + key
        + value
    )


def encode_key(
    key: bytes,
    *,
    routing_key: bytes = b"\x00" * 16,
    snapshot_seq: int = 0,
) -> bytes:
    return (
        routing_key[:16].ljust(16, b"\x00")
        + struct.pack(">QI", snapshot_seq, len(key))
        + key
    )


INDEX_DIR_TAG = 0x02


def encode_index_def(
    collection: str,
    index: str,
    fields: List[str],
    *,
    unique: bool,
    expire_after_seconds: int = 0,
    directions: Optional[List[bool]] = None,
) -> bytes:
    out = _lp(collection.encode()) + _lp(index.encode())
    out += bytes([1 if unique else 0])
    out += struct.pack(">I", len(fields))
    for field in fields:
        out += _lp(field.encode())
    dirs = list(directions) if directions is not None else []
    any_desc = len(dirs) == len(fields) and any(not d for d in dirs)
    if expire_after_seconds or any_desc:
        out += struct.pack(">Q", int(expire_after_seconds))
    if any_desc:
        out += bytes([INDEX_DIR_TAG])
        out += bytes([1 if d else 0 for d in dirs])
    return out


def encode_doc_put(
    collection: str,
    doc_id: str,
    document: Any,
    *,
    relaxed: bool,
    expires_at: int = 0,
) -> bytes:
    """DocPut payload: [collection][doc_id][body][flags][optional expires_at u64 BE].

    ``expires_at`` is absolute unix millis; ``0`` omits the trailer (never expires).
    """
    out = _lp(collection.encode()) + _lp(doc_id.encode()) + _lp(_json_bytes(document))
    out += bytes([FLAG_RELAXED if relaxed else 0])
    if expires_at:
        out += struct.pack(">Q", int(expires_at))
    return out


def encode_doc_put_if_match(
    collection: str,
    doc_id: str,
    document: Any,
    *,
    relaxed: bool,
    if_match: int,
    expires_at: int = 0,
) -> bytes:
    """Conditional replace: [collection][doc_id][body][flags][if_match][optional expires_at]."""
    out = _lp(collection.encode()) + _lp(doc_id.encode()) + _lp(_json_bytes(document))
    out += bytes([FLAG_RELAXED if relaxed else 0])
    out += struct.pack(">Q", int(if_match))
    if expires_at:
        out += struct.pack(">Q", int(expires_at))
    return out


def encode_doc_update_if_match(
    collection: str,
    doc_id: str,
    update_doc: Any,
    *,
    relaxed: bool,
    if_match: int,
) -> bytes:
    """Conditional by-id update: [collection][doc_id][update][flags][if_match]."""
    return (
        _lp(collection.encode())
        + _lp(doc_id.encode())
        + _lp(_json_bytes(update_doc))
        + bytes([FLAG_RELAXED if relaxed else 0])
        + struct.pack(">Q", int(if_match))
    )


def encode_doc_del(collection: str, doc_id: str) -> bytes:
    return _lp(collection.encode()) + _lp(doc_id.encode())


def encode_query_by_id(collection: str, doc_id: str) -> bytes:
    return bytes([QUERY_BY_ID]) + _lp(collection.encode()) + _lp(doc_id.encode())


def _encode_bound(bound: Any) -> bytes:
    if bound is None:
        return b""
    values = bound if isinstance(bound, list) else [bound]
    return _json_bytes(values)


def encode_query_index_range(
    collection: str,
    index: str,
    *,
    lo: Any,
    hi: Any,
    page_size: int,
    cursor: bytes,
    include_bodies: bool = True,
) -> bytes:
    out = bytes([QUERY_INDEX_RANGE])
    out += _lp(collection.encode()) + _lp(index.encode())
    out += struct.pack(">I", page_size)
    out += _lp(_encode_bound(lo)) + _lp(_encode_bound(hi)) + _lp(cursor)
    # Append-only trailer: omit when true so legacy vectors match.
    if not include_bodies:
        out += b"\x00"
    return out


def _filter_bytes(filt: Optional[dict]) -> bytes:
    return b"" if not filt else _json_bytes(filt)


def encode_find(
    collection: str,
    filt: Optional[dict],
    sort: Optional[List[Tuple[str, bool]]],
    projection: Optional[Tuple[int, List[str]]],
    skip: int,
    limit: int,
    cursor: bytes,
) -> bytes:
    out = _lp(collection.encode()) + _lp(_filter_bytes(filt))
    sort = sort or []
    out += struct.pack(">I", len(sort))
    for field, ascending in sort:
        out += _lp(field.encode()) + bytes([1 if ascending else 0])
    if projection is None:
        out += bytes([PROJ_NONE])
    else:
        mode, fields = projection
        out += bytes([mode]) + struct.pack(">I", len(fields))
        for field in fields:
            out += _lp(field.encode())
    out += struct.pack(">II", skip, limit) + _lp(cursor)
    return out


def encode_update(
    collection: str,
    filt: dict,
    update_doc: dict,
    *,
    multi: bool,
    relaxed: bool,
    upsert: bool = False,
) -> bytes:
    flags = 0
    if relaxed:
        flags |= FLAG_RELAXED
    if upsert:
        flags |= FLAG_UPSERT
    out = (
        _lp(collection.encode())
        + _lp(_filter_bytes(filt))
        + _lp(_json_bytes(update_doc))
        + bytes([1 if multi else 0])
        + bytes([flags])
    )
    return out


def encode_delete(collection: str, filt: dict, *, multi: bool, relaxed: bool) -> bytes:
    return (
        _lp(collection.encode())
        + _lp(_filter_bytes(filt))
        + bytes([1 if multi else 0])
        + bytes([FLAG_RELAXED if relaxed else 0])
    )


def encode_count(collection: str, filt: Optional[dict]) -> bytes:
    return bytes([COUNT_MODE_COUNT]) + _lp(collection.encode()) + _lp(_filter_bytes(filt))


def encode_distinct(collection: str, field: str, filt: Optional[dict]) -> bytes:
    return (
        bytes([COUNT_MODE_DISTINCT])
        + _lp(collection.encode())
        + _lp(_filter_bytes(filt))
        + _lp(field.encode())
    )


def encode_aggregate(collection: str, pipeline: list) -> bytes:
    """Aggregate request: [collection lp][pipeline_json lp]."""
    return _lp(collection.encode()) + _lp(_json_bytes(pipeline))


def encode_watch(collection: str, resume_token: bytes = b"") -> bytes:
    """Watch request: [collection lp][resume_token lp]."""
    return _lp(collection.encode()) + _lp(resume_token)


def decode_watch_frame(buf: bytes) -> Tuple[str, bytes, Optional[int], Optional[bytes], Optional[bytes]]:
    """Decode one Watch stream frame.

    Returns ``(kind, resume_token, op, doc_id, body)`` where *kind* is
    ``"ack"``, ``"event"``, or ``"heartbeat"``; *op*/*doc_id*/*body* are set
    only for events.
    """
    if not buf:
        raise ValueError("empty watch frame")
    kind_byte = buf[0]
    off = 1
    (tlen,) = struct.unpack_from(">I", buf, off)
    off += 4
    resume_token = buf[off : off + tlen]
    off += tlen
    if kind_byte == WATCH_FRAME_ACK:
        return "ack", resume_token, None, None, None
    if kind_byte == WATCH_FRAME_HEARTBEAT:
        return "heartbeat", resume_token, None, None, None
    if kind_byte == WATCH_FRAME_EVENT:
        op = buf[off]
        off += 1
        (idlen,) = struct.unpack_from(">I", buf, off)
        off += 4
        doc_id = buf[off : off + idlen]
        off += idlen
        (blen,) = struct.unpack_from(">I", buf, off)
        off += 4
        body = buf[off : off + blen]
        return "event", resume_token, op, doc_id, body
    raise ValueError(f"unknown watch frame 0x{kind_byte:02x}")


def decode_aggregate_response(buf: bytes) -> List[bytes]:
    """Decode Aggregate response into raw row JSON byte strings."""
    off = 0
    (count,) = struct.unpack_from(">I", buf, off)
    off += 4
    rows: List[bytes] = []
    for _ in range(count):
        (n,) = struct.unpack_from(">I", buf, off)
        off += 4
        rows.append(buf[off : off + n])
        off += n
    return rows


def decode_page(buf: bytes) -> Tuple[List[Tuple[bytes, bytes]], bytes]:
    """Decode a query/find response page into `(rows, next_cursor)`.

    Each row is `(doc_id, body)`; an empty `next_cursor` means no more pages.
    """
    rows, cursor = decode_page_with_revision(buf, with_revision=False)
    return [(doc_id, body) for doc_id, body, _rev in rows], cursor


def decode_page_with_revision(
    buf: bytes, *, with_revision: bool = True
) -> Tuple[List[Tuple[bytes, bytes, Optional[int]]], bytes]:
    """Decode a FindRev page into `((doc_id, body, revision), next_cursor)`.

    When ``with_revision`` is False, ``revision`` is always ``None``.
    """
    off = 0
    (count,) = struct.unpack_from(">I", buf, off)
    off += 4
    rows: List[Tuple[bytes, bytes, Optional[int]]] = []
    for _ in range(count):
        (klen,) = struct.unpack_from(">I", buf, off)
        off += 4
        doc_id = buf[off : off + klen]
        off += klen
        (blen,) = struct.unpack_from(">I", buf, off)
        off += 4
        body = buf[off : off + blen]
        off += blen
        revision: Optional[int] = None
        if with_revision:
            (revision,) = struct.unpack_from(">Q", buf, off)
            off += 8
        rows.append((doc_id, body, revision))
    (clen,) = struct.unpack_from(">I", buf, off)
    off += 4
    cursor = buf[off : off + clen]
    return rows, cursor


def decode_doc_get_rev_response(buf: bytes) -> Tuple[bytes, int]:
    """Decode a DocGetRev response: `(body, revision)`."""
    (blen,) = struct.unpack_from(">I", buf, 0)
    body = buf[4 : 4 + blen]
    (revision,) = struct.unpack_from(">Q", buf, 4 + blen)
    return body, revision


def decode_begin_response(buf: bytes) -> Tuple[int, int]:
    """Decode Begin response: `(tx_id, snapshot_seq)`."""
    if len(buf) != 16:
        raise ValueError("Begin response must be 16 bytes")
    tx_id, snapshot_seq = struct.unpack(">QQ", buf)
    return tx_id, snapshot_seq


def decode_commit_response(buf: bytes) -> int:
    """Decode Commit response sequence number."""
    if len(buf) != 8:
        raise ValueError("Commit response must be 8 bytes")
    (seq,) = struct.unpack(">Q", buf)
    return seq


def decode_stage_ack(buf: bytes) -> Tuple[int, int]:
    """Decode stage ack: `(logical_ops, estimated_keys)`."""
    if len(buf) != 8:
        raise ValueError("stage ack must be 8 bytes")
    return struct.unpack(">II", buf)


def status_name(status: int) -> str:
    return STATUS_NAMES.get(status, f"0x{status:02x}")
