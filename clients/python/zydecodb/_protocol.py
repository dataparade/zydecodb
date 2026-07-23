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
CMD_QUERY = 0x20
CMD_DOC_PUT = 0x21
CMD_DOC_DEL = 0x22
CMD_FIND = 0x23
CMD_UPDATE = 0x24
CMD_DELETE = 0x25
CMD_COUNT = 0x26
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

# --- status codes (response header byte 1) ---
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


def encode_index_def(
    collection: str, index: str, fields: List[str], *, unique: bool
) -> bytes:
    out = _lp(collection.encode()) + _lp(index.encode())
    out += bytes([1 if unique else 0])
    out += struct.pack(">I", len(fields))
    for field in fields:
        out += _lp(field.encode())
    return out


def encode_doc_put(collection: str, doc_id: str, document: Any, *, relaxed: bool) -> bytes:
    out = _lp(collection.encode()) + _lp(doc_id.encode()) + _lp(_json_bytes(document))
    out += bytes([FLAG_RELAXED if relaxed else 0])
    return out


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
) -> bytes:
    out = bytes([QUERY_INDEX_RANGE])
    out += _lp(collection.encode()) + _lp(index.encode())
    out += struct.pack(">I", page_size)
    out += _lp(_encode_bound(lo)) + _lp(_encode_bound(hi)) + _lp(cursor)
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


def decode_page(buf: bytes) -> Tuple[List[Tuple[bytes, bytes]], bytes]:
    """Decode a query/find response page into `(rows, next_cursor)`.

    Each row is `(doc_id, body)`; an empty `next_cursor` means no more pages.
    """
    off = 0
    (count,) = struct.unpack_from(">I", buf, off)
    off += 4
    rows: List[Tuple[bytes, bytes]] = []
    for _ in range(count):
        (klen,) = struct.unpack_from(">I", buf, off)
        off += 4
        doc_id = buf[off : off + klen]
        off += klen
        (blen,) = struct.unpack_from(">I", buf, off)
        off += 4
        body = buf[off : off + blen]
        off += blen
        rows.append((doc_id, body))
    (clen,) = struct.unpack_from(">I", buf, off)
    off += 4
    cursor = buf[off : off + clen]
    return rows, cursor


def status_name(status: int) -> str:
    return STATUS_NAMES.get(status, f"0x{status:02x}")
