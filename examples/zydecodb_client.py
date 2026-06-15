#!/usr/bin/env python3
"""
Example ZydecoDB client + Mongo-inspired document driver (Python, stdlib only).

The product surface is the `Collection` API (see `ZydecoDBClient.collection`):
if you know MongoDB you already know it — `insert_one`, `find`, `update_one`,
`delete_many`, `count_documents`, `distinct`, with `$`-operators, sort,
projection, skip/limit, and automatic pagination. The binary wire protocol
stays hidden behind it.

The low-level key-value calls (put / get / delete) are still here too; the
document layer is built on top of them and they remain handy for things like
TTL session tokens.

Usage:
    python3 examples/zydecodb_client.py
    python3 examples/zydecodb_client.py --api-key "$ZYDECODB_API_KEY"

Start the server:
    cp config/zydecodb.dev.toml /tmp/zydecodb.toml
    ./target/release/zydecodb serve --config /tmp/zydecodb.toml

See examples/README.md and docs/SECURITY.md.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
import sys
import time
from typing import Any, Iterator, Optional

PROTO_VERSION = 0x01
HEADER_LEN = 6

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

# Query sub-command (first byte of a CMD_QUERY payload).
QUERY_BY_ID = 0x00
QUERY_INDEX_RANGE = 0x01

# Projection modes (CMD_FIND payload).
PROJ_NONE = 0x00
PROJ_INCLUDE = 0x01
PROJ_EXCLUDE = 0x02

# Count sub-command (first byte of a CMD_COUNT payload).
COUNT_MODE_COUNT = 0x00
COUNT_MODE_DISTINCT = 0x01

STATUS_OK = 0x00
STATUS_NOT_FOUND = 0x01
STATUS_CONFLICT = 0x03
STATUS_UNAUTHORIZED = 0x0B
STATUS_FORBIDDEN = 0x0C


def generate_id() -> str:
    """A time-ordered id (UUIDv7-style): 48-bit ms timestamp + 80 random bits,
    hex-encoded. Lexicographically sortable by creation time."""
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    return ts_ms.to_bytes(6, "big").hex() + os.urandom(10).hex()


def _lp(b: bytes) -> bytes:
    """Length-prefix a byte string with a u32 big-endian length."""
    return struct.pack(">I", len(b)) + b

STATUS_NAMES = {
    0x00: "Ok",
    0x01: "NotFound",
    0x02: "Error",
    0x03: "Conflict",
    0x04: "IoError",
    0x05: "InvalidKey",
    0x06: "InvalidValue",
    0x07: "EngineBusy",
    0x08: "ProtocolError",
    0x0B: "Unauthorized",
    0x0C: "Forbidden",
}


class ZydecoDBError(Exception):
    """A failed ZydecoDB operation. `status` is the wire status byte when the
    failure came from a server response (None for transport/client errors)."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class ZydecoDBClient:
    """TCP client for zydecodb serve."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9470,
        timeout: float = 5.0,
        api_key: Optional[str] = None,
    ):
        self._addr = (host, port)
        self._timeout = timeout
        self._api_key = api_key
        self._sock: Optional[socket.socket] = None

    def connect(self) -> None:
        self.close()
        sock = socket.create_connection(self._addr, timeout=self._timeout)
        sock.settimeout(self._timeout)
        self._sock = sock
        if self._api_key:
            self._session_init(self._api_key)

    def _session_init(self, api_key: str) -> None:
        self._send(CMD_SESSION_INIT, api_key.encode("utf-8"))
        status, payload = self._recv()
        if status == STATUS_UNAUTHORIZED:
            detail = payload.decode("utf-8", errors="replace")
            raise ZydecoDBError(f"authentication failed: {detail}")
        self._expect_ok(status, payload, "SessionInit")

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def __enter__(self) -> "ZydecoDBClient":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def _send(self, command: int, payload: bytes = b"") -> None:
        if self._sock is None:
            raise ZydecoDBError("not connected")
        header = struct.pack(">BBI", PROTO_VERSION, command, len(payload))
        self._sock.sendall(header + payload)

    def _recv_exact(self, n: int) -> bytes:
        if self._sock is None:
            raise ZydecoDBError("not connected")
        chunks: list[bytes] = []
        got = 0
        while got < n:
            piece = self._sock.recv(n - got)
            if not piece:
                raise ZydecoDBError("connection closed while reading response")
            chunks.append(piece)
            got += len(piece)
        return b"".join(chunks)

    def _recv(self) -> tuple[int, bytes]:
        header = self._recv_exact(HEADER_LEN)
        version, status, length = struct.unpack(">BBI", header)
        if version != PROTO_VERSION:
            raise ZydecoDBError(f"unexpected protocol version 0x{version:02x}")
        payload = self._recv_exact(length) if length else b""
        return status, payload

    @staticmethod
    def _encode_put(
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

    @staticmethod
    def _encode_key(
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

    def _expect_ok(self, status: int, payload: bytes, op: str) -> bytes:
        if status == STATUS_OK:
            return payload
        name = STATUS_NAMES.get(status, f"0x{status:02x}")
        detail = payload.decode("utf-8", errors="replace")
        raise ZydecoDBError(
            f"{op} failed: {name}" + (f" ({detail})" if detail else ""),
            status=status,
        )

    def ping(self) -> None:
        self._send(CMD_PING)
        self._expect_ok(*self._recv(), "PING")

    def put(self, key: bytes, value: bytes, *, expires_at: int = 0) -> int:
        self._send(CMD_PUT, self._encode_put(key, value, expires_at=expires_at))
        payload = self._expect_ok(*self._recv(), "PUT")
        if len(payload) != 8:
            raise ZydecoDBError(f"PUT response expected 8 bytes, got {len(payload)}")
        return struct.unpack(">Q", payload)[0]

    def get(self, key: bytes) -> Optional[bytes]:
        self._send(CMD_GET, self._encode_key(key))
        status, payload = self._recv()
        if status == STATUS_NOT_FOUND:
            return None
        return self._expect_ok(status, payload, "GET")

    def delete(self, key: bytes) -> tuple[bool, int]:
        self._send(CMD_DEL, self._encode_key(key))
        payload = self._expect_ok(*self._recv(), "DEL")
        if len(payload) != 9:
            raise ZydecoDBError(f"DEL response expected 9 bytes, got {len(payload)}")
        deleted = payload[0] != 0
        seq = struct.unpack(">Q", payload[1:9])[0]
        return deleted, seq

    def stats(self) -> dict:
        self._send(CMD_STATS)
        payload = self._expect_ok(*self._recv(), "STATS")
        return json.loads(payload.decode("utf-8"))

    # Convenience wrappers — ZydecoDB still only sees bytes on the wire.

    def put_text(self, key: str, text: str) -> int:
        return self.put(key.encode("utf-8"), text.encode("utf-8"))

    def get_text(self, key: str) -> Optional[str]:
        raw = self.get(key.encode("utf-8"))
        return None if raw is None else raw.decode("utf-8")

    def put_json(self, key: str, data: Any) -> int:
        blob = json.dumps(data, indent=2).encode("utf-8")
        return self.put(key.encode("utf-8"), blob)

    def get_json(self, key: str) -> Any:
        raw = self.get(key.encode("utf-8"))
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def delete_text(self, key: str) -> tuple[bool, int]:
        return self.delete(key.encode("utf-8"))

    # ---- document store ----
    #
    # Collections of JSON documents with secondary indexes. The server keeps
    # every index in sync with the document on each write, atomically.

    @staticmethod
    def _encode_bound(bound: Any) -> bytes:
        """Encode a query bound as a JSON array of scalars (empty = unbounded).

        Accepts a single scalar (wrapped into a one-field bound) or a list for
        composite indexes.
        """
        if bound is None:
            return b""
        values = bound if isinstance(bound, list) else [bound]
        return json.dumps(values, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _decode_page(buf: bytes) -> tuple[list[tuple[bytes, bytes]], bytes]:
        """Decode an index-range response page into rows + next cursor."""
        off = 0
        (count,) = struct.unpack_from(">I", buf, off)
        off += 4
        rows: list[tuple[bytes, bytes]] = []
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

    def define_index(
        self,
        collection: str,
        index: str,
        fields: list[str],
        *,
        unique: bool = False,
        if_not_exists: bool = True,
    ) -> bool:
        """Create a collection (if needed) and a secondary index on it.

        `fields` are dotted JSON paths (e.g. ``["address.city"]``). Defining an
        index over a non-empty collection backfills existing documents. Returns
        False if the index already existed and `if_not_exists` is set.
        """
        payload = _lp(collection.encode("utf-8")) + _lp(index.encode("utf-8"))
        payload += bytes([1 if unique else 0])
        payload += struct.pack(">I", len(fields))
        for field in fields:
            payload += _lp(field.encode("utf-8"))
        self._send(CMD_INDEX_DEF, payload)
        status, body = self._recv()
        if if_not_exists and status == STATUS_CONFLICT:
            return False
        self._expect_ok(status, body, "IndexDef")
        return True

    def put_document(self, collection: str, doc_id: str, document: Any) -> int:
        """Insert or replace a document. Indexes are updated automatically."""
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
        payload = (
            _lp(collection.encode("utf-8"))
            + _lp(doc_id.encode("utf-8"))
            + _lp(body)
        )
        self._send(CMD_DOC_PUT, payload)
        out = self._expect_ok(*self._recv(), "DocPut")
        if len(out) != 8:
            raise ZydecoDBError(f"DocPut response expected 8 bytes, got {len(out)}")
        return struct.unpack(">Q", out)[0]

    def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete a document and its index entries. Returns whether it existed."""
        payload = _lp(collection.encode("utf-8")) + _lp(doc_id.encode("utf-8"))
        self._send(CMD_DOC_DEL, payload)
        out = self._expect_ok(*self._recv(), "DocDel")
        return bool(out and out[0] != 0)

    def get_document(self, collection: str, doc_id: str) -> Optional[Any]:
        """Fetch one document by id, or None if it does not exist."""
        payload = (
            bytes([QUERY_BY_ID])
            + _lp(collection.encode("utf-8"))
            + _lp(doc_id.encode("utf-8"))
        )
        self._send(CMD_QUERY, payload)
        status, out = self._recv()
        if status == STATUS_NOT_FOUND:
            return None
        body = self._expect_ok(status, out, "Query")
        return json.loads(body.decode("utf-8"))

    def query_index(
        self,
        collection: str,
        index: str,
        *,
        lo: Any = None,
        hi: Any = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Range-scan an index, returning all matching docs sorted by the field.

        `lo`/`hi` are inclusive-lower, exclusive-upper bounds (None = unbounded);
        pass a scalar or a list (for composite indexes). Pagination is handled
        internally. Returns ``[{"id": str, "doc": Any}, ...]``.
        """
        lo_b = self._encode_bound(lo)
        hi_b = self._encode_bound(hi)
        cursor = b""
        results: list[dict[str, Any]] = []
        while True:
            payload = bytes([QUERY_INDEX_RANGE])
            payload += _lp(collection.encode("utf-8"))
            payload += _lp(index.encode("utf-8"))
            payload += struct.pack(">I", page_size)
            payload += _lp(lo_b) + _lp(hi_b) + _lp(cursor)
            self._send(CMD_QUERY, payload)
            body = self._expect_ok(*self._recv(), "Query")
            rows, cursor = self._decode_page(body)
            for doc_id, doc_bytes in rows:
                results.append(
                    {
                        "id": doc_id.decode("utf-8"),
                        "doc": json.loads(doc_bytes.decode("utf-8")) if doc_bytes else None,
                    }
                )
            if not cursor:
                return results

    def find_by(self, collection: str, index: str, value: str) -> list[dict[str, Any]]:
        """Exact-match lookup on a single string-valued index.

        Implemented as the range ``[value, value + "\\u0000")``, which matches
        only documents whose field equals `value`. For numeric fields use
        `query_index` with explicit `lo`/`hi` bounds.
        """
        if not isinstance(value, str):
            raise ZydecoDBError(
                "find_by supports string fields; use query_index(lo=, hi=) for numbers"
            )
        return self.query_index(collection, index, lo=value, hi=value + "\u0000")

    # ---- filter-based query layer (server-side predicate evaluation) ----
    #
    # These take a Mongo-style JSON filter; the server plans an access path
    # (index, _id lookup, or collection scan) and re-evaluates the full filter
    # on each candidate, so any field is queryable whether indexed or not.

    @staticmethod
    def _filter_bytes(filt: Optional[dict]) -> bytes:
        if not filt:
            return b""
        return json.dumps(filt, separators=(",", ":")).encode("utf-8")

    def _find_page(
        self,
        collection: str,
        filt: Optional[dict],
        sort: Optional[list[tuple[str, bool]]],
        projection: Optional[tuple[int, list[str]]],
        skip: int,
        limit: int,
        cursor: bytes,
    ) -> tuple[list[tuple[bytes, bytes]], bytes]:
        payload = _lp(collection.encode("utf-8")) + _lp(self._filter_bytes(filt))
        sort = sort or []
        payload += struct.pack(">I", len(sort))
        for field, ascending in sort:
            payload += _lp(field.encode("utf-8")) + bytes([1 if ascending else 0])
        if projection is None:
            payload += bytes([PROJ_NONE])
        else:
            mode, fields = projection
            payload += bytes([mode]) + struct.pack(">I", len(fields))
            for field in fields:
                payload += _lp(field.encode("utf-8"))
        payload += struct.pack(">II", skip, limit) + _lp(cursor)
        self._send(CMD_FIND, payload)
        body = self._expect_ok(*self._recv(), "Find")
        return self._decode_page(body)

    def find(
        self,
        collection: str,
        filt: Optional[dict] = None,
        *,
        sort: Optional[list[tuple[str, bool]]] = None,
        projection: Optional[tuple[int, list[str]]] = None,
        skip: int = 0,
        limit: int = 0,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Yield matching documents as dicts, auto-paginating. `limit` caps the
        total returned (0 = all); `page_size` controls the wire batch size."""
        cursor = b""
        yielded = 0
        while True:
            want = page_size if limit == 0 else min(page_size, limit - yielded)
            if want <= 0:
                return
            rows, cursor = self._find_page(
                collection, filt, sort, projection, skip, want, cursor
            )
            # `skip` is applied by the first request; the cursor carries it after.
            skip = 0
            for _doc_id, body in rows:
                yield json.loads(body.decode("utf-8")) if body else {}
                yielded += 1
                if limit and yielded >= limit:
                    return
            if not cursor:
                return

    def update(
        self,
        collection: str,
        filt: dict,
        update_doc: dict,
        *,
        multi: bool,
    ) -> dict:
        """Apply $-operators to matching documents. Returns
        ``{"matched": n, "modified": m}``."""
        payload = (
            _lp(collection.encode("utf-8"))
            + _lp(self._filter_bytes(filt))
            + _lp(json.dumps(update_doc, separators=(",", ":")).encode("utf-8"))
            + bytes([1 if multi else 0])
        )
        self._send(CMD_UPDATE, payload)
        body = self._expect_ok(*self._recv(), "Update")
        return json.loads(body.decode("utf-8"))

    def delete_by_filter(self, collection: str, filt: dict, *, multi: bool) -> int:
        """Delete matching documents. Returns the number deleted."""
        payload = (
            _lp(collection.encode("utf-8"))
            + _lp(self._filter_bytes(filt))
            + bytes([1 if multi else 0])
        )
        self._send(CMD_DELETE, payload)
        body = self._expect_ok(*self._recv(), "Delete")
        return json.loads(body.decode("utf-8"))["deleted"]

    def count(self, collection: str, filt: Optional[dict] = None) -> int:
        """Count documents matching `filt` (all if None)."""
        payload = (
            bytes([COUNT_MODE_COUNT])
            + _lp(collection.encode("utf-8"))
            + _lp(self._filter_bytes(filt))
        )
        self._send(CMD_COUNT, payload)
        body = self._expect_ok(*self._recv(), "Count")
        return int(body.decode("utf-8"))

    def distinct(
        self, collection: str, field: str, filt: Optional[dict] = None
    ) -> list[Any]:
        """Distinct values of `field` across documents matching `filt`."""
        payload = (
            bytes([COUNT_MODE_DISTINCT])
            + _lp(collection.encode("utf-8"))
            + _lp(self._filter_bytes(filt))
            + _lp(field.encode("utf-8"))
        )
        self._send(CMD_COUNT, payload)
        body = self._expect_ok(*self._recv(), "Distinct")
        return json.loads(body.decode("utf-8"))

    def collection(self, name: str) -> "Collection":
        """A Mongo-inspired handle for a collection of JSON documents."""
        return Collection(self, name)


class Collection:
    """Mongo-inspired document API: the actual product surface.

    Documents are plain dicts. Each carries a string ``_id`` (auto-generated,
    time-ordered, if you don't supply one) that doubles as its storage key.
    Filters and updates use the familiar ``$``-operators.
    """

    def __init__(self, client: "ZydecoDBClient", name: str):
        self._c = client
        self._name = name

    # --- schema ---

    def create_index(
        self, fields: list[str], *, unique: bool = False, name: Optional[str] = None
    ) -> bool:
        """Create a secondary index over one or more dotted field paths."""
        index_name = name or "by_" + "_".join(f.replace(".", "_") for f in fields)
        return self._c.define_index(self._name, index_name, fields, unique=unique)

    # --- writes ---

    def insert_one(self, document: dict) -> str:
        """Insert a document, generating `_id` if absent. Returns the id."""
        doc = dict(document)
        doc_id = str(doc.get("_id") or generate_id())
        doc["_id"] = doc_id
        self._c.put_document(self._name, doc_id, doc)
        return doc_id

    def insert_many(self, documents: list[dict]) -> list[str]:
        return [self.insert_one(d) for d in documents]

    def update_one(self, filt: dict, update: dict) -> dict:
        return self._c.update(self._name, filt, update, multi=False)

    def update_many(self, filt: dict, update: dict) -> dict:
        return self._c.update(self._name, filt, update, multi=True)

    def delete_one(self, filt: dict) -> int:
        return self._c.delete_by_filter(self._name, filt, multi=False)

    def delete_many(self, filt: dict) -> int:
        return self._c.delete_by_filter(self._name, filt, multi=True)

    # --- reads ---

    def find(
        self,
        filt: Optional[dict] = None,
        *,
        sort: Optional[list[tuple[str, bool]]] = None,
        projection: Optional[dict[str, int]] = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[dict]:
        """Iterate matching documents. `sort` is a list of ``(field, ascending)``;
        `projection` is ``{field: 1}`` (include) or ``{field: 0}`` (exclude)."""
        return self._c.find(
            self._name,
            filt,
            sort=sort,
            projection=_encode_projection(projection),
            skip=skip,
            limit=limit,
        )

    def find_one(self, filt: Optional[dict] = None, **kwargs) -> Optional[dict]:
        for doc in self.find(filt, limit=1, **kwargs):
            return doc
        return None

    def count_documents(self, filt: Optional[dict] = None) -> int:
        return self._c.count(self._name, filt)

    def distinct(self, field: str, filt: Optional[dict] = None) -> list[Any]:
        return self._c.distinct(self._name, field, filt)


def _encode_projection(projection: Optional[dict[str, int]]) -> Optional[tuple[int, list[str]]]:
    """Translate a Mongo-style ``{field: 1|0}`` projection into the wire form.
    Include and exclude cannot be mixed (except dropping ``_id``)."""
    if not projection:
        return None
    includes = [f for f, v in projection.items() if v]
    excludes = [f for f, v in projection.items() if not v]
    if includes and excludes:
        raise ZydecoDBError("projection cannot mix include and exclude fields")
    if includes:
        return (PROJ_INCLUDE, includes)
    return (PROJ_EXCLUDE, excludes)


def demo(host: str, port: int, api_key: Optional[str]) -> None:
    print(f"Connecting to ZydecoDB at {host}:{port} ...\n")

    with ZydecoDBClient(host, port, api_key=api_key) as db:
        db.ping()
        print("Server is up.\n")

        contacts = db.collection("contacts")

        # --- one-time setup: an index makes city/age queries fast, but is NOT
        #     required to query those fields (the server can always scan). ---
        print("Creating an index on 'age' ...")
        contacts.create_index(["age"])
        print("  Index ready.\n")

        # --- insert: _id is auto-generated (time-ordered) and returned ---
        print("Inserting a few contacts ...")
        ids = contacts.insert_many(
            [
                {"name": "Margaret Chen", "city": "New Orleans", "age": 38},
                {"name": "James Roux", "city": "New Orleans", "age": 45},
                {"name": "Ada Lovelace", "city": "London", "age": 30},
            ]
        )
        print(f"  Inserted {len(ids)} (e.g. _id={ids[0]}).\n")

        # --- find with an operator filter + sort (uses the age index) ---
        print("Contacts aged 31–50, youngest first ...")
        for c in contacts.find({"age": {"$gte": 31, "$lte": 50}}, sort=[("age", True)]):
            print(f"  {c['age']}  {c['name']} ({c['city']})")
        print()

        # --- find on an UNINDEXED field: still works (collection scan) ---
        print("Everyone in New Orleans (no index on 'city') ...")
        for c in contacts.find({"city": "New Orleans"}, projection={"name": 1}):
            print(f"  {c['name']}")
        print()

        # --- update with $set / $inc ---
        print("Ada has a birthday ...")
        res = contacts.update_one({"name": "Ada Lovelace"}, {"$inc": {"age": 1}})
        print(f"  matched={res['matched']} modified={res['modified']}")
        ada = contacts.find_one({"name": "Ada Lovelace"})
        print(f"  Ada is now {ada['age']}.\n")

        # --- count + distinct ---
        print(f"Total contacts: {contacts.count_documents()}")
        print(f"Cities: {contacts.distinct('city')}\n")

        # --- filtered delete ---
        print("Deleting everyone under 40 ...")
        deleted = contacts.delete_many({"age": {"$lt": 40}})
        print(f"  Deleted {deleted}; remaining: {contacts.count_documents()}\n")

        stats = db.stats()
        print("Server stats")
        print(f"  Running for {stats.get('uptime_s', 0)} seconds")
        print(f"  Last durable write: #{stats.get('last_durable_seq', 0)}")
        print(f"  Items in memory table: {stats.get('memtable_entries', 0)}")

    print("\nAll done.")


def main() -> int:
    parser = argparse.ArgumentParser(description="ZydecoDB example Python client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9470)
    parser.add_argument(
        "--api-key",
        default=os.environ.get("ZYDECODB_API_KEY"),
        help="API key for SessionInit (or set ZYDECODB_API_KEY)",
    )
    args = parser.parse_args()

    try:
        demo(args.host, args.port, args.api_key)
    except (ZydecoDBError, OSError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
