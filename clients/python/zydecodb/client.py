"""The top-level client: pooled, retrying request execution plus the document API.

`Client` owns a `ConnectionPool` and is safe to share across threads. Transient
transport failures (and server `EngineBusy`) are retried with exponential
backoff for operations that are safe to repeat; non-idempotent operations
(operator updates, deletes) are never retried automatically.
"""

from __future__ import annotations

import json
import os
import random
import ssl
import time
from typing import Any, Iterator, List, Optional, Tuple, Union

from . import _protocol as proto
from .collection import Collection
from .errors import ConnectionError as ZConnectionError
from .errors import ServerBusyError, ZydecoError, from_status
from .pool import ConnectionPool

# True enables system-default TLS; pass an SSLContext for custom roots/SNI.
TlsOption = Union[bool, ssl.SSLContext]


def generate_id() -> str:
    """A time-ordered id (UUIDv7-style): 48-bit ms timestamp + 80 random bits,
    hex-encoded. Lexicographically sortable by creation time."""
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    return ts_ms.to_bytes(6, "big").hex() + os.urandom(10).hex()


class Client:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9470,
        *,
        api_key: Optional[str] = None,
        timeout: float = 5.0,
        pool_size: int = 8,
        max_retries: int = 2,
        backoff_base: float = 0.05,
        backoff_cap: float = 2.0,
        tls: Optional[TlsOption] = None,
    ):
        self._pool = ConnectionPool(
            host,
            port,
            api_key=api_key,
            timeout=timeout,
            max_size=pool_size,
            tls=tls,
        )
        self._max_retries = max(0, max_retries)
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap

    # --- lifecycle ---

    def close(self) -> None:
        self._pool.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # --- core request execution ---

    def _backoff(self, attempt: int) -> float:
        # Full-jitter exponential backoff.
        ceiling = min(self._backoff_cap, self._backoff_base * (2 ** attempt))
        return random.uniform(0, ceiling)

    def _execute(
        self,
        command: int,
        payload: bytes,
        op: str,
        *,
        retryable: bool,
        not_found_none: bool = False,
    ) -> Optional[bytes]:
        last_exc: Optional[ZydecoError] = None
        for attempt in range(self._max_retries + 1):
            conn = self._pool.acquire()
            try:
                status, body = conn.request(command, payload)
            except ZConnectionError as exc:
                self._pool.discard(conn)
                last_exc = exc
                if retryable and attempt < self._max_retries:
                    time.sleep(self._backoff(attempt))
                    continue
                raise
            self._pool.release(conn)

            if status == proto.STATUS_OK:
                return body
            if not_found_none and status == proto.STATUS_NOT_FOUND:
                return None
            if (
                status == proto.STATUS_ENGINE_BUSY
                and retryable
                and attempt < self._max_retries
            ):
                last_exc = ServerBusyError(f"{op}: server busy", status=status)
                time.sleep(self._backoff(attempt))
                continue
            raise from_status(status, op, body)
        assert last_exc is not None
        raise last_exc

    # --- health / introspection ---

    def ping(self) -> None:
        self._execute(proto.CMD_PING, b"", "Ping", retryable=True)

    def stats(self) -> dict:
        body = self._execute(proto.CMD_STATS, b"", "Stats", retryable=True)
        return json.loads(body.decode("utf-8"))

    # --- raw key/value (idempotent set/get) ---

    def put(self, key: bytes, value: bytes, *, expires_at: int = 0) -> int:
        body = self._execute(
            proto.CMD_PUT,
            proto.encode_put(key, value, expires_at=expires_at),
            "Put",
            retryable=True,
        )
        return _u64(body, "Put")

    def get(self, key: bytes) -> Optional[bytes]:
        return self._execute(
            proto.CMD_GET,
            proto.encode_key(key),
            "Get",
            retryable=True,
            not_found_none=True,
        )

    def delete(self, key: bytes) -> bool:
        body = self._execute(
            proto.CMD_DEL, proto.encode_key(key), "Delete", retryable=False
        )
        return bool(body and body[0] != 0)

    # --- document layer (used by Collection) ---

    def define_index(
        self,
        collection: str,
        index: str,
        fields: List[str],
        *,
        unique: bool = False,
        if_not_exists: bool = True,
    ) -> bool:
        payload = proto.encode_index_def(collection, index, fields, unique=unique)
        conn = self._pool.acquire()
        try:
            status, body = conn.request(proto.CMD_INDEX_DEF, payload)
        except ZConnectionError:
            self._pool.discard(conn)
            raise
        self._pool.release(conn)
        if if_not_exists and status == proto.STATUS_CONFLICT:
            return False
        if status != proto.STATUS_OK:
            raise from_status(status, "IndexDef", body)
        return True

    def put_document(
        self, collection: str, doc_id: str, document: Any, *, relaxed: bool = False
    ) -> int:
        body = self._execute(
            proto.CMD_DOC_PUT,
            proto.encode_doc_put(collection, doc_id, document, relaxed=relaxed),
            "DocPut",
            retryable=True,
        )
        return _u64(body, "DocPut")

    def delete_document(self, collection: str, doc_id: str) -> bool:
        body = self._execute(
            proto.CMD_DOC_DEL,
            proto.encode_doc_del(collection, doc_id),
            "DocDel",
            retryable=False,
        )
        return bool(body and body[0] != 0)

    def get_document(self, collection: str, doc_id: str) -> Optional[Any]:
        body = self._execute(
            proto.CMD_QUERY,
            proto.encode_query_by_id(collection, doc_id),
            "Query",
            retryable=True,
            not_found_none=True,
        )
        return None if body is None else json.loads(body.decode("utf-8"))

    def find(
        self,
        collection: str,
        filt: Optional[dict] = None,
        *,
        sort: Optional[List[Tuple[str, bool]]] = None,
        projection: Optional[Tuple[int, List[str]]] = None,
        skip: int = 0,
        limit: int = 0,
        page_size: int = 100,
    ) -> Iterator[dict]:
        cursor = b""
        yielded = 0
        while True:
            want = page_size if limit == 0 else min(page_size, limit - yielded)
            if want <= 0:
                return
            payload = proto.encode_find(
                collection, filt, sort, projection, skip, want, cursor
            )
            body = self._execute(proto.CMD_FIND, payload, "Find", retryable=True)
            rows, cursor = proto.decode_page(body)
            skip = 0  # applied on the first page; the cursor carries it onward
            for _doc_id, raw in rows:
                yield json.loads(raw.decode("utf-8")) if raw else {}
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
        relaxed: bool = False,
        upsert: bool = False,
    ) -> dict:
        body = self._execute(
            proto.CMD_UPDATE,
            proto.encode_update(
                collection,
                filt,
                update_doc,
                multi=multi,
                relaxed=relaxed,
                upsert=upsert,
            ),
            "Update",
            retryable=False,
        )
        return json.loads(body.decode("utf-8"))

    def delete_by_filter(
        self, collection: str, filt: dict, *, multi: bool, relaxed: bool = False
    ) -> int:
        body = self._execute(
            proto.CMD_DELETE,
            proto.encode_delete(collection, filt, multi=multi, relaxed=relaxed),
            "Delete",
            retryable=False,
        )
        return json.loads(body.decode("utf-8"))["deleted"]

    def count(self, collection: str, filt: Optional[dict] = None) -> int:
        body = self._execute(
            proto.CMD_COUNT, proto.encode_count(collection, filt), "Count", retryable=True
        )
        return int(body.decode("utf-8"))

    def distinct(
        self, collection: str, field: str, filt: Optional[dict] = None
    ) -> List[Any]:
        body = self._execute(
            proto.CMD_COUNT,
            proto.encode_distinct(collection, field, filt),
            "Distinct",
            retryable=True,
        )
        return json.loads(body.decode("utf-8"))

    # --- handle factory ---

    def collection(self, name: str) -> Collection:
        return Collection(self, name)


def _u64(body: Optional[bytes], op: str) -> int:
    import struct

    if body is None or len(body) != 8:
        raise ZydecoError(f"{op}: expected 8-byte sequence, got {len(body or b'')}")
    return struct.unpack(">Q", body)[0]
