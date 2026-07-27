"""Pinned-connection bounded transactions."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Tuple

from . import _protocol as proto
from .errors import ConnectionError as ZConnectionError
from .errors import ZydecoError, from_status

if TYPE_CHECKING:
    from ._connection import Connection
    from .client import Client


class UnknownCommitError(ZydecoError):
    """Commit may have succeeded; transport failed before the ack was received."""


class Transaction:
    """A single-connection transaction. Not thread-safe. Not reusable after finish."""

    def __init__(self, client: "Client", conn: "Connection") -> None:
        self._client = client
        self._conn = conn
        self._done = False
        self.tx_id = 0
        self.snapshot_seq = 0

    def _ensure_open(self) -> None:
        if self._done:
            raise ZydecoError("transaction already finished")

    def _request(
        self, command: int, payload: bytes, op: str, *, not_found_none: bool = False
    ) -> Optional[bytes]:
        self._ensure_open()
        try:
            status, body = self._conn.request(command, payload)
        except ZConnectionError:
            self._client._pool.discard(self._conn)
            self._conn = None  # type: ignore[assignment]
            self._done = True
            raise
        if status == proto.STATUS_OK:
            return body
        if not_found_none and status == proto.STATUS_NOT_FOUND:
            return None
        raise from_status(status, op, body)

    def _begin(self) -> None:
        body = self._request(proto.CMD_BEGIN, b"", "Begin")
        assert body is not None
        self.tx_id, self.snapshot_seq = proto.decode_begin_response(body)

    def commit(self) -> int:
        self._ensure_open()
        try:
            status, body = self._conn.request(proto.CMD_COMMIT, b"")
        except ZConnectionError as exc:
            self._client._pool.discard(self._conn)
            self._conn = None  # type: ignore[assignment]
            self._done = True
            raise UnknownCommitError(f"Commit: transport failed: {exc}") from exc
        self._client._pool.release(self._conn)
        self._conn = None  # type: ignore[assignment]
        self._done = True
        if status != proto.STATUS_OK:
            raise from_status(status, "Commit", body)
        return proto.decode_commit_response(body)

    def rollback(self) -> None:
        if self._done:
            return
        if self._conn is None:
            self._done = True
            return
        try:
            status, body = self._conn.request(proto.CMD_ROLLBACK, b"")
        except ZConnectionError:
            self._client._pool.discard(self._conn)
            self._conn = None  # type: ignore[assignment]
            self._done = True
            raise
        self._client._pool.release(self._conn)
        self._conn = None  # type: ignore[assignment]
        self._done = True
        if status != proto.STATUS_OK:
            raise from_status(status, "Rollback", body)

    def put(self, key: bytes, value: bytes, *, expires_at: int = 0) -> None:
        self._request(
            proto.CMD_PUT, proto.encode_put(key, value, expires_at=expires_at), "Put"
        )

    def get(self, key: bytes) -> Optional[bytes]:
        return self._request(
            proto.CMD_GET, proto.encode_key(key), "Get", not_found_none=True
        )

    def delete(self, key: bytes) -> None:
        self._request(proto.CMD_DEL, proto.encode_key(key), "Del")

    def put_document(
        self, collection: str, doc_id: str, doc: Any, *, expires_at: int = 0
    ) -> None:
        body = json.dumps(doc, separators=(",", ":")).encode("utf-8")
        self._request(
            proto.CMD_DOC_PUT,
            proto.encode_doc_put(collection, doc_id, body, relaxed=False, expires_at=expires_at),
            "DocPut",
        )

    def put_document_if_match(
        self,
        collection: str,
        doc_id: str,
        doc: Any,
        if_match: int,
        *,
        expires_at: int = 0,
    ) -> None:
        body = json.dumps(doc, separators=(",", ":")).encode("utf-8")
        self._request(
            proto.CMD_DOC_PUT_IF_MATCH,
            proto.encode_doc_put_if_match(
                collection, doc_id, body, relaxed=False, if_match=if_match, expires_at=expires_at
            ),
            "DocPutIfMatch",
        )

    def delete_document(self, collection: str, doc_id: str) -> None:
        self._request(
            proto.CMD_DOC_DEL, proto.encode_doc_del(collection, doc_id), "DocDel"
        )

    def get_document_with_revision(
        self, collection: str, doc_id: str
    ) -> Optional[Tuple[Dict[str, Any], int]]:
        body = self._request(
            proto.CMD_DOC_GET_REV,
            proto.encode_query_by_id(collection, doc_id),
            "DocGetRev",
            not_found_none=True,
        )
        if body is None:
            return None
        raw, rev = proto.decode_doc_get_rev_response(body)
        return json.loads(raw.decode("utf-8")), rev

    def update_document_if_match(
        self, collection: str, doc_id: str, update: Any, if_match: int
    ) -> None:
        body = json.dumps(update, separators=(",", ":")).encode("utf-8")
        self._request(
            proto.CMD_DOC_UPDATE_IF_MATCH,
            proto.encode_doc_update_if_match(
                collection, doc_id, body, relaxed=False, if_match=if_match
            ),
            "DocUpdateIfMatch",
        )


@contextmanager
def transaction(client: "Client") -> Iterator[Transaction]:
    """Context manager: begin, yield Tx, commit on success, rollback on error."""
    conn = client._pool.acquire()
    tx = Transaction(client, conn)
    try:
        try:
            tx._begin()
        except Exception:
            if not tx._done:
                client._pool.release(conn)
                tx._done = True
            raise
        yield tx
        tx.commit()
    except Exception:
        try:
            tx.rollback()
        except Exception:
            pass
        raise
