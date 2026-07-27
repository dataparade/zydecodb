"""Collection change-stream (Watch) over a dedicated connection."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Optional

from . import _protocol as proto
from .errors import from_status

if TYPE_CHECKING:
    from .client import Client


@dataclass(frozen=True)
class ChangeEvent:
    """One durable change from a Watch subscription."""

    op: str  # "upsert" or "delete"
    doc_id: str
    document: Optional[dict]
    resume_token: str  # base64-encoded opaque bytes


class ChangeStream:
    """Iterator and context manager over Watch stream events."""

    def __init__(self, client: "Client", collection: str, resume_token: Optional[bytes] = None):
        self._client = client
        self._collection = collection
        self._resume = resume_token or b""
        self._conn = client._pool.open_dedicated()
        self._opened = False
        self._closed = False
        self._last_token = b""

    def _open(self) -> None:
        if self._opened:
            return
        payload = proto.encode_watch(self._collection, self._resume)
        status, body = self._conn.request(proto.CMD_WATCH, payload)
        if status != proto.STATUS_OK:
            self.close()
            raise from_status(status, "Watch", body)
        # Initial ACK: keep last token but do not surface as an event.
        kind, token, _, _, _ = proto.decode_watch_frame(body)
        if kind != "ack":
            self.close()
            raise from_status(proto.STATUS_PROTOCOL_ERROR, "Watch", b"expected initial ack")
        self._last_token = token
        self._opened = True

    def __enter__(self) -> "ChangeStream":
        self._open()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __iter__(self) -> Iterator[ChangeEvent]:
        self._open()
        while not self._closed:
            status, body = self._conn.recv()
            if status != proto.STATUS_OK:
                self.close()
                raise from_status(status, "Watch", body)
            kind, token, op, doc_id, raw_body = proto.decode_watch_frame(body)
            self._last_token = token
            if kind in ("ack", "heartbeat"):
                continue
            if kind != "event" or op is None or doc_id is None:
                self.close()
                raise from_status(proto.STATUS_PROTOCOL_ERROR, "Watch", b"invalid event frame")
            if op == proto.WATCH_OP_UPSERT:
                document = json.loads(raw_body.decode("utf-8")) if raw_body else {}
                yield ChangeEvent(
                    op="upsert",
                    doc_id=doc_id.decode("utf-8"),
                    document=document,
                    resume_token=base64.b64encode(token).decode("ascii"),
                )
            elif op == proto.WATCH_OP_DELETE:
                yield ChangeEvent(
                    op="delete",
                    doc_id=doc_id.decode("utf-8"),
                    document=None,
                    resume_token=base64.b64encode(token).decode("ascii"),
                )
            else:
                self.close()
                raise from_status(
                    proto.STATUS_PROTOCOL_ERROR, "Watch", f"unknown op 0x{op:02x}".encode()
                )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._conn.close()
