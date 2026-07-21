"""A single TCP connection to a ZydecoDB server.

One connection is NOT thread-safe; the pool guarantees a connection is only ever
checked out to one caller at a time. Framing and (optional) authentication live
here; pooling, retries, and the typed API live above.
"""

from __future__ import annotations

import socket
import ssl
import struct
import time
from typing import Optional, Tuple, Union

from . import _protocol as proto
from .errors import ConnectionError as ZConnectionError
from .errors import from_status

# True enables system-default TLS; an SSLContext customizes verification/SNI.
TlsOption = Union[bool, ssl.SSLContext]


class Connection:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        timeout: float,
        api_key: Optional[str],
        tls: Optional[TlsOption] = None,
    ):
        self._addr = (host, port)
        self._timeout = timeout
        self._api_key = api_key
        self._tls = tls
        self._sock: Optional[socket.socket] = None
        self.last_used = 0.0

    def connect(self) -> None:
        self.close()
        try:
            sock = socket.create_connection(self._addr, timeout=self._timeout)
            sock.settimeout(self._timeout)
            # Disable Nagle: requests are small and latency-sensitive.
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self._tls:
                ctx = (
                    ssl.create_default_context()
                    if self._tls is True
                    else self._tls
                )
                sock = ctx.wrap_socket(sock, server_hostname=self._addr[0])
        except OSError as exc:
            raise ZConnectionError(f"connect to {self._addr[0]}:{self._addr[1]} failed: {exc}")
        self._sock = sock
        self.last_used = time.monotonic()
        if self._api_key:
            self._session_init(self._api_key)

    def _session_init(self, api_key: str) -> None:
        status, payload = self.request(proto.CMD_SESSION_INIT, api_key.encode("utf-8"))
        if status != proto.STATUS_OK:
            raise from_status(status, "SessionInit", payload)

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def request(self, command: int, payload: bytes = b"") -> Tuple[int, bytes]:
        """Send one framed request and read the framed response. Raises
        `ConnectionError` on any transport failure (the pool discards the
        connection; the client may retry idempotent calls on a fresh one)."""
        if self._sock is None:
            raise ZConnectionError("not connected")
        try:
            self._sock.sendall(proto.encode_header(command, len(payload)) + payload)
            status, body = self._recv()
        except (OSError, ZConnectionError) as exc:
            self.close()
            raise ZConnectionError(f"request failed: {exc}")
        self.last_used = time.monotonic()
        return status, body

    def ping(self) -> bool:
        """Send a keepalive Ping. Returns True if the server answered OK."""
        try:
            status, _ = self.request(proto.CMD_PING)
        except ZConnectionError:
            return False
        return status == proto.STATUS_OK

    def _recv(self) -> Tuple[int, bytes]:
        header = self._recv_exact(proto.HEADER_LEN)
        version, status, length = struct.unpack(">BBI", header)
        if version != proto.PROTO_VERSION:
            raise ZConnectionError(f"unexpected protocol version 0x{version:02x}")
        payload = self._recv_exact(length) if length else b""
        return status, payload

    def _recv_exact(self, n: int) -> bytes:
        assert self._sock is not None
        chunks = []
        got = 0
        while got < n:
            chunk = self._sock.recv(n - got)
            if not chunk:
                raise ZConnectionError("connection closed while reading")
            chunks.append(chunk)
            got += len(chunk)
        return b"".join(chunks)
