"""Exception taxonomy.

`ZydecoError` is the base. Transport problems (socket errors, closed
connections) raise `ConnectionError`. Server responses with a non-OK status
raise the matching `ServerError` subclass, carrying the wire status byte so
callers can branch on the failure class without string-matching messages.
"""

from __future__ import annotations

from typing import Optional

from . import _protocol as proto


class ZydecoError(Exception):
    """Base class for every error raised by this driver."""


class ConfigError(ZydecoError):
    """Invalid client configuration or argument."""


class ConnectionError(ZydecoError):
    """A transport-level failure (connect/send/recv). Safe to retry for
    idempotent operations; the driver does this automatically."""


class ServerError(ZydecoError):
    """A non-OK response from the server. `status` is the wire status byte."""

    status: Optional[int] = None

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class AuthError(ServerError):
    """Unauthorized or forbidden (status 0x0B / 0x0C)."""


class ConflictError(ServerError):
    """A constraint conflict, e.g. a unique-index violation (status 0x03)."""


class InvalidRequestError(ServerError):
    """The server rejected the request as malformed or invalid
    (protocol/invalid-key/invalid-value)."""


class ServerBusyError(ServerError):
    """The server is shedding load (rate limit / engine busy, status 0x07)."""


_STATUS_TO_EXC = {
    proto.STATUS_CONFLICT: ConflictError,
    proto.STATUS_UNAUTHORIZED: AuthError,
    proto.STATUS_FORBIDDEN: AuthError,
    proto.STATUS_ENGINE_BUSY: ServerBusyError,
    proto.STATUS_PROTOCOL_ERROR: InvalidRequestError,
    proto.STATUS_INVALID_KEY: InvalidRequestError,
    proto.STATUS_INVALID_VALUE: InvalidRequestError,
}


def from_status(status: int, op: str, payload: bytes) -> ServerError:
    """Build the most specific `ServerError` for a non-OK response."""
    detail = payload.decode("utf-8", errors="replace")
    name = proto.status_name(status)
    message = f"{op} failed: {name}" + (f" ({detail})" if detail else "")
    exc_cls = _STATUS_TO_EXC.get(status, ServerError)
    return exc_cls(message, status=status)
