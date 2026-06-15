"""A thread-safe, bounded connection pool.

Connections are created lazily up to `max_size`. Checkout blocks (up to
`acquire_timeout`) when the pool is saturated. Idle connections older than
`keepalive_idle` are validated with a Ping on checkout and replaced if dead, so
callers rarely observe a stale socket.
"""

from __future__ import annotations

import threading
import time
from typing import List, Optional

from ._connection import Connection
from .errors import ConfigError
from .errors import ConnectionError as ZConnectionError


class ConnectionPool:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        api_key: Optional[str] = None,
        timeout: float = 5.0,
        max_size: int = 8,
        acquire_timeout: float = 10.0,
        keepalive_idle: float = 30.0,
    ):
        if max_size < 1:
            raise ConfigError("max_size must be >= 1")
        self._host = host
        self._port = port
        self._api_key = api_key
        self._timeout = timeout
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._keepalive_idle = keepalive_idle

        self._lock = threading.Condition()
        self._idle: List[Connection] = []
        self._total = 0
        self._closed = False

    def _new_connection(self) -> Connection:
        conn = Connection(
            self._host, self._port, timeout=self._timeout, api_key=self._api_key
        )
        conn.connect()
        return conn

    def acquire(self) -> Connection:
        """Check out a healthy connection, creating one if capacity allows or
        waiting for one to be returned otherwise."""
        deadline = time.monotonic() + self._acquire_timeout
        with self._lock:
            while True:
                if self._closed:
                    raise ZConnectionError("pool is closed")
                if self._idle:
                    conn = self._idle.pop()
                    # Validate connections that have been idle a while.
                    if time.monotonic() - conn.last_used > self._keepalive_idle:
                        if not conn.ping():
                            conn.close()
                            self._total -= 1
                            continue
                    return conn
                if self._total < self._max_size:
                    self._total += 1
                    break  # create outside the lock
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ZConnectionError("timed out acquiring a connection from the pool")
                self._lock.wait(remaining)

        # Create the new connection without holding the lock (it does network I/O).
        try:
            return self._new_connection()
        except Exception:
            with self._lock:
                self._total -= 1
                self._lock.notify()
            raise

    def release(self, conn: Connection) -> None:
        """Return a connection to the pool, or drop it if it died."""
        with self._lock:
            if self._closed or not conn.connected:
                conn.close()
                self._total -= 1
                self._lock.notify()
                return
            self._idle.append(conn)
            self._lock.notify()

    def discard(self, conn: Connection) -> None:
        """Permanently drop a (presumed-broken) connection."""
        conn.close()
        with self._lock:
            self._total -= 1
            self._lock.notify()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for conn in self._idle:
                conn.close()
            self._total -= len(self._idle)
            self._idle.clear()
            self._lock.notify_all()
