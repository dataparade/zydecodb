"""ZydecoDB Python driver.

Quick start::

    from zydecodb import Client

    with Client("127.0.0.1", 9470, api_key="...") as db:
        users = db.collection("users")
        users.create_index(["email"], unique=True)
        uid = users.insert_one({"email": "a@b.com", "name": "Ada"})
        for u in users.find({"name": "Ada"}):
            print(u)
"""

from .change_stream import ChangeEvent, ChangeStream
from .client import Client, generate_id
from .collection import Collection
from .errors import (
    AuthError,
    ConfigError,
    ConflictError,
    ConnectionError,
    InvalidRequestError,
    PolicyError,
    ServerBusyError,
    ServerError,
    UnsupportedFormatError,
    ZydecoError,
)
from .transaction import Transaction, UnknownCommitError

__version__ = "0.10.0"

__all__ = [
    "Client",
    "Collection",
    "ChangeEvent",
    "ChangeStream",
    "Transaction",
    "UnknownCommitError",
    "generate_id",
    "ZydecoError",
    "ConfigError",
    "ConnectionError",
    "ServerError",
    "AuthError",
    "ConflictError",
    "InvalidRequestError",
    "ServerBusyError",
    "PolicyError",
    "UnsupportedFormatError",
    "__version__",
]
