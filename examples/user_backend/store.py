"""
User storage on top of ZydecoDB's document driver.

Users are JSON documents in a collection, accessed through the `Collection`
API (find / insert_one / update_one / delete_one with `$`-operators). ZydecoDB
maintains the secondary indexes, so this layer never hand-rolls lookups:

  collection "users", _id = <uuid>      full user record (JSON)
    index on ["email"]                  email lookup + login
    index on ["created_at"]             list users in signup order

Login sessions stay on the raw key-value path because they need a TTL, which
the document write path does not expose:

  session:<token>             maps login token -> user id (24h TTL)

Human auth (passwords, bearer tokens) lives here.
Database auth (ZYDECODB_API_KEY) is handled by ZydecoDBClient on connect.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from zydecodb import Client, ConflictError as DbConflictError, ZydecoError


class StoreError(Exception):
    pass


class NotFound(StoreError):
    pass


class Conflict(StoreError):
    pass


class AuthError(StoreError):
    pass


@dataclass
class User:
    id: str
    email: str
    name: str
    created_at: str
    password_hash: str
    password_salt: str

    def public_view(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "email": self.email,
            "name": self.name,
            "created_at": self.created_at,
        }


class UserStore:
    SESSION_TTL_SECONDS = 24 * 60 * 60

    COLLECTION = "users"

    def __init__(self, db: Client):
        self._db = db
        self._users = db.collection(self.COLLECTION)

    def ensure_schema(self) -> None:
        """Define the collection + indexes once. Idempotent (safe to re-run)."""
        # The email index is UNIQUE: the server rejects a second account with the
        # same email atomically, so we don't need a check-then-insert race here.
        self._users.create_index(["email"], unique=True)
        self._users.create_index(["created_at"])

    # ---- document <-> dataclass mapping ----
    #
    # The document's `_id` is its storage key; we map it to/from `User.id`.

    @staticmethod
    def _to_doc(user: User) -> dict[str, Any]:
        doc = asdict(user)
        doc["_id"] = doc.pop("id")
        return doc

    @staticmethod
    def _from_doc(data: dict[str, Any]) -> User:
        d = dict(data)
        d["id"] = d.pop("_id")
        return User(**d)

    # ---- key helpers ----

    @staticmethod
    def _norm_email(email: str) -> str:
        return email.strip().lower()

    @staticmethod
    def _session_key(token: str) -> str:
        return f"session:{token}"

    # ---- password hashing (stdlib only) ----

    @staticmethod
    def _hash_password(password: str, salt: bytes) -> str:
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            iterations=200_000,
        )
        return digest.hex()

    @staticmethod
    def _verify_password(password: str, user: User) -> bool:
        expected = UserStore._hash_password(password, bytes.fromhex(user.password_salt))
        return hmac.compare_digest(expected, user.password_hash)

    # ---- raw-KV json helpers (used for TTL sessions) ----

    def _get_json(self, key: str) -> Any:
        raw = self._db.get(key.encode("utf-8"))
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def _put_json(self, key: str, data: Any, *, expires_at: int = 0) -> None:
        blob = json.dumps(data, separators=(",", ":")).encode("utf-8")
        self._db.put(key.encode("utf-8"), blob, expires_at=expires_at)

    def _delete(self, key: str) -> None:
        self._db.delete(key.encode("utf-8"))

    def _find_by_email(self, email: str) -> Optional[User]:
        data = self._users.find_one({"email": email})
        return None if data is None else self._from_doc(data)

    # ---- users (documents) ----

    def create_user(self, email: str, name: str, password: str) -> User:
        email = self._norm_email(email)
        name = name.strip()
        if not email or "@" not in email:
            raise StoreError("email is required")
        if not name:
            raise StoreError("name is required")
        if len(password) < 8:
            raise StoreError("password must be at least 8 characters")

        salt = secrets.token_bytes(16)
        user = User(
            id=str(uuid.uuid4()),
            email=email,
            name=name,
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            password_hash=self._hash_password(password, salt),
            password_salt=salt.hex(),
        )

        # One write stores the document and both index entries atomically. The
        # unique email index is enforced server-side: a duplicate is rejected
        # with a Conflict status, with no check-then-insert race.
        try:
            self._users.insert_one(self._to_doc(user))
        except DbConflictError as exc:
            raise Conflict(f"an account already exists for {email}") from exc
        return user

    def get_user(self, user_id: str) -> User:
        data = self._users.find_one({"_id": user_id})
        if data is None:
            raise NotFound(f"user {user_id} not found")
        return self._from_doc(data)

    def list_users(self) -> list[dict[str, Any]]:
        return [
            self._from_doc(doc).public_view()
            for doc in self._users.find(sort=[("created_at", True)])
        ]

    def update_user_name(self, user_id: str, name: str) -> User:
        name = name.strip()
        if not name:
            raise StoreError("name is required")
        res = self._users.update_one({"_id": user_id}, {"$set": {"name": name}})
        if res["matched"] == 0:
            raise NotFound(f"user {user_id} not found")
        return self.get_user(user_id)

    def delete_user(self, user_id: str) -> None:
        if self._users.delete_one({"_id": user_id}) == 0:
            raise NotFound(f"user {user_id} not found")

    # ---- auth sessions (raw KV, TTL) ----

    def login(self, email: str, password: str) -> tuple[str, User]:
        email = self._norm_email(email)
        user = self._find_by_email(email)
        if user is None or not self._verify_password(password, user):
            raise AuthError("invalid email or password")

        token = secrets.token_urlsafe(32)
        expires_at = int((time.time() + self.SESSION_TTL_SECONDS) * 1000)
        self._put_json(
            self._session_key(token),
            {"user_id": user.id},
            expires_at=expires_at,
        )
        return token, user

    def user_for_token(self, token: str) -> User:
        session = self._get_json(self._session_key(token))
        if session is None:
            raise AuthError("session expired or invalid")
        return self.get_user(session["user_id"])

    def logout(self, token: str) -> None:
        self._delete(self._session_key(token))


def open_store(
    host: str, port: int, api_key: str | None = None
) -> tuple[Client, UserStore]:
    db = Client(f"{host}:{port}", api_key=api_key)
    db.ping()
    store = UserStore(db)
    store.ensure_schema()
    return db, store
