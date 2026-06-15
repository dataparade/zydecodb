"""Integration tests against a live ZydecoDB server.

Set ZYDECODB_TEST_HOST / ZYDECODB_TEST_PORT (and optionally ZYDECODB_TEST_API_KEY)
to point at a running server. The whole module is skipped when the server is not
reachable, so a plain `pytest` run stays green offline; CI starts a server first.
"""

import os
import socket
import uuid

import pytest

from zydecodb import Client, ConflictError

HOST = os.environ.get("ZYDECODB_TEST_HOST", "127.0.0.1")
PORT = int(os.environ.get("ZYDECODB_TEST_PORT", "9470"))
API_KEY = os.environ.get("ZYDECODB_TEST_API_KEY") or None


def _server_up() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=1.0):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _server_up(), reason=f"no ZydecoDB server at {HOST}:{PORT}"
)


@pytest.fixture()
def db():
    client = Client(HOST, PORT, api_key=API_KEY)
    yield client
    client.close()


@pytest.fixture()
def coll(db):
    # A unique collection per test keeps runs isolated.
    return db.collection(f"pytest_{uuid.uuid4().hex[:12]}")


def test_ping(db):
    db.ping()


def test_insert_find_update_delete(coll):
    coll.create_index(["age"])
    ids = coll.insert_many(
        [
            {"name": "Ada", "age": 30, "city": "London"},
            {"name": "Bo", "age": 25, "city": "NOLA"},
            {"name": "Cy", "age": 40, "city": "NOLA"},
        ]
    )
    assert len(ids) == 3

    got = list(coll.find({"age": {"$gte": 30}}, sort=[("age", True)]))
    assert [d["name"] for d in got] == ["Ada", "Cy"]

    res = coll.update_one({"name": "Bo"}, {"$inc": {"age": 10}})
    assert res["matched"] == 1 and res["modified"] == 1
    assert coll.find_one({"name": "Bo"})["age"] == 35

    assert coll.count_documents() == 3
    assert sorted(coll.distinct("city")) == ["London", "NOLA"]

    assert coll.delete_many({"city": "NOLA"}) == 2
    assert coll.count_documents() == 1


def test_unique_index_conflict(coll):
    coll.create_index(["email"], unique=True)
    coll.insert_one({"email": "a@b.com"})
    with pytest.raises(ConflictError):
        coll.insert_one({"email": "a@b.com"})


def test_pagination_is_stable(coll):
    coll.create_index(["n"])
    coll.insert_many([{"n": i} for i in range(25)])
    seen = [d["n"] for d in coll.find({"n": {"$gte": 0}}, page_size=10)]
    assert sorted(seen) == list(range(25))
    assert len(seen) == 25  # no duplicates across pages


def test_get_by_id(coll):
    doc_id = coll.insert_one({"name": "Zee"})
    fetched = coll.get(doc_id)
    assert fetched["name"] == "Zee"
    assert coll.get("does-not-exist") is None
