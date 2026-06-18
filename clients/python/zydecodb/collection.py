"""The product surface: a MongoDB-inspired `Collection` of JSON documents.

Documents are plain dicts, each with a string ``_id`` (auto-generated and
time-ordered if you don't supply one) that doubles as its storage key. Filters
and updates use the familiar ``$``-operators; the server plans the access path
(index, ``_id`` lookup, or collection scan) and re-checks the full filter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

from . import _protocol as proto
from .errors import ConfigError

if TYPE_CHECKING:
    from .client import Client


class Collection:
    def __init__(self, client: "Client", name: str):
        self._c = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    # --- schema ---

    def create_index(
        self,
        fields: List[str],
        *,
        unique: bool = False,
        name: Optional[str] = None,
    ) -> bool:
        """Create a secondary index over one or more dotted field paths. Returns
        False if it already existed."""
        index_name = name or "by_" + "_".join(f.replace(".", "_") for f in fields)
        return self._c.define_index(self._name, index_name, fields, unique=unique)

    # --- writes ---

    def insert_one(self, document: dict, *, relaxed: bool = False) -> str:
        """Insert a document, generating ``_id`` if absent. Returns the id.
        Raises `ConflictError` if a unique index would be violated."""
        from .client import generate_id

        doc = dict(document)
        doc_id = str(doc.get("_id") or generate_id())
        doc["_id"] = doc_id
        self._c.put_document(self._name, doc_id, doc, relaxed=relaxed)
        return doc_id

    def insert_many(self, documents: List[dict]) -> List[str]:
        return [self.insert_one(d) for d in documents]

    def replace_one(self, doc_id: str, document: dict, *, relaxed: bool = False) -> int:
        """Insert or fully replace the document at ``doc_id``."""
        doc = dict(document)
        doc["_id"] = str(doc_id)
        return self._c.put_document(self._name, str(doc_id), doc, relaxed=relaxed)

    def update_one(self, filt: dict, update: dict, *, relaxed: bool = False) -> dict:
        return self._c.update(self._name, filt, update, multi=False, relaxed=relaxed)

    def update_many(self, filt: dict, update: dict, *, relaxed: bool = False) -> dict:
        return self._c.update(self._name, filt, update, multi=True, relaxed=relaxed)

    def delete_one(self, filt: dict, *, relaxed: bool = False) -> int:
        return self._c.delete_by_filter(self._name, filt, multi=False, relaxed=relaxed)

    def delete_many(self, filt: dict, *, relaxed: bool = False) -> int:
        return self._c.delete_by_filter(self._name, filt, multi=True, relaxed=relaxed)

    # --- reads ---

    def find(
        self,
        filt: Optional[dict] = None,
        *,
        sort: Optional[List[Tuple[str, bool]]] = None,
        projection: Optional[Dict[str, int]] = None,
        skip: int = 0,
        limit: int = 0,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Iterate matching documents (auto-paginating). ``sort`` is a list of
        ``(field, ascending)``; ``projection`` is ``{field: 1}`` (include) or
        ``{field: 0}`` (exclude)."""
        return self._c.find(
            self._name,
            filt,
            sort=sort,
            projection=_encode_projection(projection),
            skip=skip,
            limit=limit,
            page_size=page_size,
        )

    def find_one(self, filt: Optional[dict] = None, **kwargs: Any) -> Optional[dict]:
        for doc in self.find(filt, limit=1, **kwargs):
            return doc
        return None

    def get(self, doc_id: str) -> Optional[dict]:
        """Fetch one document directly by id (fast path)."""
        return self._c.get_document(self._name, str(doc_id))

    def count_documents(self, filt: Optional[dict] = None) -> int:
        return self._c.count(self._name, filt)

    def distinct(self, field: str, filt: Optional[dict] = None) -> List[Any]:
        return self._c.distinct(self._name, field, filt)


def _encode_projection(
    projection: Optional[Dict[str, int]]
) -> Optional[Tuple[int, List[str]]]:
    if not projection:
        return None
    includes = [f for f, v in projection.items() if v]
    excludes = [f for f, v in projection.items() if not v]
    if includes and excludes:
        raise ConfigError("projection cannot mix include and exclude fields")
    if includes:
        return (proto.PROJ_INCLUDE, includes)
    return (proto.PROJ_EXCLUDE, excludes)
