"""Query-time retrieval over the `tables` collection.

Given a NL question + (optional) ranked domain list, returns the top-K most
relevant tables. Supports:
  * vector-only search
  * hybrid (vector + BM25) search
  * domain pre-filter via payload tag (multi-domain OR)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from text2sql.providers.base import EmbeddingProvider, VectorStore


@dataclass
class TableHit:
    fqn: str
    score: float
    domains: list[str]
    is_descriptor: bool
    text: str                 # the embed blob, useful for downstream context


class TableRetriever:
    def __init__(
        self,
        embedder: EmbeddingProvider,
        store: VectorStore,
        *,
        collection: str = "tables",
    ) -> None:
        self._embedder = embedder
        self._store = store
        self._collection = collection

    def search(
        self,
        query: str,
        *,
        k: int = 8,
        domains: Iterable[str] | None = None,
        hybrid: bool = True,
    ) -> list[TableHit]:
        vec = self._embedder.embed([query], kind="query")[0]
        filters: dict | None = None
        if domains:
            filters = {"domains": list(domains)}
        if hybrid:
            hits = self._store.hybrid_search(
                self._collection, vec, query, k=k, filters=filters
            )
        else:
            hits = self._store.search(
                self._collection, vec, k=k, filters=filters
            )
        return [
            TableHit(
                fqn=h.payload.get("fqn", h.id),
                score=h.score,
                domains=h.payload.get("domains", []),
                is_descriptor=h.payload.get("is_descriptor", False),
                text=h.payload.get("text", ""),
            )
            for h in hits
        ]
