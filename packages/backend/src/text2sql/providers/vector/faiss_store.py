"""File-backed FAISS vector store with optional BM25 lexical fusion.

Default vector backend — zero infra, persists to a directory of .index + .json
files. Hybrid search uses rank_bm25 for the lexical leg.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from text2sql.config import ProviderEntry
from text2sql.providers.base import VectorHit, VectorStore
from text2sql.providers.factory import register_vector


class FaissStore(VectorStore):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._root = Path(cfg["path"])
        self._root.mkdir(parents=True, exist_ok=True)
        self._collections: dict[str, _Collection] = {}

    def _coll(self, name: str) -> "_Collection":
        if name not in self._collections:
            self._collections[name] = _Collection(self._root / name)
        return self._collections[name]

    def upsert(
        self,
        collection: str,
        ids: list[str],
        vectors: np.ndarray,
        payloads: list[dict[str, Any]],
    ) -> None:
        self._coll(collection).upsert(ids, vectors, payloads)

    def search(
        self,
        collection: str,
        vector: np.ndarray,
        k: int = 5,
        filters: dict[str, Any] | None = None,
    ) -> list[VectorHit]:
        return self._coll(collection).search(vector, k, filters)

    def hybrid_search(
        self,
        collection: str,
        vector: np.ndarray,
        text: str,
        k: int = 5,
        filters: dict[str, Any] | None = None,
    ) -> list[VectorHit]:
        return self._coll(collection).hybrid_search(vector, text, k, filters)


class _Collection:
    """Lazy-loaded per-collection FAISS index + payload store."""

    def __init__(self, dir_: Path) -> None:
        self._dir = dir_
        self._dir.mkdir(parents=True, exist_ok=True)
        self._index = None
        self._payloads: list[dict[str, Any]] = []
        self._ids: list[str] = []
        self._dim: int | None = None
        self._load()

    def _meta_path(self) -> Path:
        return self._dir / "meta.json"

    def _load(self) -> None:
        meta = self._meta_path()
        if not meta.exists():
            return
        data = json.loads(meta.read_text())
        self._ids = data["ids"]
        self._payloads = data["payloads"]
        self._dim = data.get("dim")
        idx_path = self._dir / "index.faiss"
        if self._dim and idx_path.exists():
            import faiss  # local import — heavy
            self._index = faiss.read_index(str(idx_path))

    def _persist(self) -> None:
        import faiss
        if self._index is not None:
            faiss.write_index(self._index, str(self._dir / "index.faiss"))
        self._meta_path().write_text(
            json.dumps({"ids": self._ids, "payloads": self._payloads, "dim": self._dim})
        )

    def upsert(self, ids: list[str], vectors: np.ndarray, payloads: list[dict[str, Any]]) -> None:
        if vectors.size == 0:
            return
        import faiss
        vectors = np.ascontiguousarray(vectors, dtype=np.float32)
        if self._index is None:
            self._dim = vectors.shape[1]
            self._index = faiss.IndexFlatIP(self._dim)
        # Normalize for cosine via inner product
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self._index.add(vectors / norms)
        self._ids.extend(ids)
        self._payloads.extend(payloads)
        self._persist()

    def search(
        self, vector: np.ndarray, k: int, filters: dict[str, Any] | None
    ) -> list[VectorHit]:
        if self._index is None or len(self._ids) == 0:
            return []
        v = np.ascontiguousarray(vector.reshape(1, -1), dtype=np.float32)
        n = np.linalg.norm(v)
        if n > 0:
            v = v / n
        # When a filter is active we may need to scan deep — most candidates
        # might fail the filter. Without a filter, k*4 is plenty of headroom.
        n_total = len(self._ids)
        n_request = n_total if filters else min(k * 4, n_total)
        scores, idxs = self._index.search(v, n_request)
        hits: list[VectorHit] = []
        for score, idx in zip(scores[0], idxs[0]):
            if idx < 0:
                continue
            payload = self._payloads[idx]
            if filters and not _matches(payload, filters):
                continue
            hits.append(VectorHit(id=self._ids[idx], score=float(score), payload=payload))
            if len(hits) >= k:
                break
        return hits

    def hybrid_search(
        self,
        vector: np.ndarray,
        text: str,
        k: int,
        filters: dict[str, Any] | None,
    ) -> list[VectorHit]:
        from rank_bm25 import BM25Okapi

        vec_hits = self.search(vector, max(k * 4, 64), filters)
        corpus = [_tokenize_for_bm25(str(p.get("text") or p.get("name") or "")) for p in self._payloads]
        # rank_bm25 divides by total corpus length — if every doc tokenizes to
        # an empty list (e.g. a collection of pure numeric values like
        # column_values), avgdl is 0/0. Fall back to vector-only.
        non_empty_docs = sum(1 for c in corpus if c)
        query_toks = _tokenize_for_bm25(text)
        if non_empty_docs == 0 or not query_toks:
            return vec_hits[:k]
        bm25 = BM25Okapi(corpus)
        bm25_scores = bm25.get_scores(query_toks)
        hi = float(bm25_scores.max()) if len(bm25_scores) else 1.0
        norm = (lambda x: x / hi) if hi > 0 else (lambda x: x)
        scored: dict[str, float] = {h.id: 0.6 * h.score for h in vec_hits}
        for i, score in enumerate(bm25_scores):
            id_ = self._ids[i]
            if filters and not _matches(self._payloads[i], filters):
                continue
            scored[id_] = scored.get(id_, 0.0) + 0.4 * float(norm(score))
        out = sorted(scored.items(), key=lambda kv: kv[1], reverse=True)[:k]
        idx_by_id = {id_: i for i, id_ in enumerate(self._ids)}
        return [
            VectorHit(id=id_, score=score, payload=self._payloads[idx_by_id[id_]])
            for id_, score in out
        ]


# BM25 tokenization: lowercase, split on non-alphanumeric, drop short tokens
# (single letters and 2-letter noise like 'a', 'i', 'of'), and a tiny stop list.
import re as _re

_BM25_STOPS = frozenset({
    "the", "and", "for", "with", "from", "this", "that", "are", "was",
    "were", "all", "any", "but", "not", "have", "has", "had", "you", "your",
    "what", "which", "who", "how", "why", "where", "when", "into", "out",
    "over", "under", "per", "via", "etc",
})


def _tokenize_for_bm25(text: str) -> list[str]:
    raw = _re.findall(r"[A-Za-z0-9]+", text.lower())
    return [t for t in raw if len(t) >= 3 and t not in _BM25_STOPS]


def _matches(payload: dict[str, Any], filters: dict[str, Any]) -> bool:
    for k, v in filters.items():
        pv = payload.get(k)
        # Both list ⇒ set intersection (tag filter)
        if isinstance(v, list) and isinstance(pv, list):
            if not (set(pv) & set(v)):
                return False
        # Filter is a list, payload is a scalar ⇒ scalar must be in the list
        elif isinstance(v, list):
            if pv not in v:
                return False
        # Filter is a scalar, payload is a list ⇒ scalar must be in the list
        elif isinstance(pv, list):
            if v not in pv:
                return False
        # Both scalar ⇒ exact match
        elif pv != v:
            return False
    return True


@register_vector("faiss")
def _build(spec: ProviderEntry) -> FaissStore:
    return FaissStore(spec)
