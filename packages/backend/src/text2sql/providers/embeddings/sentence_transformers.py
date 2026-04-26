"""Local sentence-transformers / Hugging Face embeddings.

Wraps any HF embedding model (BGE-M3, E5-Mistral, all-MiniLM, multilingual-e5,
bge-large-en-v1.5, …) so the platform can run fully offline without paying
per-call cloud embedding fees.

The model is loaded once at startup and lives in process memory. CPU works
fine for the 829-table catalog; for big rebuilds set `device: cuda` or `mps`.

Many encoder-style models expect different prompts for documents vs queries
(e.g. `query: …` for E5, `Represent this sentence … :` for INSTRUCTOR). The
provider's `query_prefix` / `doc_prefix` knobs let you set them in YAML
without subclassing.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from text2sql.config import ProviderEntry
from text2sql.providers.base import EmbeddingKind, EmbeddingProvider
from text2sql.providers.factory import register_embedding


class SentenceTransformersEmbedding(EmbeddingProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        # Lazy import so the dep is optional; only people using this provider
        # need to install sentence-transformers.
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise RuntimeError(
                "sentence-transformers is not installed. "
                "Run: uv add sentence-transformers"
            ) from e

        cfg: dict[str, Any] = spec.model_dump()
        model_name: str = cfg["model"]
        device: str | None = cfg.get("device") or None
        cache_dir: str | None = cfg.get("cache_dir") or None
        trust_remote_code: bool = bool(cfg.get("trust_remote_code", False))

        self._model = SentenceTransformer(
            model_name,
            device=device,
            cache_folder=cache_dir,
            trust_remote_code=trust_remote_code,
        )
        # Different sentence-transformers versions expose dim under different
        # method names. Try each in turn, fall back to YAML override.
        dim_method = (
            getattr(self._model, "get_embedding_dimension", None)
            or getattr(self._model, "get_sentence_embedding_dimension", None)
        )
        try:
            self._dim = int(dim_method()) if dim_method else int(cfg.get("dim", 768))
        except Exception:
            self._dim = int(cfg.get("dim", 768))
        # Override dim if explicitly set (some BGE-M3 variants use 1024).
        if cfg.get("dim"):
            self._dim = int(cfg["dim"])

        self._batch_size = int(cfg.get("batch_size", 32))
        self._normalize = bool(cfg.get("normalize", True))
        # Per-kind prefixes, e.g. for E5: {"query": "query: ", "doc": "passage: "}
        self._query_prefix = str(cfg.get("query_prefix") or "")
        self._doc_prefix = str(cfg.get("doc_prefix") or "")

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str], kind: EmbeddingKind = "doc") -> np.ndarray:
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)
        prefix = self._query_prefix if kind == "query" else self._doc_prefix
        prepared = [prefix + t for t in texts] if prefix else texts
        vecs = self._model.encode(
            prepared,
            batch_size=self._batch_size,
            normalize_embeddings=self._normalize,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return vecs.astype(np.float32, copy=False)


@register_embedding("sentence_transformers")
def _build(spec: ProviderEntry) -> SentenceTransformersEmbedding:
    return SentenceTransformersEmbedding(spec)
