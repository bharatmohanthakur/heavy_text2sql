"""OpenAI direct embeddings — text-embedding-3-large / -small."""

from __future__ import annotations

import numpy as np
from openai import OpenAI

from text2sql.config import ProviderEntry
from text2sql.providers.base import EmbeddingKind, EmbeddingProvider
from text2sql.providers.factory import _resolve_secret, register_embedding


class OpenAIEmbedding(EmbeddingProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._client = OpenAI(api_key=_resolve_secret(cfg["api_key_env"]))
        self._dim: int = int(cfg.get("dim", 3072))
        self._batch_size: int = int(cfg.get("batch_size", 64))

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str], kind: EmbeddingKind = "doc") -> np.ndarray:
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)
        out: list[np.ndarray] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            resp = self._client.embeddings.create(model=self._model, input=batch)
            out.extend(np.asarray(d.embedding, dtype=np.float32) for d in resp.data)
        return np.vstack(out)


@register_embedding("openai")
def _build(spec: ProviderEntry) -> OpenAIEmbedding:
    return OpenAIEmbedding(spec)
