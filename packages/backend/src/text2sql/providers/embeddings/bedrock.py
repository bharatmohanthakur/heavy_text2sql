"""AWS Bedrock embeddings via API key — Titan or Cohere.

Same auth path as the Bedrock LLM provider (Authorization: Bearer
<BEDROCK_API_KEY>), different payload shapes per family. Set `family` in
YAML to pick the wire format:

    family: titan      # amazon.titan-embed-text-v2:0 (default)
    family: cohere     # cohere.embed-english-v3, cohere.embed-multilingual-v3

Titan supports a single text per call; we batch in Python. Cohere takes a
list of up to 96 strings per call natively.
"""

from __future__ import annotations

from typing import Any

import httpx
import numpy as np

from text2sql.config import ProviderEntry
from text2sql.providers.base import EmbeddingKind, EmbeddingProvider
from text2sql.providers.factory import _resolve_secret, register_embedding


class BedrockEmbedding(EmbeddingProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg: dict[str, Any] = spec.model_dump()
        self._model: str = cfg["model"]
        self._region: str = cfg.get("region", "us-west-2")
        self._api_key: str = _resolve_secret(cfg["api_key_env"])
        self._family: str = (cfg.get("family") or "titan").lower()
        self._dim: int = int(cfg.get("dim", 1024))
        self._batch_size: int = int(cfg.get("batch_size", 16))
        self._timeout_s: float = float(cfg.get("timeout_s", 60.0))
        if self._family not in ("titan", "cohere"):
            raise ValueError(
                f"unknown bedrock embedding family: {self._family!r} "
                "(expected 'titan' or 'cohere')"
            )
        self._client = httpx.Client(
            base_url=f"https://bedrock-runtime.{self._region}.amazonaws.com",
            timeout=self._timeout_s,
        )

    @property
    def dim(self) -> int:
        return self._dim

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._client.post(
            f"/model/{self._model}/invoke",
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
            },
            json=payload,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"bedrock embed {resp.status_code} (model={self._model}, "
                f"region={self._region}): {resp.text[:400]}"
            )
        return resp.json()

    def embed(self, texts: list[str], kind: EmbeddingKind = "doc") -> np.ndarray:
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)
        if self._family == "titan":
            # Titan v2: one inputText per request; loop client-side.
            vecs: list[np.ndarray] = []
            for t in texts:
                body = self._post({
                    "inputText": t,
                    "dimensions": self._dim,
                    "normalize": True,
                })
                vecs.append(np.asarray(body["embedding"], dtype=np.float32))
            return np.vstack(vecs)
        # cohere — batch up to 96 per call, separate input_type for query/doc
        input_type = "search_query" if kind == "query" else "search_document"
        out: list[np.ndarray] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            body = self._post({
                "texts": batch,
                "input_type": input_type,
                "embedding_types": ["float"],
            })
            # Cohere returns {"embeddings": {"float": [[...], ...]}}
            embs = body.get("embeddings") or {}
            arr = embs.get("float") if isinstance(embs, dict) else embs
            for v in arr:
                out.append(np.asarray(v, dtype=np.float32))
        return np.vstack(out)


@register_embedding("bedrock")
def _build(spec: ProviderEntry) -> BedrockEmbedding:
    return BedrockEmbedding(spec)
