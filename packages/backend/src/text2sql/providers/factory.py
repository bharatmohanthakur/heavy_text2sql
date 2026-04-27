"""Config-driven factory: builds providers from `ProviderEntry` specs.

Adding a new provider = register a builder in the relevant registry. Callers
never import concrete classes — they pass a `ProviderEntry` (parsed from YAML)
and get back something that satisfies the Protocol.
"""

from __future__ import annotations

import os
from typing import Callable

from text2sql.config import ProviderEntry
from text2sql.providers.base import EmbeddingProvider, LLMProvider, SqlEngine, VectorStore

LLMBuilder = Callable[[ProviderEntry], LLMProvider]
EmbeddingBuilder = Callable[[ProviderEntry], EmbeddingProvider]
VectorBuilder = Callable[[ProviderEntry], VectorStore]
SqlEngineBuilder = Callable[[ProviderEntry], SqlEngine]


_LLM_REGISTRY: dict[str, LLMBuilder] = {}
_EMBEDDING_REGISTRY: dict[str, EmbeddingBuilder] = {}
_VECTOR_REGISTRY: dict[str, VectorBuilder] = {}
_SQL_REGISTRY: dict[str, SqlEngineBuilder] = {}


def register_llm(kind: str) -> Callable[[LLMBuilder], LLMBuilder]:
    def deco(fn: LLMBuilder) -> LLMBuilder:
        _LLM_REGISTRY[kind] = fn
        return fn
    return deco


def register_embedding(kind: str) -> Callable[[EmbeddingBuilder], EmbeddingBuilder]:
    def deco(fn: EmbeddingBuilder) -> EmbeddingBuilder:
        _EMBEDDING_REGISTRY[kind] = fn
        return fn
    return deco


def register_vector(kind: str) -> Callable[[VectorBuilder], VectorBuilder]:
    def deco(fn: VectorBuilder) -> VectorBuilder:
        _VECTOR_REGISTRY[kind] = fn
        return fn
    return deco


def register_sql_engine(kind: str) -> Callable[[SqlEngineBuilder], SqlEngineBuilder]:
    def deco(fn: SqlEngineBuilder) -> SqlEngineBuilder:
        _SQL_REGISTRY[kind] = fn
        return fn
    return deco


def _resolve_secret(env_key: str) -> str:
    val = os.environ.get(env_key)
    if not val:
        raise RuntimeError(f"required secret env var {env_key!r} is not set")
    return val


def build_llm(spec: ProviderEntry) -> LLMProvider:
    _ensure_loaded()
    if spec.kind not in _LLM_REGISTRY:
        raise ValueError(f"unknown LLM provider kind: {spec.kind}")
    return _LLM_REGISTRY[spec.kind](spec)


def build_embedding(spec: ProviderEntry) -> EmbeddingProvider:
    _ensure_loaded()
    if spec.kind not in _EMBEDDING_REGISTRY:
        raise ValueError(f"unknown embedding provider kind: {spec.kind}")
    return _EMBEDDING_REGISTRY[spec.kind](spec)


def build_vector_store(spec: ProviderEntry) -> VectorStore:
    _ensure_loaded()
    if spec.kind not in _VECTOR_REGISTRY:
        raise ValueError(f"unknown vector store kind: {spec.kind}")
    return _VECTOR_REGISTRY[spec.kind](spec)


def build_sql_engine(spec: ProviderEntry) -> SqlEngine:
    _ensure_loaded()
    if spec.kind not in _SQL_REGISTRY:
        raise ValueError(f"unknown SQL engine kind: {spec.kind}")
    return _SQL_REGISTRY[spec.kind](spec)


_LOADED = False


def _ensure_loaded() -> None:
    global _LOADED
    if _LOADED:
        return
    # Side-effect imports register builders into the registries.
    from text2sql.providers.llm import azure_openai as _llm_azure  # noqa: F401
    from text2sql.providers.llm import anthropic as _llm_anthropic  # noqa: F401
    from text2sql.providers.llm import openai as _llm_openai  # noqa: F401
    from text2sql.providers.llm import openrouter as _llm_openrouter  # noqa: F401
    from text2sql.providers.llm import bedrock as _llm_bedrock  # noqa: F401
    from text2sql.providers.embeddings import azure_openai as _emb_azure  # noqa: F401
    from text2sql.providers.embeddings import openai as _emb_openai  # noqa: F401
    from text2sql.providers.embeddings import bedrock as _emb_bedrock  # noqa: F401
    # sentence-transformers is optional — register only if importable so a
    # missing dep doesn't break users who only use cloud embeddings.
    try:
        from text2sql.providers.embeddings import sentence_transformers as _emb_st  # noqa: F401
    except Exception:
        pass
    from text2sql.providers.vector import faiss_store as _vec_faiss  # noqa: F401
    from text2sql.providers.db import postgresql as _db_pg  # noqa: F401
    from text2sql.providers.db import mssql as _db_mssql  # noqa: F401
    from text2sql.providers.db import sqlite as _db_sqlite  # noqa: F401

    _LOADED = True


__all__ = [
    "build_embedding",
    "build_llm",
    "build_sql_engine",
    "build_vector_store",
    "register_embedding",
    "register_llm",
    "register_sql_engine",
    "register_vector",
]
