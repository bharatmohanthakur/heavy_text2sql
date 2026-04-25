"""Provider abstraction layer.

Three Protocols (LLMProvider, EmbeddingProvider, VectorStore) and one factory
make every external dependency swappable via configs/default.yaml. Concrete
implementations live under llm/, embeddings/, vector/, and db/ subpackages.
"""

from text2sql.providers.base import (
    EmbeddingProvider,
    LLMMessage,
    LLMProvider,
    SqlEngine,
    VectorStore,
)
from text2sql.providers.factory import build_embedding, build_llm, build_sql_engine, build_vector_store

__all__ = [
    "EmbeddingProvider",
    "LLMMessage",
    "LLMProvider",
    "SqlEngine",
    "VectorStore",
    "build_embedding",
    "build_llm",
    "build_sql_engine",
    "build_vector_store",
]
