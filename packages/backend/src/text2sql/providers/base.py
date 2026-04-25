"""Provider Protocol definitions — implement to add a new backend."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Literal, Protocol, runtime_checkable

import numpy as np


@dataclass(frozen=True)
class LLMMessage:
    role: Literal["system", "user", "assistant"]
    content: str


@dataclass(frozen=True)
class VectorHit:
    id: str
    score: float
    payload: dict[str, Any]


EmbeddingKind = Literal["doc", "query"]


@runtime_checkable
class LLMProvider(Protocol):
    @property
    def model_id(self) -> str: ...

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str: ...

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]: ...


@runtime_checkable
class EmbeddingProvider(Protocol):
    @property
    def dim(self) -> int: ...

    def embed(self, texts: list[str], kind: EmbeddingKind = "doc") -> np.ndarray: ...


@runtime_checkable
class VectorStore(Protocol):
    def upsert(
        self,
        collection: str,
        ids: list[str],
        vectors: np.ndarray,
        payloads: list[dict[str, Any]],
    ) -> None: ...

    def search(
        self,
        collection: str,
        vector: np.ndarray,
        k: int = 5,
        filters: dict[str, Any] | None = None,
    ) -> list[VectorHit]: ...

    def hybrid_search(
        self,
        collection: str,
        vector: np.ndarray,
        text: str,
        k: int = 5,
        filters: dict[str, Any] | None = None,
    ) -> list[VectorHit]: ...


@runtime_checkable
class SqlEngine(Protocol):
    """Target SQL execution engine — pluggable per dialect (postgresql, mssql, snowflake)."""

    @property
    def dialect(self) -> str: ...

    def execute(
        self, sql: str, *, params: dict[str, Any] | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]: ...

    def explain(self, sql: str) -> str: ...

    def list_tables(self) -> list[tuple[str, str]]:
        """Return [(schema, table), ...] across configured search paths."""
        ...

    def list_columns(self, schema: str, table: str) -> list[tuple[str, str, bool]]:
        """Return [(column_name, data_type, nullable), ...] for the table."""
        ...

    def quote_identifier(self, name: str) -> str: ...
