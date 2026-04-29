"""Gold SQL CRUD + retrieval.

Stores NL→SQL pairs in Postgres `text2sql_meta`. Retrieval combines:
  * NL embedding cosine similarity (semantic match)
  * AST embedding cosine similarity (structural match)
  * Domain overlap with the current query's routed domains (re-rank boost)
  * approval_status filter (default: only `approved`)

For corpora < 5000 rows, brute-force cosine in NumPy is sub-millisecond and
keeps us free of pgvector. Swap in pgvector later if scale demands it.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import numpy as np
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from text2sql.gold.ast_flatten import flatten_sql_ast
from text2sql.gold.schema import GoldRecord, GoldSqlRow, _Base
from text2sql.providers.base import EmbeddingProvider

log = logging.getLogger(__name__)


def _domains_for_tables(tables: Iterable[str], catalog) -> list[str]:
    if catalog is None:
        return []
    by_fqn = catalog.by_fqn()
    out: set[str] = set()
    for fqn in tables:
        e = by_fqn.get(fqn)
        if e:
            out.update(e.domains)
    return sorted(out)


def _cosine_matrix(query: np.ndarray, corpus: np.ndarray) -> np.ndarray:
    if corpus.size == 0:
        return np.zeros((0,), dtype=np.float32)
    q = query / (np.linalg.norm(query) + 1e-12)
    c_norms = np.linalg.norm(corpus, axis=1, keepdims=True) + 1e-12
    return (corpus / c_norms) @ q


@dataclass
class GoldHit:
    record: GoldRecord
    score: float


class GoldStore:
    def __init__(
        self,
        sa_url: str,
        embedder: EmbeddingProvider,
        *,
        catalog=None,
        active_provider: str = "",
        active_dialect: str = "",
    ) -> None:
        """`active_provider` and `active_dialect` (N4) tag every NEWLY
        created row and become the default scope for retrieval. They
        come from cfg.target_db.primary + cfg.target_db_provider().kind
        at server startup. Existing rows with non-matching scope are
        invisible to retrieval — gold authored against MSSQL won't steer
        SQLite queries to write `TOP 50` syntax."""
        self._engine = sa.create_engine(sa_url, future=True, pool_pre_ping=True)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)
        self._embedder = embedder
        self._catalog = catalog
        self._active_provider = active_provider
        self._active_dialect = active_dialect

    # ── Schema management ────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        _Base.metadata.create_all(self._engine)

    def drop_schema(self) -> None:
        _Base.metadata.drop_all(self._engine)

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def create(
        self,
        nl_question: str,
        sql_text: str,
        *,
        tables_used: list[str] | None = None,
        author: str = "",
        approval_status: str = "pending",
        note: str = "",
        target_provider: str | None = None,
        dialect: str | None = None,
        source_gold_id: uuid.UUID | None = None,
    ) -> GoldRecord:
        ast_flat = flatten_sql_ast(sql_text)
        nl_vec = self._embedder.embed([nl_question], kind="doc")[0].tolist()
        sql_vec = self._embedder.embed([ast_flat or sql_text], kind="doc")[0].tolist()
        tables = list(tables_used or [])
        domains = _domains_for_tables(tables, self._catalog)
        with self._Session.begin() as session:
            row = GoldSqlRow(
                nl_question=nl_question,
                sql_text=sql_text,
                sql_ast_flat=ast_flat,
                tables_used=tables,
                domains_used=domains,
                embedding_nl=nl_vec,
                embedding_sql=sql_vec,
                author=author,
                approval_status=approval_status,
                note=note,
                target_provider=(target_provider if target_provider is not None
                                  else self._active_provider),
                dialect=(dialect if dialect is not None else self._active_dialect),
                source_gold_id=source_gold_id,
            )
            session.add(row)
            session.flush()
            return GoldRecord.from_row(row)

    def get(self, id_: uuid.UUID) -> GoldRecord | None:
        with self._Session() as session:
            row = session.get(GoldSqlRow, id_)
            return GoldRecord.from_row(row) if row else None

    def update(
        self,
        id_: uuid.UUID,
        *,
        nl_question: str | None = None,
        sql_text: str | None = None,
        tables_used: list[str] | None = None,
    ) -> GoldRecord | None:
        with self._Session.begin() as session:
            row = session.get(GoldSqlRow, id_)
            if not row:
                return None
            if nl_question is not None:
                row.nl_question = nl_question
                row.embedding_nl = self._embedder.embed([nl_question], kind="doc")[0].tolist()
            if sql_text is not None:
                row.sql_text = sql_text
                row.sql_ast_flat = flatten_sql_ast(sql_text)
                row.embedding_sql = self._embedder.embed(
                    [row.sql_ast_flat or sql_text], kind="doc"
                )[0].tolist()
            if tables_used is not None:
                row.tables_used = list(tables_used)
                row.domains_used = _domains_for_tables(tables_used, self._catalog)
            return GoldRecord.from_row(row)

    def approve(self, id_: uuid.UUID, reviewer: str) -> GoldRecord | None:
        with self._Session.begin() as session:
            row = session.get(GoldSqlRow, id_)
            if not row:
                return None
            row.approval_status = "approved"
            row.approved_by = reviewer
            row.approved_at = datetime.now(timezone.utc)
            return GoldRecord.from_row(row)

    def reject(self, id_: uuid.UUID, reviewer: str, reason: str = "") -> GoldRecord | None:
        with self._Session.begin() as session:
            row = session.get(GoldSqlRow, id_)
            if not row:
                return None
            row.approval_status = "rejected"
            row.approved_by = reviewer
            row.note = reason or row.note
            return GoldRecord.from_row(row)

    def mark_exec_passed(self, id_: uuid.UUID, *, ok: bool) -> None:
        with self._Session.begin() as session:
            row = session.get(GoldSqlRow, id_)
            if not row:
                return
            row.exec_check_passed = ok
            row.exec_check_at = datetime.now(timezone.utc)

    def list(
        self,
        *,
        approval_status: str | None = None,
        domain: str | None = None,
        target_provider: str | None = None,
        all_providers: bool = False,
        limit: int = 100,
    ) -> list[GoldRecord]:
        """List gold rows. By default, scoped to `active_provider` (the
        server's current target). Pass `all_providers=True` for the
        admin / Gold Studio cross-provider view. Pass an explicit
        `target_provider` to scope to a non-active one."""
        stmt = sa.select(GoldSqlRow)
        if approval_status:
            stmt = stmt.where(GoldSqlRow.approval_status == approval_status)
        if domain:
            stmt = stmt.where(GoldSqlRow.domains_used.any(domain))
        if not all_providers:
            scope = (target_provider if target_provider is not None
                     else self._active_provider)
            if scope:
                # Tolerate legacy untagged rows so in-place upgrades don't
                # hide every pre-N4 gold pair until somebody back-tags them.
                stmt = stmt.where(sa.or_(
                    GoldSqlRow.target_provider == scope,
                    GoldSqlRow.target_provider == "",
                ))
        stmt = stmt.order_by(GoldSqlRow.created_at.desc()).limit(limit)
        with self._Session() as session:
            return [GoldRecord.from_row(r) for r in session.scalars(stmt)]

    def delete(self, id_: uuid.UUID) -> bool:
        with self._Session.begin() as session:
            row = session.get(GoldSqlRow, id_)
            if not row:
                return False
            session.delete(row)
            return True

    # ── Retrieval ────────────────────────────────────────────────────────────

    def retrieve_top_k(
        self,
        nl_question: str,
        *,
        domains: list[str] | None = None,
        k: int = 3,
        approved_only: bool = True,
        domain_overlap_boost: float = 0.15,
        all_providers: bool = False,
    ) -> list[GoldHit]:
        """NL-vector cosine + small domain-overlap boost. Returns top-K.

        Scoped to `active_provider` by default — gold authored for a
        different DB dialect would steer the LLM toward syntax that
        won't execute against the active engine. `all_providers=True`
        is reserved for admin / debugging views, never the pipeline."""
        with self._Session() as session:
            stmt = sa.select(GoldSqlRow)
            if approved_only:
                stmt = stmt.where(GoldSqlRow.approval_status == "approved")
            if not all_providers and self._active_provider:
                stmt = stmt.where(sa.or_(
                    GoldSqlRow.target_provider == self._active_provider,
                    GoldSqlRow.target_provider == "",
                ))
            rows = list(session.scalars(stmt))
        if not rows:
            return []
        q_vec = np.asarray(
            self._embedder.embed([nl_question], kind="query")[0],
            dtype=np.float32,
        )
        corpus_nl = np.asarray(
            [r.embedding_nl for r in rows], dtype=np.float32
        )
        sims = _cosine_matrix(q_vec, corpus_nl)
        wanted = set(domains or [])
        out: list[GoldHit] = []
        for i, row in enumerate(rows):
            score = float(sims[i])
            if wanted:
                overlap = len(set(row.domains_used or []) & wanted)
                if overlap:
                    score += domain_overlap_boost * min(1.0, overlap / max(1, len(wanted)))
            out.append(GoldHit(record=GoldRecord.from_row(row), score=score))
        out.sort(key=lambda h: h.score, reverse=True)
        return out[:k]

    # ── Utilities ────────────────────────────────────────────────────────────

    def count(self, *, approval_status: str | None = None) -> int:
        stmt = sa.select(sa.func.count(GoldSqlRow.id))
        if approval_status:
            stmt = stmt.where(GoldSqlRow.approval_status == approval_status)
        with self._Session() as session:
            return int(session.scalar(stmt) or 0)
