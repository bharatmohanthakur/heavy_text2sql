"""SQLAlchemy schema for the gold_sql table.

Embeddings are stored as JSON arrays (no pgvector required). When pgvector
is available, swap to that for fast ANN; current corpus is small enough
(<1000 rows) that brute-force cosine in Python is fine.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase


# Cross-dialect text-array type: PG gets a real `text[]`; everything
# else (sqlite, mssql) gets a JSON-encoded list. The Python-side value
# is always `list[str]`, so callers don't care which dialect rendered it.
_TextArray = JSON().with_variant(PG_ARRAY(Text), "postgresql")
_UuidColumn = sa.Uuid(as_uuid=True).with_variant(PG_UUID(as_uuid=True), "postgresql")


class _Base(DeclarativeBase):
    pass


class GoldSqlRow(_Base):
    """ORM row corresponding to one approved or pending NL-SQL pair."""

    __tablename__ = "gold_sql"

    id = Column(_UuidColumn, primary_key=True, default=uuid.uuid4)
    nl_question = Column(Text, nullable=False)
    sql_text = Column(Text, nullable=False)
    sql_ast_flat = Column(Text, nullable=False, default="")
    tables_used = Column(_TextArray, nullable=False, default=list)
    domains_used = Column(_TextArray, nullable=False, default=list)

    # Embeddings as JSON arrays. Switch to pgvector when corpus > a few thousand.
    embedding_nl = Column(JSON, nullable=False, default=list)
    embedding_sql = Column(JSON, nullable=False, default=list)

    author = Column(String(128), nullable=False, default="")
    approval_status = Column(String(16), nullable=False, default="pending")  # pending|approved|rejected
    approved_by = Column(String(128), nullable=True)
    approved_at = Column(DateTime(timezone=True), nullable=True)
    exec_check_passed = Column(Boolean, nullable=False, default=False)
    exec_check_at = Column(DateTime(timezone=True), nullable=True)

    note = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Per-provider scoping (N4). target_provider is the target_db.primary
    # at the time this gold pair was authored — gold retrieval at query
    # time MUST filter by the active provider so a Postgres-flavored gold
    # SQL doesn't steer the LLM to write Postgres syntax against MSSQL.
    # dialect is denormalized for fast filter / display (mssql / postgresql
    # / sqlite). source_gold_id links a row promoted across providers
    # back to its origin so the curator can see the lineage.
    target_provider = Column(String(128), nullable=False, default="")
    dialect = Column(String(32), nullable=False, default="")
    source_gold_id = Column(_UuidColumn, nullable=True)


@dataclass
class GoldRecord:
    """Plain-Python view of a gold row, used for I/O outside the ORM."""
    id: uuid.UUID
    nl_question: str
    sql_text: str
    tables_used: list[str]
    domains_used: list[str]
    approval_status: str
    exec_check_passed: bool
    author: str = ""
    approved_by: str | None = None
    note: str = ""
    sql_ast_flat: str = ""
    embedding_nl: list[float] = field(default_factory=list)
    embedding_sql: list[float] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    approved_at: datetime | None = None
    exec_check_at: datetime | None = None
    # Per-provider scoping (N4)
    target_provider: str = ""
    dialect: str = ""
    source_gold_id: uuid.UUID | None = None

    @classmethod
    def from_row(cls, row: GoldSqlRow) -> "GoldRecord":
        return cls(
            id=row.id,
            nl_question=row.nl_question,
            sql_text=row.sql_text,
            sql_ast_flat=row.sql_ast_flat or "",
            tables_used=list(row.tables_used or []),
            domains_used=list(row.domains_used or []),
            embedding_nl=list(row.embedding_nl or []),
            embedding_sql=list(row.embedding_sql or []),
            author=row.author or "",
            approval_status=row.approval_status,
            approved_by=row.approved_by,
            note=row.note or "",
            created_at=row.created_at,
            updated_at=row.updated_at,
            approved_at=row.approved_at,
            exec_check_at=row.exec_check_at,
            exec_check_passed=row.exec_check_passed,
            target_provider=row.target_provider or "",
            dialect=row.dialect or "",
            source_gold_id=row.source_gold_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "nl_question": self.nl_question,
            "sql_text": self.sql_text,
            "sql_ast_flat": self.sql_ast_flat,
            "tables_used": self.tables_used,
            "domains_used": self.domains_used,
            "approval_status": self.approval_status,
            "exec_check_passed": self.exec_check_passed,
            "author": self.author,
            "approved_by": self.approved_by,
            "note": self.note,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "target_provider": self.target_provider,
            "dialect": self.dialect,
            "source_gold_id": str(self.source_gold_id) if self.source_gold_id else None,
        }
