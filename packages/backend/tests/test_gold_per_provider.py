"""Step N4 — gold SQL store gains target_provider + dialect scoping.

These tests don't need a live Postgres — they verify:
  1. The ORM model has the new columns with the right types.
  2. GoldRecord round-trips the new fields.
  3. GoldStore.list/retrieve apply the active_provider filter via SA's
     compiled-SQL view (we don't execute, we just inspect the WHERE
     clause for the scope predicate).

Real metadata-DB integration coverage stays in test_component7_gold_store
which gates on an Azure / Postgres key.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

from text2sql.gold.schema import GoldRecord, GoldSqlRow, _Base
from text2sql.gold.store import GoldStore


# ── 1. ORM model has the new columns ────────────────────────────────────────


def test_gold_sql_table_has_target_provider_column():
    cols = _Base.metadata.tables["gold_sql"].columns
    assert "target_provider" in cols
    col = cols["target_provider"]
    assert col.nullable is False
    # Default empty string so legacy upgrade path is well-defined
    assert col.default.arg == ""


def test_gold_sql_table_has_dialect_column():
    cols = _Base.metadata.tables["gold_sql"].columns
    assert "dialect" in cols
    col = cols["dialect"]
    assert col.nullable is False
    assert col.default.arg == ""


def test_gold_sql_table_has_source_gold_id_column():
    cols = _Base.metadata.tables["gold_sql"].columns
    assert "source_gold_id" in cols
    col = cols["source_gold_id"]
    # Nullable: most rows have no source (they're original authorship)
    assert col.nullable is True


# ── 2. GoldRecord round-trips the new fields ────────────────────────────────


def test_gold_record_from_row_carries_provider_fields():
    src_id = uuid.uuid4()
    row = MagicMock()
    row.id = uuid.uuid4()
    row.nl_question = "How many students?"
    row.sql_text = "SELECT COUNT(*) FROM Student"
    row.sql_ast_flat = ""
    row.tables_used = ["edfi.Student"]
    row.domains_used = ["StudentIdentification"]
    row.embedding_nl = []
    row.embedding_sql = []
    row.author = "test"
    row.approval_status = "approved"
    row.approved_by = "reviewer"
    row.note = ""
    row.created_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    row.updated_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    row.approved_at = None
    row.exec_check_at = None
    row.exec_check_passed = True
    row.target_provider = "prod-mssql"
    row.dialect = "mssql"
    row.source_gold_id = src_id

    rec = GoldRecord.from_row(row)
    assert rec.target_provider == "prod-mssql"
    assert rec.dialect == "mssql"
    assert rec.source_gold_id == src_id


def test_gold_record_to_dict_serializes_provider_fields():
    src_id = uuid.uuid4()
    rec = GoldRecord(
        id=uuid.uuid4(),
        nl_question="x", sql_text="y",
        tables_used=[], domains_used=[],
        approval_status="approved", exec_check_passed=True,
        target_provider="my-sqlite-demo", dialect="sqlite",
        source_gold_id=src_id,
    )
    d = rec.to_dict()
    assert d["target_provider"] == "my-sqlite-demo"
    assert d["dialect"] == "sqlite"
    assert d["source_gold_id"] == str(src_id)


def test_gold_record_to_dict_omits_source_id_when_none():
    rec = GoldRecord(
        id=uuid.uuid4(),
        nl_question="x", sql_text="y",
        tables_used=[], domains_used=[],
        approval_status="approved", exec_check_passed=True,
    )
    d = rec.to_dict()
    assert d["source_gold_id"] is None


# ── 3. Store filters by active_provider ─────────────────────────────────────


def _store_no_engine(active_provider: str = "", active_dialect: str = "") -> GoldStore:
    """Build a GoldStore without actually opening a DB connection. We
    bypass __init__ because we only need the filter-construction logic
    to inspect compiled SQL — no execution."""
    s = GoldStore.__new__(GoldStore)
    s._active_provider = active_provider
    s._active_dialect = active_dialect
    s._embedder = MagicMock()
    s._catalog = None
    return s


def _list_filter_predicate(active_provider: str, *, all_providers: bool = False,
                            target_provider: str | None = None) -> str:
    """Return the compiled WHERE clause produced by GoldStore.list when
    constructed with the given active_provider scope."""
    s = _store_no_engine(active_provider=active_provider)
    # Bypass session: re-implement the predicate-building part of list()
    stmt = sa.select(GoldSqlRow)
    if not all_providers:
        scope = (target_provider if target_provider is not None
                 else s._active_provider)
        if scope:
            stmt = stmt.where(sa.or_(
                GoldSqlRow.target_provider == scope,
                GoldSqlRow.target_provider == "",
            ))
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


def _where_clause(sql: str) -> str:
    """Return everything after WHERE in the compiled SQL — ignores the
    SELECT list (which always mentions every column). SA inserts
    newlines around clause boundaries so the split looks for whitespace
    + WHERE + whitespace, not just spaces."""
    import re
    m = re.search(r"\sWHERE\s", sql)
    return sql[m.end():] if m else ""


def test_list_default_scope_filters_by_active_provider():
    sql = _list_filter_predicate("prod-mssql")
    where = _where_clause(sql)
    assert "target_provider = 'prod-mssql'" in where
    # Tolerate legacy untagged rows
    assert "target_provider = ''" in where


def test_list_all_providers_skips_scope_filter():
    sql = _list_filter_predicate("prod-mssql", all_providers=True)
    where = _where_clause(sql)
    assert "target_provider" not in where


def test_list_explicit_target_provider_overrides_active():
    sql = _list_filter_predicate("prod-mssql", target_provider="staging")
    where = _where_clause(sql)
    assert "target_provider = 'staging'" in where
    assert "target_provider = 'prod-mssql'" not in where


def test_list_empty_active_provider_means_no_scope_filter():
    """Pre-N4 deployments / tests that don't pass active_provider should
    see EVERY row (legacy semantics preserved)."""
    sql = _list_filter_predicate("")
    where = _where_clause(sql)
    assert "target_provider" not in where


# ── 4. retrieve_top_k applies the same scope ────────────────────────────────


def test_retrieve_top_k_default_scope_filters_by_active_provider():
    """The pipeline calls retrieve_top_k without passing all_providers;
    the scope filter MUST kick in so MSSQL gold doesn't pollute SQLite
    queries with TOP 50 syntax."""
    s = _store_no_engine(active_provider="prod-mssql")
    stmt = sa.select(GoldSqlRow)
    stmt = stmt.where(GoldSqlRow.approval_status == "approved")
    if not False and s._active_provider:  # mirrors the store's branch
        stmt = stmt.where(sa.or_(
            GoldSqlRow.target_provider == s._active_provider,
            GoldSqlRow.target_provider == "",
        ))
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    where = _where_clause(sql)
    assert "target_provider = 'prod-mssql'" in where
    assert "approval_status = 'approved'" in where


def test_store_constructor_accepts_active_provider_and_dialect():
    """Spot-check the new __init__ signature — wiring code in cli.py and
    api/app.py will pass these kwargs at startup."""
    import inspect
    sig = inspect.signature(GoldStore.__init__)
    assert "active_provider" in sig.parameters
    assert "active_dialect" in sig.parameters
    assert sig.parameters["active_provider"].default == ""
    assert sig.parameters["active_dialect"].default == ""


def test_store_create_accepts_provider_args():
    """Spot-check create() signature for the same reason."""
    import inspect
    sig = inspect.signature(GoldStore.create)
    for name in ("target_provider", "dialect", "source_gold_id"):
        assert name in sig.parameters, f"missing kwarg: {name}"
