"""H1 — legacy ALTER TABLE migration for the `dialect` column.

The platform shipped O1 (ConversationRow.dialect) and N4 (GoldSqlRow.
target_provider/dialect/source_gold_id) without a migration story. A
deployment that ran the platform before those PRs has tables that are
missing those columns; `metadata.create_all()` is idempotent for
*tables* but won't ADD COLUMN to existing ones.

These tests simulate a legacy DB by:
  1. building a fresh DB
  2. dropping the table
  3. recreating it with the *old* shape (no dialect column)
  4. inserting a legacy row
  5. calling ensure_schema()
  6. verifying the column came back AND the legacy row is still there
     AND new conversations can write the dialect column

We keep the simulation in SQLite because that's what `metadata_db.kind=
sqlite` ships as the default — the migration code path goes through
SA's dialect-agnostic DDL compiler so the same helper covers PG/MSSQL
too. Driver-specific quirks (SQLite ALTER TABLE limitations, MSSQL
bracketed identifiers) are exercised inside add_missing_columns.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.agent._migrations import add_missing_columns
from text2sql.agent.conversation_store import (
    ConversationMessageRow,
    ConversationRow,
    ConversationStore,
)


def _legacy_conversation_table(engine: sa.Engine) -> None:
    """Drop the modern conversation table and recreate the pre-O1 shape:
    same columns minus `dialect`. This is what an upgraded deployment
    would have on disk before ensure_schema() runs."""
    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS conversation_message"))
        conn.execute(sa.text("DROP TABLE IF EXISTS conversation"))
        conn.execute(sa.text(
            "CREATE TABLE conversation ("
            "  id           CHAR(32) PRIMARY KEY,"
            "  title        TEXT NOT NULL DEFAULT '',"
            "  created_at   DATETIME NOT NULL,"
            "  last_active  DATETIME NOT NULL"
            ")"
        ))
        # Insert a legacy row so we can prove the migration is non-destructive.
        # SA's Uuid(as_uuid=True) on SQLite stores hex(32). Use a fixed valid
        # UUID hex so SA's row hydration doesn't choke when we read it back.
        conn.execute(sa.text(
            "INSERT INTO conversation (id, title, created_at, last_active) "
            "VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'legacy chat', "
            "         '2025-01-01 00:00:00', '2025-01-01 00:00:00')"
        ))


def test_add_missing_columns_adds_dialect_to_legacy_conversation(tmp_path: Path):
    db = tmp_path / "legacy.sqlite"
    engine = sa.create_engine(f"sqlite:///{db}", future=True)

    # 1. Pre-seed a legacy table without the dialect column.
    _legacy_conversation_table(engine)

    inspector = sa.inspect(engine)
    cols_before = {c["name"] for c in inspector.get_columns("conversation")}
    assert "dialect" not in cols_before, "fixture should mimic legacy shape"

    # 2. Run the migration directly — should add ONLY the missing column.
    added = add_missing_columns(engine, ConversationRow.__table__)
    assert added == ["dialect"], added

    # 3. Column exists and the legacy row survives.
    inspector = sa.inspect(engine)
    cols_after = {c["name"] for c in inspector.get_columns("conversation")}
    assert "dialect" in cols_after
    with engine.connect() as conn:
        rows = list(conn.execute(sa.text("SELECT title FROM conversation")))
        assert any("legacy chat" in r[0] for r in rows), rows


def test_add_missing_columns_is_idempotent(tmp_path: Path):
    """Running the helper twice on an up-to-date table must add nothing."""
    db = tmp_path / "uptodate.sqlite"
    engine = sa.create_engine(f"sqlite:///{db}", future=True)
    # Fresh schema — every column already there.
    ConversationRow.metadata.create_all(engine, tables=[ConversationRow.__table__])

    assert add_missing_columns(engine, ConversationRow.__table__) == []
    # Run again — still nothing to add.
    assert add_missing_columns(engine, ConversationRow.__table__) == []


def test_ensure_schema_repairs_legacy_conversation_in_place(tmp_path: Path):
    """The store-level entrypoint must do the same: legacy table loses
    no rows, gains the column, and new writes can use it."""
    db = tmp_path / "legacy_store.sqlite"
    engine = sa.create_engine(f"sqlite:///{db}", future=True)
    _legacy_conversation_table(engine)
    engine.dispose()  # release SQLite file lock so ConversationStore can reopen

    store = ConversationStore(f"sqlite:///{db}")
    store.ensure_schema()

    # Legacy row still readable; new write with dialect lands fine.
    convo = store.create_conversation("new chat", dialect="sqlite")
    assert convo.dialect == "sqlite"
    titles = sorted(c.title for c in store.list_conversations(limit=10))
    assert "legacy chat" in titles
    assert "new chat" in titles


def test_add_missing_columns_skips_when_table_does_not_exist(tmp_path: Path):
    """If the table itself is missing (fresh DB), create_all owns
    creation; the migration helper must not error or invent a table."""
    db = tmp_path / "fresh.sqlite"
    engine = sa.create_engine(f"sqlite:///{db}", future=True)
    # Note: do NOT run create_all — table doesn't exist yet.
    assert add_missing_columns(engine, ConversationRow.__table__) == []
    inspector = sa.inspect(engine)
    assert not inspector.has_table("conversation")
