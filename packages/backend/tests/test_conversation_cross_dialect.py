"""Step O1 — ConversationStore on SQLite + Postgres dialect column.

Proves the metadata-DB type strategy works: dialect-aware UUID + JSON
types let the same ConversationStore back onto an in-process SQLite
file, with the per-conversation `dialect` column round-tripping.

Real Postgres coverage stays in higher-level integration tests; this
file gives us hermetic CI coverage for the SA 2.0 type variants.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.agent.conversation_store import (
    Conversation,
    ConversationMessage,
    ConversationMessageRow,
    ConversationRow,
    ConversationStore,
    _Base,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def sqlite_store(tmp_path):
    """An in-process SQLite-backed ConversationStore, tables created."""
    db_path = tmp_path / "convo.sqlite"
    s = ConversationStore(f"sqlite:///{db_path}")
    s.ensure_schema()
    yield s
    s.drop_schema()


# ── Schema lands cleanly on SQLite ──────────────────────────────────────────


def test_ensure_schema_creates_tables_on_sqlite(sqlite_store):
    """The whole point of O1: cross-dialect types let SA emit valid
    SQLite DDL. Pre-O1 this raised because JSONB / UUID don't compile
    on the SQLite dialect."""
    insp = sa.inspect(sqlite_store._engine)
    tables = set(insp.get_table_names())
    assert "conversation" in tables
    assert "conversation_message" in tables


def test_conversation_table_has_dialect_column(sqlite_store):
    insp = sa.inspect(sqlite_store._engine)
    cols = {c["name"]: c for c in insp.get_columns("conversation")}
    assert "dialect" in cols
    # On SQLite, String(32) compiles to VARCHAR(32). The driver normalizes
    # to upper- or lower-case depending on version; just match prefix.
    assert "VARCHAR" in cols["dialect"]["type"].compile().upper()


def test_conversation_message_uses_json_for_tool_calls_on_sqlite(sqlite_store):
    """JSONB → JSON-as-TEXT on SQLite via SA's with_variant."""
    insp = sa.inspect(sqlite_store._engine)
    cols = {c["name"]: c for c in insp.get_columns("conversation_message")}
    # SQLite renders JSON as the literal type name "JSON" via SA's
    # generic JSON type — confirms the variant fall-through worked.
    assert "tool_calls" in cols
    assert "JSON" in cols["tool_calls"]["type"].compile().upper()


# ── CRUD round-trip ─────────────────────────────────────────────────────────


def test_create_conversation_persists_dialect_field(sqlite_store):
    conv = sqlite_store.create_conversation(title="My SQLite chat",
                                              dialect="sqlite")
    assert conv.dialect == "sqlite"
    assert conv.title == "My SQLite chat"
    assert isinstance(conv.id, uuid.UUID)

    # Fetch it back via the public API
    loaded = sqlite_store.get_conversation(conv.id)
    assert loaded is not None
    assert loaded.dialect == "sqlite"
    assert loaded.title == "My SQLite chat"


def test_create_conversation_defaults_dialect_to_empty(sqlite_store):
    """Backwards compat: callers that don't pass dialect get '' so
    legacy code paths don't break."""
    conv = sqlite_store.create_conversation(title="legacy-style")
    assert conv.dialect == ""


def test_list_conversations_returns_dialect_per_row(sqlite_store):
    sqlite_store.create_conversation(title="a", dialect="mssql")
    sqlite_store.create_conversation(title="b", dialect="sqlite")
    sqlite_store.create_conversation(title="c", dialect="postgresql")

    convs = sqlite_store.list_conversations(limit=10)
    by_title = {c.title: c.dialect for c in convs}
    assert by_title["a"] == "mssql"
    assert by_title["b"] == "sqlite"
    assert by_title["c"] == "postgresql"


def test_append_message_with_tool_calls_round_trips_on_sqlite(sqlite_store):
    """The JSON variant must survive write+read on SQLite (this is
    where pre-O1 code would have died — JSONB is unknown to SQLite)."""
    conv = sqlite_store.create_conversation(title="t", dialect="sqlite")
    sqlite_store.append_message(conv.id, role="user", content="How many?")
    sqlite_store.append_message(
        conv.id, role="assistant", content="",
        tool_calls=[{
            "id": "call_1", "type": "function",
            "function": {"name": "run_sql", "arguments": '{"sql": "SELECT 1"}'},
        }],
    )

    msgs = sqlite_store.history(conv.id)
    assert len(msgs) == 2
    user_msg, asst_msg = msgs
    assert user_msg.role == "user"
    assert user_msg.content == "How many?"
    assert asst_msg.role == "assistant"
    assert asst_msg.tool_calls is not None
    assert asst_msg.tool_calls[0]["function"]["name"] == "run_sql"


def test_uuid_type_round_trips_as_python_uuid_on_sqlite(sqlite_store):
    """SA 2.0 sa.Uuid stores as CHAR(32) on SQLite but yields uuid.UUID
    on read — the application layer never sees the wire encoding."""
    conv = sqlite_store.create_conversation(title="x", dialect="sqlite")
    assert isinstance(conv.id, uuid.UUID)
    loaded = sqlite_store.get_conversation(conv.id)
    assert loaded is not None
    assert isinstance(loaded.id, uuid.UUID)
    assert loaded.id == conv.id


# ── Legacy-row tolerance ────────────────────────────────────────────────────


def test_legacy_row_with_no_dialect_loads_as_empty_string(sqlite_store):
    """A row whose dialect column is empty string (the default before
    callers were updated to pass dialect=) must still load cleanly."""
    # Bypass the ORM default by setting dialect="" explicitly — same
    # state as a row that landed before the column had a populated value
    conv = sqlite_store.create_conversation(title="legacy", dialect="")
    loaded = sqlite_store.get_conversation(conv.id)
    assert loaded is not None
    assert loaded.dialect == ""
    assert loaded.title == "legacy"


# ── Drop schema is well-scoped ─────────────────────────────────────────────


def test_drop_schema_drops_only_conversation_tables(tmp_path):
    """The new drop_all expects to receive only the conversation tables.
    Verifies a custom non-conversation table coexisting in the same DB
    survives drop_schema()."""
    s = ConversationStore(f"sqlite:///{tmp_path}/c.sqlite")
    s.ensure_schema()
    # Add a foreign table to the same connection
    with s._engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE other_thing (k TEXT PRIMARY KEY)"))
    s.drop_schema()
    insp = sa.inspect(s._engine)
    tables = set(insp.get_table_names())
    assert "conversation" not in tables
    assert "conversation_message" not in tables
    assert "other_thing" in tables, "drop_schema must not touch foreign tables"
