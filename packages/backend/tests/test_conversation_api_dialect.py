"""Step O3 — /conversations endpoints surface the dialect field.

Backend half of O3: the chat list UI cannot render a dialect badge if
the API doesn't return the column. Locks the JSON schema so a future
refactor of ConversationStore can't silently drop it.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from text2sql.agent.conversation_store import ConversationStore
from text2sql.api import build_app
from text2sql.pipeline import PipelineResult
from text2sql.table_catalog import TableCatalog


# ── Fixtures ─────────────────────────────────────────────────────────────────


class _StubPipeline:
    def answer(self, question: str, *, execute: bool = True, max_rows: int = 100, **_):
        return PipelineResult(
            nl_question=question, sql="SELECT 1", rationale="x",
            rows=[], row_count=0, executed=False, validated=False,
            description="",
        )


class _StubAgentRunner:
    """Just satisfies the truthy check in build_app — no chat in this test."""
    def run(self, *_a, **_kw):  # pragma: no cover — never called here
        raise NotImplementedError


@pytest.fixture
def conv_store(tmp_path):
    s = ConversationStore(f"sqlite:///{tmp_path}/api_convo.sqlite")
    s.ensure_schema()
    yield s
    s.drop_schema()


@pytest.fixture
def client(conv_store):
    cat = TableCatalog(
        data_standard_version="6.1.0", generated_at="2026-01-01T00:00:00Z",
        entries=[],
    )
    app = build_app(
        pipeline=_StubPipeline(), catalog=cat, gold_store=None,
        agent_runner=_StubAgentRunner(), conv_store=conv_store,
    )
    return TestClient(app)


# ── Tests ───────────────────────────────────────────────────────────────────


def test_list_conversations_includes_dialect_per_row(conv_store, client):
    conv_store.create_conversation(title="ms-chat", dialect="mssql")
    conv_store.create_conversation(title="lite-chat", dialect="sqlite")
    conv_store.create_conversation(title="legacy-chat")  # no dialect kwarg

    r = client.get("/conversations")
    assert r.status_code == 200
    body = r.json()
    by_title = {c["title"]: c["dialect"] for c in body["conversations"]}
    assert by_title["ms-chat"] == "mssql"
    assert by_title["lite-chat"] == "sqlite"
    assert by_title["legacy-chat"] == ""


def test_get_conversation_includes_dialect(conv_store, client):
    conv = conv_store.create_conversation(title="t", dialect="postgresql")
    r = client.get(f"/conversations/{conv.id}")
    assert r.status_code == 200
    body = r.json()
    assert body["dialect"] == "postgresql"
    assert body["title"] == "t"
    assert body["messages"] == []


def test_dialect_field_is_always_present_string(conv_store, client):
    """Frontend types `dialect: string` (not optional). API must never
    elide the key — even for legacy rows where the value is empty."""
    conv_store.create_conversation(title="anything")  # legacy row, no dialect
    r = client.get("/conversations").json()
    for row in r["conversations"]:
        assert "dialect" in row
        assert isinstance(row["dialect"], str)
