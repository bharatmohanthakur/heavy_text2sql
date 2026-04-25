"""Component 11 — FastAPI surface tests.

Uses FastAPI's TestClient to drive the app in-process. Real catalog from
data/artifacts/table_catalog.json; pipeline + gold store stubbed when their
backing services aren't available.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
from fastapi.testclient import TestClient

from text2sql.api import build_app
from text2sql.config import REPO_ROOT, load_config
from text2sql.pipeline import PipelineResult
from text2sql.table_catalog import load_table_catalog


CATALOG_PATH = REPO_ROOT / "data/artifacts/table_catalog.json"


@pytest.fixture(scope="module")
def catalog():
    if not CATALOG_PATH.exists():
        pytest.skip("no catalog; run text2sql build-table-catalog-cmd")
    return load_table_catalog(CATALOG_PATH)


# ── Stub pipeline (lets us exercise routes without real LLM/DB) ──────────────


class _StubPipeline:
    def answer(self, question: str, *, execute: bool = True, max_rows: int = 100, **_):
        return PipelineResult(
            nl_question=question,
            sql="SELECT 1 AS n",
            rationale="stub",
            rows=[{"n": 1}],
            row_count=1,
            executed=True,
            validated=True,
            description="A stub result.",
        )


@pytest.fixture(scope="module")
def app(catalog):
    return build_app(pipeline=_StubPipeline(), catalog=catalog, gold_store=None)


@pytest.fixture(scope="module")
def client(app):
    return TestClient(app)


# ── Health + catalog browse ──────────────────────────────────────────────────


def test_health(client) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["tables"] >= 800


def test_list_tables_default(client) -> None:
    r = client.get("/tables?limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert len(body["tables"]) <= 10
    assert "fqn" in body["tables"][0]


def test_list_tables_filter_by_domain(client) -> None:
    r = client.get("/tables?domain=Enrollment&limit=5")
    assert r.status_code == 200
    for t in r.json()["tables"]:
        assert "Enrollment" in t["domains"]


def test_get_table_404(client) -> None:
    r = client.get("/tables/edfi.NotARealTable")
    assert r.status_code == 404


def test_get_table_known(client) -> None:
    r = client.get("/tables/edfi.Student")
    assert r.status_code == 200
    body = r.json()
    assert body["fqn"] == "edfi.Student"
    assert body["columns"]
    assert body["primary_key"]


def test_list_domains(client) -> None:
    r = client.get("/domains")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 30
    assert all("name" in d and "table_count" in d for d in body["domains"])


# ── Query (with stubbed pipeline) ────────────────────────────────────────────


def test_query_sync(client) -> None:
    r = client.post("/query", json={"question": "How many students?"})
    assert r.status_code == 200
    body = r.json()
    assert body["nl_question"] == "How many students?"
    assert body["sql"] == "SELECT 1 AS n"
    assert body["row_count"] == 1
    assert body["description"]


def test_query_stream_emits_events(client) -> None:
    with client.websocket_connect("/query/stream") as ws:
        ws.send_json({"question": "How many students?"})
        events: list[dict[str, Any]] = []
        while True:
            msg = ws.receive_json()
            events.append(msg)
            if msg.get("event") == "done":
                break
            if msg.get("event") == "error":
                pytest.fail(f"stream error: {msg.get('error')}")
    kinds = [e["event"] for e in events]
    assert "started" in kinds
    assert "sql" in kinds
    assert "rows" in kinds
    assert "done" == kinds[-1]


# ── Gold endpoints absent when no gold_store ─────────────────────────────────


def test_gold_endpoints_absent_without_store(client) -> None:
    r = client.get("/gold")
    assert r.status_code == 404


# ── Gold endpoints present when a real store is wired ────────────────────────


def _has_metadata_db() -> bool:
    import sqlalchemy as sa
    cfg = load_config()
    pw = (
        os.environ.get("METADATA_DB_PASSWORD")
        or os.environ.get("TARGET_DB_PASSWORD")
        or "edfi"
    )
    spec = cfg.metadata_db.model_dump()
    url = (
        f"postgresql+psycopg://{spec['user']}:{pw}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )
    try:
        sa.create_engine(url).connect().close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(
    not (_has_metadata_db() and os.environ.get("AZURE_OPENAI_API_KEY")),
    reason="metadata DB or LLM unavailable",
)
def test_gold_crud_with_real_store(catalog) -> None:
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding

    cfg = load_config()
    pw = (
        os.environ.get("METADATA_DB_PASSWORD")
        or os.environ.get("TARGET_DB_PASSWORD")
        or "edfi"
    )
    spec = cfg.metadata_db.model_dump()
    url = (
        f"postgresql+psycopg://{spec['user']}:{pw}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )
    embedder = build_embedding(cfg.embedding_provider())
    store = GoldStore(url, embedder, catalog=catalog)
    store.ensure_schema()
    app = build_app(pipeline=_StubPipeline(), catalog=catalog, gold_store=store)
    c = TestClient(app)

    # Create
    r = c.post("/gold", json={
        "nl_question": "test gold from api",
        "sql": "SELECT 1 AS n",
        "tables_used": [],
        "author": "api-test",
    })
    assert r.status_code == 201
    rec = r.json()
    gid = rec["id"]
    assert rec["approval_status"] == "pending"

    # List pending
    r = c.get(f"/gold?status=pending")
    assert r.status_code == 200
    assert any(g["id"] == gid for g in r.json()["gold"])

    # Approve
    r = c.post(f"/gold/{gid}/approve", json={"reviewer": "api-test"})
    assert r.status_code == 200
    assert r.json()["approval_status"] == "approved"

    # Reject
    r = c.post(f"/gold/{gid}/reject", json={"reviewer": "api-test", "reason": "test"})
    assert r.status_code == 200
    assert r.json()["approval_status"] == "rejected"
