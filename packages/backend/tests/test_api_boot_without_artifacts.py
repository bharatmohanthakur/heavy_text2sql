"""Step P4 — server must boot when no artifacts have been built yet.

Without this, fresh-clone users hit a chicken-and-egg: cli.serve crashes
on FileNotFoundError loading data/artifacts/table_catalog.json before
the Settings/Rebuild UI is reachable, so they can't bootstrap from the
UI even though that's what the UI is for.

The fix: build_app accepts pipeline=None and catalog=None. /health
reports the onboarding state; catalog endpoints 503 with a useful
message; admin endpoints stay live so Rebuild is reachable.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from text2sql.api import build_app


@pytest.fixture
def client():
    app = build_app(pipeline=None, catalog=None, gold_store=None)
    return TestClient(app)


# ── Health surfaces onboarding state ────────────────────────────────────────


def test_health_reports_unloaded_state_without_failing(client):
    r = client.get("/health")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["catalog_loaded"] is False
    assert body["pipeline_ready"] is False
    assert body["tables"] == 0
    assert body["domains"] == 0


# ── Catalog endpoints fail loud with actionable 503 ────────────────────────


def test_tables_returns_503_with_rebuild_hint(client):
    r = client.get("/tables")
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert "Rebuild" in detail or "ingest" in detail.lower()


def test_table_detail_returns_503(client):
    r = client.get("/tables/edfi.Student")
    assert r.status_code == 503


def test_domains_returns_503(client):
    r = client.get("/domains")
    assert r.status_code == 503


def test_query_endpoint_returns_503(client):
    r = client.post("/query", json={"question": "anything"})
    assert r.status_code == 503
    assert "rebuild" in r.json()["detail"].lower()


# ── Admin endpoints stay live so Rebuild is reachable ──────────────────────


def test_admin_config_works_without_artifacts(client):
    """The whole point: /admin/config must answer so the operator can
    open Settings and start a Rebuild job."""
    r = client.get("/admin/config")
    assert r.status_code == 200, r.text
    body = r.json()
    # Doesn't matter what the values are — only that the endpoint serves.
    assert "target_db" in body
    assert "metadata_db" in body
