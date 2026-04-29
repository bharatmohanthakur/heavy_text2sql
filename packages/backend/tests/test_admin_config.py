"""Admin config endpoints — overlay layering, redaction, secret-rejection.

These tests exercise the FastAPI routes via TestClient with a stubbed
runtime_overrides path so the real artifact stays untouched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import text2sql.config as cfg_mod
from text2sql.api.admin import router


@pytest.fixture
def app(tmp_path: Path, monkeypatch):
    """A FastAPI app with the admin router mounted, pointed at a temp
    runtime_overrides.json so writes don't touch the real artifact."""
    overlay = tmp_path / "runtime_overrides.json"
    monkeypatch.setattr(cfg_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    # The admin module captured the path at import time — patch there too.
    import text2sql.api.admin as admin_mod
    monkeypatch.setattr(admin_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    a = FastAPI()
    a.include_router(router)
    return a


@pytest.fixture
def client(app):
    return TestClient(app)


# ── GET /admin/config — redaction ──────────────────────────────────────────


def test_get_config_redacts_passwords_but_not_env_pointers(client):
    r = client.get("/admin/config")
    assert r.status_code == 200, r.text
    body = r.json()
    # Top-level shape
    for k in ("llm", "embeddings", "vector_store", "target_db", "metadata_db", "overlay", "env_present"):
        assert k in body, f"missing key {k}"

    # Walk every provider entry — there should be NO raw secret left.
    for section in ("llm", "embeddings", "vector_store", "target_db"):
        for prov in body[section].get("providers", {}).values():
            for k, v in prov.items():
                if k.endswith("_env"):
                    # Env-var NAME is not a secret — must remain visible
                    assert v and v != "***", f"{section}.{k} should NOT be redacted"
                if any(h in k.lower() for h in ("api_key", "password", "secret", "token")) and not k.endswith("_env"):
                    assert v == "***", f"{section}.{k} should be redacted"


def test_get_config_includes_env_presence_map(client):
    r = client.get("/admin/config")
    body = r.json()
    # Every *_env value referenced in any provider should appear in env_present.
    referenced: set[str] = set()
    for section in ("llm", "embeddings", "vector_store", "target_db"):
        for prov in body[section].get("providers", {}).values():
            for k, v in prov.items():
                if k.endswith("_env") and isinstance(v, str):
                    referenced.add(v)
    for name in referenced:
        assert name in body["env_present"], f"{name} should be in env_present map"
        assert isinstance(body["env_present"][name], bool)


# ── POST /admin/config — overlay write-through + secret rejection ──────────


def test_patch_writes_overlay_and_changes_primary(client, tmp_path):
    # Switch the primary embedding provider
    r = client.post("/admin/config", json={
        "embeddings": {"primary": "minilm-l6-local"},
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["embeddings"]["primary"] == "minilm-l6-local"
    # Overlay file mirrors the patch
    assert body["overlay"] == {"embeddings": {"primary": "minilm-l6-local"}}


def test_patch_deep_merges_with_existing_overlay(client):
    client.post("/admin/config", json={"embeddings": {"primary": "minilm-l6-local"}})
    # A second patch on a different section should NOT clobber the first.
    r2 = client.post("/admin/config", json={"llm": {"primary": "anthropic-sonnet"}})
    assert r2.status_code == 200
    overlay = r2.json()["overlay"]
    assert overlay == {
        "embeddings": {"primary": "minilm-l6-local"},
        "llm": {"primary": "anthropic-sonnet"},
    }


def test_patch_refuses_secret_values(client):
    """A POST that puts a raw API key in the body must be rejected."""
    r = client.post("/admin/config", json={
        "llm": {"providers": {"openai-gpt-4o": {"api_key": "sk-real-secret"}}},
    })
    assert r.status_code == 400
    assert "secret" in r.text.lower() or "api_key" in r.text.lower()
    # Same for password under target_db
    r2 = client.post("/admin/config", json={
        "target_db": {"providers": {"foo": {"password": "hunter2"}}},
    })
    assert r2.status_code == 400


def test_patch_allows_password_env_pointer(client):
    """*_env keys hold the env var NAME, not a secret — these are OK."""
    r = client.post("/admin/config", json={
        "target_db": {"providers": {"my-ods": {"password_env": "MY_DB_PASSWORD"}}},
    })
    # If the resulting overlay invalidates the config schema we'll get 400,
    # but the admin layer must NOT reject this on secret-detection grounds.
    if r.status_code == 400:
        assert "secret" not in r.text.lower()


def test_patch_rejects_overlay_that_invalidates_config(client):
    """Bad shape (e.g. unknown provider as primary) must be rejected before
    persisting, so the next process boot can still load_config()."""
    r = client.post("/admin/config", json={
        "embeddings": {"primary": "absolutely-not-a-real-provider"},
    })
    # Pydantic validation should reject this since the provider isn't in
    # the providers map.
    assert r.status_code in (400, 422), r.text


# ── /admin/test_metadata_db — multi-dialect (O4) ───────────────────────────


def test_test_metadata_db_succeeds_for_sqlite_overlay(tmp_path, client):
    """Overlay metadata_db kind=sqlite + a tmp file path; the multi-
    dialect endpoint must connect, run sqlite_version(), and return ok."""
    db_path = tmp_path / "metadata.sqlite"
    # Write the overlay directly to the path the fixture monkeypatched.
    import text2sql.api.admin as admin_mod
    overlay_path = Path(admin_mod.RUNTIME_OVERRIDES_PATH)
    overlay_path.write_text(json.dumps({
        "metadata_db": {"kind": "sqlite", "path": str(db_path)},
    }, indent=2))

    r = client.post("/admin/test_metadata_db")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True, body
    assert body["server_version"].startswith("SQLite ")
    assert body["elapsed_ms"] is not None and body["elapsed_ms"] > 0
    # SQLite on-the-fly creates the file on first connection
    assert db_path.exists()


def test_test_metadata_db_reports_sqlite_missing_path_clearly(client):
    """Misconfigured overlay (sqlite kind but no path) should not crash —
    the endpoint must respond 200 with ok=false + a useful error string."""
    import text2sql.api.admin as admin_mod
    overlay_path = Path(admin_mod.RUNTIME_OVERRIDES_PATH)
    overlay_path.write_text(json.dumps({
        "metadata_db": {"kind": "sqlite"},  # no path
    }, indent=2))

    r = client.post("/admin/test_metadata_db")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert "path" in (body["error"] or "").lower()
