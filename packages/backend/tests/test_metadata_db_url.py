"""Step O2 — _metadata_sa_url branches on metadata_db.kind.

Three dialects supported: postgresql, mssql, sqlite. SQLite makes the
zero-infra demo work — same folder of files for target + metadata, no
Docker required.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.cli import _metadata_sa_url
from text2sql.config import REPO_ROOT, AppConfig


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _cfg(metadata_kind: str, **metadata_extra) -> AppConfig:
    return AppConfig.model_validate({
        "ed_fi": {
            "data_standard_version": "6.1.0",
            "extensions": [],
            "cache_dir": "data/edfi",
            "artifact_dir": "data/artifacts",
            "github": {
                "ods_repo": "x", "extensions_repo": "y",
            },
        },
        "llm": {"primary": "x", "providers": {"x": {"kind": "openai"}}},
        "embeddings": {"primary": "x", "providers": {"x": {"kind": "openai"}}},
        "vector_store": {"primary": "x", "providers": {"x": {"kind": "faiss"}}},
        "target_db": {
            "primary": "demo",
            "providers": {"demo": {"kind": "postgresql"}},
        },
        "metadata_db": {"kind": metadata_kind, **metadata_extra},
        "logging": {"level": "INFO", "format": "json"},
    })


# ── Postgres path (regression — must keep working) ─────────────────────────


def test_postgresql_url_uses_psycopg_driver(monkeypatch):
    monkeypatch.setenv("PG_PASSWORD", "secret")
    cfg = _cfg("postgresql",
                host="db.example.com", port=5432, database="meta",
                user="meta_user", password_env="PG_PASSWORD")
    url = _metadata_sa_url(cfg)
    assert url == "postgresql+psycopg://meta_user:secret@db.example.com:5432/meta"


def test_postgresql_default_port_5432(monkeypatch):
    monkeypatch.setenv("PG_PASSWORD", "")
    cfg = _cfg("postgresql",
                host="h", database="d", user="u", password_env="PG_PASSWORD")
    url = _metadata_sa_url(cfg)
    assert ":5432/" in url


# ── MSSQL path (new) ───────────────────────────────────────────────────────


def test_mssql_url_uses_pymssql_with_tds_version(monkeypatch):
    monkeypatch.setenv("MSSQL_PASSWORD", "Pass123")
    cfg = _cfg("mssql",
                host="sql.example.com", port=1433, database="meta",
                user="sa", password_env="MSSQL_PASSWORD")
    url = _metadata_sa_url(cfg)
    assert url == "mssql+pymssql://sa:Pass123@sql.example.com:1433/meta?tds_version=7.4"


def test_mssql_default_port_1433(monkeypatch):
    monkeypatch.setenv("MSSQL_PASSWORD", "")
    cfg = _cfg("mssql", host="h", database="d", user="u",
                password_env="MSSQL_PASSWORD")
    url = _metadata_sa_url(cfg)
    assert ":1433/" in url


# ── SQLite path (new — zero-infra) ─────────────────────────────────────────


def test_sqlite_url_with_absolute_path(tmp_path):
    abs_path = tmp_path / "metadata.sqlite"
    cfg = _cfg("sqlite", path=str(abs_path))
    url = _metadata_sa_url(cfg)
    assert url == f"sqlite:///{abs_path}"


def test_sqlite_url_resolves_repo_relative_path():
    """A repo-relative path should land under REPO_ROOT so the same
    config works whether run from the repo root or a sub-dir."""
    cfg = _cfg("sqlite", path="data/artifacts/test.sqlite")
    url = _metadata_sa_url(cfg)
    expected = REPO_ROOT / "data/artifacts/test.sqlite"
    assert url == f"sqlite:///{expected}"


def test_sqlite_url_passes_memory_through_unchanged():
    """`:memory:` is a valid SA URL spelling and must not be path-joined."""
    cfg = _cfg("sqlite", path=":memory:")
    url = _metadata_sa_url(cfg)
    assert url == "sqlite:///:memory:"


def test_sqlite_url_raises_when_path_missing():
    """A SQLite metadata config without `path` is operator error — fail
    loudly rather than producing an empty URL that points nowhere."""
    cfg = _cfg("sqlite")  # no path
    with pytest.raises(RuntimeError) as ei:
        _metadata_sa_url(cfg)
    assert "path" in str(ei.value).lower()


# ── Unknown kind ───────────────────────────────────────────────────────────


def test_unknown_kind_raises_with_actionable_message():
    cfg = _cfg("snowflake")
    with pytest.raises(RuntimeError) as ei:
        _metadata_sa_url(cfg)
    assert "snowflake" in str(ei.value)
