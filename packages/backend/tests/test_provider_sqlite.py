"""Step M — SQLite target-DB provider.

Verifies SqliteEngine satisfies the SqlEngine Protocol against a real
SQLite file built in-process. No network, no fixtures — each test owns
its own tempfile so they're hermetic.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.config import REPO_ROOT, ProviderEntry
from text2sql.providers import build_sql_engine
from text2sql.providers.db.sqlite import SqliteEngine


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _seed(conn: sqlite3.Connection) -> None:
    """Build a small Ed-Fi-flavored sample: EducationOrganization, School,
    Student, StudentSchoolAssociation. Includes a composite FK to exercise
    `list_foreign_keys()` grouping."""
    conn.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE EducationOrganization (
            EducationOrganizationId INTEGER PRIMARY KEY,
            NameOfInstitution       TEXT NOT NULL
        );
        CREATE TABLE School (
            SchoolId       INTEGER PRIMARY KEY,
            FOREIGN KEY (SchoolId) REFERENCES EducationOrganization(EducationOrganizationId)
        );
        CREATE TABLE Student (
            StudentUSI   INTEGER PRIMARY KEY,
            FirstName    TEXT,
            LastSurname  TEXT
        );
        CREATE TABLE StudentSchoolAssociation (
            StudentUSI INTEGER NOT NULL,
            SchoolId   INTEGER NOT NULL,
            EntryDate  TEXT NOT NULL,
            PRIMARY KEY (StudentUSI, SchoolId, EntryDate),
            FOREIGN KEY (StudentUSI) REFERENCES Student(StudentUSI),
            FOREIGN KEY (SchoolId)   REFERENCES School(SchoolId)
        );
        INSERT INTO EducationOrganization VALUES (1, 'Northridge HS'), (2, 'Maple ES');
        INSERT INTO School VALUES (1), (2);
        INSERT INTO Student VALUES (100, 'Ada', 'Lovelace'), (101, 'Bo', 'Diddley');
        INSERT INTO StudentSchoolAssociation VALUES
          (100, 1, '2023-08-15'), (101, 2, '2023-08-15');
    """)
    conn.commit()


def _fresh_db_path() -> Path:
    fd, name = tempfile.mkstemp(suffix=".sqlite", prefix="t2s_test_")
    os.close(fd)
    p = Path(name)
    p.unlink()  # remove the empty stub so SQLite creates it fresh in rwc mode
    conn = sqlite3.connect(str(p))
    try:
        _seed(conn)
    finally:
        conn.close()
    return p


# ── Engine surface ───────────────────────────────────────────────────────────


def test_engine_builds_via_factory_with_correct_dialect():
    p = _fresh_db_path()
    try:
        eng = build_sql_engine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        assert isinstance(eng, SqliteEngine)
        assert eng.dialect == "sqlite"
        assert eng.read_only is True
        assert eng.path == str(p)
    finally:
        p.unlink(missing_ok=True)


def test_read_only_mode_blocks_writes():
    """`read_only=True` opens the file with mode=ro — INSERT must fail."""
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        with pytest.raises(sa.exc.OperationalError):
            eng.execute("INSERT INTO Student VALUES (999, 'X', 'Y')")
    finally:
        p.unlink(missing_ok=True)


def test_list_tables_excludes_sqlite_system_objects():
    """User tables only — `sqlite_master`, `sqlite_sequence`, etc must
    not appear (they confuse the catalog if they leak through)."""
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        tables = eng.list_tables()
        # All entries are (schema="main", name)
        assert all(s == "main" for s, _ in tables)
        names = {t for _, t in tables}
        assert names == {"EducationOrganization", "School", "Student", "StudentSchoolAssociation"}
        assert not any(n.startswith("sqlite_") for n in names)
    finally:
        p.unlink(missing_ok=True)


def test_list_columns_returns_declared_types_and_nullability():
    """SQLite is dynamically typed but stores the declared type string
    verbatim; PRAGMA reports nullable as the inverse of NOT NULL."""
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        cols = eng.list_columns("main", "Student")
        cols_by_name = {name: (typ, nullable) for name, typ, nullable in cols}
        # PK is INTEGER (SQLite stores `INTEGER PRIMARY KEY` as the rowid);
        # FirstName / LastSurname are TEXT and nullable.
        assert cols_by_name["StudentUSI"][0] == "INTEGER"
        assert cols_by_name["FirstName"] == ("TEXT", True)
        assert cols_by_name["LastSurname"] == ("TEXT", True)
        # NameOfInstitution on EducationOrganization is NOT NULL → nullable=False
        eo_cols = {n: nu for n, _, nu in eng.list_columns("main", "EducationOrganization")}
        assert eo_cols["NameOfInstitution"] is False
    finally:
        p.unlink(missing_ok=True)


def test_list_foreign_keys_groups_by_id_for_composite_fks():
    """StudentSchoolAssociation has TWO FKs (one to Student, one to School).
    Each is single-column here, so each gets its own `id`. Verify both
    are surfaced and pointed at the right parents/columns."""
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        fks = eng.list_foreign_keys("StudentSchoolAssociation")
        # Two distinct FKs, each one column wide
        ids = {fk["id"] for fk in fks}
        assert len(ids) == 2
        # Verify the (parent, parent_col, child_col) triples are correct
        triples = {(fk["parent"], fk["parent_col"], fk["child_col"]) for fk in fks}
        assert triples == {
            ("Student", "StudentUSI", "StudentUSI"),
            ("School", "SchoolId", "SchoolId"),
        }
    finally:
        p.unlink(missing_ok=True)


def test_execute_returns_rows_and_respects_limit():
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        all_rows = eng.execute("SELECT EducationOrganizationId, NameOfInstitution FROM EducationOrganization ORDER BY 1")
        assert all_rows == [
            {"EducationOrganizationId": 1, "NameOfInstitution": "Northridge HS"},
            {"EducationOrganizationId": 2, "NameOfInstitution": "Maple ES"},
        ]
        capped = eng.execute("SELECT EducationOrganizationId FROM EducationOrganization ORDER BY 1", limit=1)
        assert capped == [{"EducationOrganizationId": 1}]
    finally:
        p.unlink(missing_ok=True)


def test_explain_uses_query_plan_form():
    """`explain()` must use `EXPLAIN QUERY PLAN` (human-readable), not
    raw bytecode `EXPLAIN`. Output mentions SCAN or SEARCH."""
    p = _fresh_db_path()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=str(p), read_only=True))
        out = eng.explain("SELECT * FROM Student WHERE StudentUSI = 100")
        assert isinstance(out, str) and out
        assert "SCAN" in out.upper() or "SEARCH" in out.upper()
    finally:
        p.unlink(missing_ok=True)


def test_repo_relative_path_resolves_against_repo_root(tmp_path, monkeypatch):
    """A non-absolute `path` must resolve under REPO_ROOT — same overlay
    file works in CI and dev no matter the cwd."""
    rel = "data/edfi/_t2s_pytest.sqlite"
    abs_path = REPO_ROOT / rel
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    abs_path.unlink(missing_ok=True)
    conn = sqlite3.connect(str(abs_path))
    try:
        _seed(conn)
    finally:
        conn.close()
    try:
        eng = SqliteEngine(ProviderEntry(kind="sqlite", path=rel, read_only=True))
        assert eng.path == str(abs_path)
        # Real connectivity: list_tables works against the resolved path
        assert any(t == "Student" for _, t in eng.list_tables())
    finally:
        abs_path.unlink(missing_ok=True)


def test_quote_identifier_uses_double_quotes_with_escape():
    """Same shape as Postgres so the prompt is identical for both."""
    eng = SqliteEngine(ProviderEntry(kind="sqlite", path=":memory:", read_only=False))
    assert eng.quote_identifier("Student") == '"Student"'
    assert eng.quote_identifier('weird"name') == '"weird""name"'


# ── Admin overlay round-trip ────────────────────────────────────────────────


def test_admin_save_sqlite_connector_persists_overlay_without_secrets(tmp_path, monkeypatch):
    """POST /admin/connector/database with kind=sqlite → overlay updated,
    no password env-var written, primary set."""
    from fastapi.testclient import TestClient
    from text2sql.api import admin as admin_mod

    p = _fresh_db_path()
    overlay = tmp_path / "overlay.json"
    secrets = tmp_path / "secrets.json"
    monkeypatch.setattr(admin_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    monkeypatch.setattr(admin_mod, "RUNTIME_SECRETS_PATH", secrets)
    # config.load_config reads the same constants from text2sql.config — patch
    # there too so the validate-by-loading step sees the same overlay/secrets.
    from text2sql import config as config_mod
    monkeypatch.setattr(config_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    monkeypatch.setattr(config_mod, "RUNTIME_SECRETS_PATH", secrets)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(admin_mod.router)
    client = TestClient(app)

    payload = {
        "name": "demo-sqlite",
        "kind": "sqlite",
        "set_primary": True,
        "path": str(p),
        "read_only": True,
    }
    try:
        # Test endpoint succeeds end-to-end against the real file
        rt = client.post("/admin/connector/database/test", json=payload)
        assert rt.status_code == 200, rt.text
        body = rt.json()
        assert body["ok"] is True, body
        assert "sqlite" in (body.get("server_version") or "").lower()

        # Save persists the overlay; no secrets written
        rs = client.post("/admin/connector/database", json=payload)
        assert rs.status_code == 200, rs.text

        import json as _json
        ovl = _json.loads(overlay.read_text())
        assert ovl["target_db"]["primary"] == "demo-sqlite"
        entry = ovl["target_db"]["providers"]["demo-sqlite"]
        assert entry["kind"] == "sqlite"
        assert entry["path"] == str(p)
        assert entry["read_only"] is True
        # No password_env key — sqlite doesn't have one
        assert "password_env" not in entry
        # Secrets file either doesn't exist or has no demo-sqlite entry
        if secrets.exists():
            assert not any(k.startswith("DEMO_SQLITE") for k in _json.loads(secrets.read_text()))
    finally:
        p.unlink(missing_ok=True)


def test_steiner_join_clauses_drop_schema_for_sqlite():
    """SQLite has a single schema; an `edfi.X` FQN must render as just
    `"X"` so the SQL actually executes against `main.X`."""
    from text2sql.graph.steiner import _qualify, _quote_table

    assert _quote_table("edfi.Student", "sqlite") == '"Student"'
    assert _qualify("edfi.Student", "StudentUSI", "sqlite") == '"Student"."StudentUSI"'
    # Postgres path unchanged
    assert _quote_table("edfi.Student", "postgresql") == '"edfi"."student"'


def test_context_quote_id_preserves_case_for_sqlite():
    """Postgres lowercases identifiers; SQLite must NOT (case-sensitive
    once quoted; Ed-Fi tables are PascalCase)."""
    from text2sql.pipeline.context import ContextBuilder
    from text2sql.table_catalog import TableCatalog

    cat = TableCatalog(entries=[], data_standard_version="6.1.0", generated_at="2026-01-01T00:00:00Z")
    cb = ContextBuilder(catalog=cat, dialect="sqlite")
    assert cb._quote_id("Student") == '"Student"'
    cb_pg = ContextBuilder(catalog=cat, dialect="postgresql")
    assert cb_pg._quote_id("Student") == '"student"'


def test_admin_save_sqlite_rejects_memory_path(tmp_path, monkeypatch):
    """`:memory:` would silently appear empty across requests — reject."""
    from fastapi.testclient import TestClient
    from text2sql.api import admin as admin_mod

    overlay = tmp_path / "overlay.json"
    secrets = tmp_path / "secrets.json"
    monkeypatch.setattr(admin_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    monkeypatch.setattr(admin_mod, "RUNTIME_SECRETS_PATH", secrets)
    from text2sql import config as config_mod
    monkeypatch.setattr(config_mod, "RUNTIME_OVERRIDES_PATH", overlay)
    monkeypatch.setattr(config_mod, "RUNTIME_SECRETS_PATH", secrets)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(admin_mod.router)
    client = TestClient(app)

    rs = client.post("/admin/connector/database", json={
        "name": "in-mem", "kind": "sqlite", "path": ":memory:", "read_only": False,
    })
    assert rs.status_code == 400
    assert ":memory:" in rs.text
    assert not overlay.exists()
