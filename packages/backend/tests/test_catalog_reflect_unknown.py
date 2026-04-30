"""Step P7 — reflect_unknown_tables turns live-DB tables not in the
ApiModel into best-effort catalog entries.

Hermetic test on a SQLite file. Stages a tiny "DB" with a mix of
tables — some in the simulated known_fqns set, some not — and checks
that:

* known tables are NOT re-emitted (no double-counting).
* unknown tables come back with reflected columns + PK + sample rows.
* `domains == ["Other"]` so downstream routing doesn't pretend they
  belong to a real Ed-Fi domain.
* FK records pair child/parent across the unknown tables.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.config import ProviderEntry
from text2sql.providers.db.sqlite import SqliteEngine
from text2sql.table_catalog.catalog_builder import reflect_unknown_tables


# ── Fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
def engine(tmp_path: Path):
    """A SQLite file with three tables:
      Student              — known-to-ApiModel; should be skipped.
      DistrictReport       — unknown; should be reflected.
      DistrictReportDetail — unknown; FK to DistrictReport.
    """
    db_path = tmp_path / "live.sqlite"
    raw = sa.create_engine(f"sqlite:///{db_path}", future=True)
    with raw.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE Student (StudentUSI INTEGER PRIMARY KEY, FirstName TEXT)"
        ))
        conn.execute(sa.text(
            "CREATE TABLE DistrictReport ("
            "  ReportId   INTEGER PRIMARY KEY,"
            "  Title      TEXT NOT NULL,"
            "  CreatedAt  TEXT"
            ")"
        ))
        conn.execute(sa.text(
            "CREATE TABLE DistrictReportDetail ("
            "  DetailId   INTEGER PRIMARY KEY,"
            "  ReportId   INTEGER NOT NULL,"
            "  Note       TEXT,"
            "  FOREIGN KEY (ReportId) REFERENCES DistrictReport(ReportId)"
            ")"
        ))
        conn.execute(sa.text(
            "INSERT INTO DistrictReport (ReportId, Title, CreatedAt) "
            "VALUES (1, 'Q1 Snapshot', '2026-01-01')"
        ))
    raw.dispose()  # release the SQLite file lock before SqliteEngine opens it

    spec = ProviderEntry.model_validate({
        "kind": "sqlite", "path": str(db_path), "read_only": False,
    })
    return SqliteEngine(spec)


# ── Tests ───────────────────────────────────────────────────────────────────


def test_known_tables_are_skipped(engine):
    """edfi.Student is in the known_fqns set, so it must not appear in
    the reflected output."""
    known = {"edfi.Student"}
    extras, _ = reflect_unknown_tables(engine, known)
    fqns = {e.fqn for e in extras}
    assert "edfi.Student" not in fqns
    assert "edfi.DistrictReport" in fqns
    assert "edfi.DistrictReportDetail" in fqns


def test_unknown_table_columns_reflected(engine):
    extras, _ = reflect_unknown_tables(engine, known_fqns={"edfi.Student"})
    by_fqn = {e.fqn: e for e in extras}
    rep = by_fqn["edfi.DistrictReport"]
    col_names = {c.name for c in rep.columns}
    assert col_names == {"ReportId", "Title", "CreatedAt"}
    pk_col = next(c for c in rep.columns if c.name == "ReportId")
    assert pk_col.is_identifying is True


def test_unknown_table_pk_populated(engine):
    extras, _ = reflect_unknown_tables(engine, known_fqns=set())
    by_fqn = {e.fqn: e for e in extras}
    assert by_fqn["edfi.DistrictReport"].primary_key == ["ReportId"]
    assert by_fqn["edfi.DistrictReportDetail"].primary_key == ["DetailId"]


def test_unknown_table_tagged_as_other_domain(engine):
    """domains=['Other'] keeps reflection-only tables clearly marked so
    operators can re-classify them via overrides without being misled
    by a guessed Ed-Fi domain."""
    extras, _ = reflect_unknown_tables(engine, known_fqns=set())
    for e in extras:
        assert e.domains == ["Other"], (e.fqn, e.domains)
        assert e.is_extension is True


def test_unknown_table_sample_rows_captured(engine):
    extras, _ = reflect_unknown_tables(engine, known_fqns=set())
    by_fqn = {e.fqn: e for e in extras}
    rep = by_fqn["edfi.DistrictReport"]
    assert rep.row_count == 1
    assert len(rep.sample_rows) == 1
    assert rep.sample_rows[0]["Title"] == "Q1 Snapshot"


def test_fk_records_pair_child_to_parent(engine):
    _, fk_records = reflect_unknown_tables(engine, known_fqns=set())
    assert any(
        fk["child_table"] == "DistrictReportDetail"
        and fk["parent_table"] == "DistrictReport"
        and fk["child_columns"] == ["ReportId"]
        and fk["parent_columns"] == ["ReportId"]
        for fk in fk_records
    ), fk_records


def test_child_neighbors_populated_symmetrically(engine):
    """When both parent and child are reflected, the parent's
    child_neighbors must list the child fqn — the graph builder uses
    these to seed undirected edges."""
    extras, _ = reflect_unknown_tables(engine, known_fqns=set())
    by_fqn = {e.fqn: e for e in extras}
    parent = by_fqn["edfi.DistrictReport"]
    assert "edfi.DistrictReportDetail" in parent.child_neighbors


def test_returns_empty_on_empty_db(tmp_path: Path):
    """An engine with zero user tables shouldn't crash — returns
    empty lists. Lets the catalog builder handle "fresh DB" cleanly."""
    p = tmp_path / "empty.sqlite"
    sa.create_engine(f"sqlite:///{p}", future=True).dispose()
    spec = ProviderEntry.model_validate({"kind": "sqlite", "path": str(p), "read_only": False})
    engine = SqliteEngine(spec)
    extras, fks = reflect_unknown_tables(engine, known_fqns={"edfi.Anything"})
    assert extras == []
    assert fks == []
