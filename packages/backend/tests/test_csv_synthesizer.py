"""Q3 — synthesize TableMetadata + TableClassification from CatalogInputs,
then run the existing `build_table_catalog()` against operator CSVs only.

Two layers of tests:

  1. Synthesizer in isolation — verifies neighbor wiring, descriptor /
     association heuristics, single-domain-per-table mapping.
  2. End-to-end CSV → live DB → catalog — proves the operator-CSV path
     reaches a real `TableCatalog` artifact without ever touching Ed-Fi
     GitHub. Uses an in-memory SQLite DB seeded with three small tables.

The end-to-end test is the main artifact for Q3: it pins the new flow
in place against the existing builder so the next step (Q3 finish:
CLI + admin endpoint) can lean on a green test.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.catalog_inputs import (
    CatalogInputs,
    synthesize_inputs_for_builder,
    synthesize_metadata,
)
from text2sql.config import ProviderEntry
from text2sql.providers.db.sqlite import SqliteEngine
from text2sql.table_catalog.catalog_builder import build_table_catalog


def _schema_csv(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"


def _rel_csv(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"


# ── Synthesizer in isolation ────────────────────────────────────────────────


def _three_table_inputs() -> CatalogInputs:
    schema = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Student,edfi,Student,StudentUSI,Yes
        0,Student,edfi,Student,FirstName,Yes
        0,Enrollment,edfi,StudentSchoolAssoc,StudentUSI,Yes
        0,Enrollment,edfi,StudentSchoolAssoc,SchoolId,Yes
        1,EducationOrg,edfi,School,SchoolId,Yes
    """)
    rels = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_SSA_Student,StudentSchoolAssoc,StudentUSI,Student,StudentUSI,edfi,edfi
        FK_SSA_School,StudentSchoolAssoc,SchoolId,School,SchoolId,edfi,edfi
    """)
    return CatalogInputs.from_csvs(schema, rels)


def test_synthesize_metadata_emits_one_record_per_csv_table():
    inputs = _three_table_inputs()
    metas, classifications = synthesize_metadata(inputs)
    assert [m.fqn for m in metas] == [
        "edfi.Student", "edfi.StudentSchoolAssoc", "edfi.School",
    ]
    assert len(classifications) == 3
    assert all(c.source == "operator_csv" for c in classifications)


def test_synthesize_metadata_assigns_single_domain_per_table():
    """Stage-1/2/3 ladder collapses on the CSV path: the operator
    declares the answer, so each TableClassification carries exactly
    one domain."""
    metas, classifications = synthesize_metadata(_three_table_inputs())
    by_fqn = {c.fqn: c for c in classifications}
    assert by_fqn["edfi.Student"].domains == ["Student"]
    assert by_fqn["edfi.StudentSchoolAssoc"].domains == ["Enrollment"]
    assert by_fqn["edfi.School"].domains == ["EducationOrg"]


def test_synthesize_metadata_wires_neighbors_from_fk_edges():
    metas, _ = synthesize_metadata(_three_table_inputs())
    by_fqn = {m.fqn: m for m in metas}
    # SSA points at Student and School (parent_neighbors)
    assert set(by_fqn["edfi.StudentSchoolAssoc"].parent_neighbors) == {
        "edfi.Student", "edfi.School",
    }
    # Student is referenced by SSA (child_neighbor)
    assert "edfi.StudentSchoolAssoc" in by_fqn["edfi.Student"].child_neighbors
    assert "edfi.StudentSchoolAssoc" in by_fqn["edfi.School"].child_neighbors


def test_synthesize_metadata_flags_descriptor_and_association_heuristics():
    schema = _schema_csv("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Descriptor,edfi,GradeLevelDescriptor,GradeLevelDescriptorId,Yes
        0,Enrollment,edfi,StudentSchoolAssoc,StudentUSI,Yes
        0,Student,edfi,Student,StudentUSI,Yes
        0,EducationOrg,edfi,School,SchoolId,Yes
    """)
    rels = _rel_csv("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_SSA_Student,StudentSchoolAssoc,StudentUSI,Student,StudentUSI,edfi,edfi
        FK_SSA_School,StudentSchoolAssoc,SchoolId,School,SchoolId,edfi,edfi
    """)
    metas, _ = synthesize_metadata(CatalogInputs.from_csvs(schema, rels))
    by_fqn = {m.fqn: m for m in metas}
    # Descriptor heuristic: name-suffix match
    assert by_fqn["edfi.GradeLevelDescriptor"].is_descriptor is True
    assert by_fqn["edfi.Student"].is_descriptor is False
    # Association heuristic: ≥2 outgoing FK edges
    assert by_fqn["edfi.StudentSchoolAssoc"].is_association is True
    assert by_fqn["edfi.Student"].is_association is False


def test_synthesize_inputs_for_builder_returns_indexed_catalog():
    idx, classifications, manifest = synthesize_inputs_for_builder(_three_table_inputs())
    assert idx.by_fqn.keys() == {
        "edfi.Student", "edfi.StudentSchoolAssoc", "edfi.School",
    }
    assert len(classifications) == 3
    # Stub manifest carries an empty artifact list — that's the contract
    # that lets build_table_catalog skip the ApiModel column path.
    assert manifest.artifacts == ()


# ── End-to-end: CSV → live DB → built catalog ───────────────────────────────


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    """A SQLite DB matching the three-table CSV. PKs + FKs declared so
    the catalog builder's reflection path can pick them up — this is
    the path that replaces the old ApiModel column source."""
    db_path = tmp_path / "operator.sqlite"
    raw = sa.create_engine(f"sqlite:///{db_path}", future=True)
    with raw.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE Student ("
            "  StudentUSI INTEGER PRIMARY KEY,"
            "  FirstName TEXT NOT NULL"
            ")"
        ))
        conn.execute(sa.text(
            "CREATE TABLE School ("
            "  SchoolId INTEGER PRIMARY KEY,"
            "  Name TEXT"
            ")"
        ))
        conn.execute(sa.text(
            "CREATE TABLE StudentSchoolAssoc ("
            "  StudentUSI INTEGER NOT NULL,"
            "  SchoolId INTEGER NOT NULL,"
            "  EntryDate TEXT,"
            "  PRIMARY KEY (StudentUSI, SchoolId),"
            "  FOREIGN KEY (StudentUSI) REFERENCES Student(StudentUSI),"
            "  FOREIGN KEY (SchoolId) REFERENCES School(SchoolId)"
            ")"
        ))
        conn.execute(sa.text("INSERT INTO Student VALUES (101, 'Ana')"))
        conn.execute(sa.text("INSERT INTO Student VALUES (102, 'Bilal')"))
        conn.execute(sa.text("INSERT INTO School VALUES (1, 'Northridge HS')"))
        conn.execute(sa.text(
            "INSERT INTO StudentSchoolAssoc VALUES (101, 1, '2023-08-21')"
        ))
    raw.dispose()

    spec = ProviderEntry.model_validate({
        "kind": "sqlite", "path": str(db_path), "read_only": False,
    })
    return SqliteEngine(spec)


def test_end_to_end_csv_only_catalog_build(sqlite_engine):
    """Build a catalog from CSVs + live DB only — no Ed-Fi GitHub."""
    inputs = _three_table_inputs()
    idx, classifications, manifest = synthesize_inputs_for_builder(
        inputs, sql_engine=sqlite_engine,
    )

    catalog = build_table_catalog(
        classifications=classifications,
        catalog_index=idx,
        manifest=manifest,
        sql_engine=sqlite_engine,
        description_generator=None,            # description LLM stays optional
        enrich_values=True,
        sample_row_count=2,
        # Don't reflect into the live DB for "extra" tables — the CSV is
        # already authoritative for what we want catalogued.
        include_unknown_tables=False,
    )
    by_fqn = {e.fqn: e for e in catalog.entries}
    assert {"edfi.Student", "edfi.StudentSchoolAssoc", "edfi.School"} <= set(by_fqn)

    # Columns came from the live DB (the CSV path's intended behavior).
    student = by_fqn["edfi.Student"]
    student_cols = {c.name for c in student.columns}
    assert {"StudentUSI", "FirstName"} <= student_cols

    # PK reflected from the live DB
    ssa = by_fqn["edfi.StudentSchoolAssoc"]
    assert ssa.primary_key == ["StudentUSI", "SchoolId"]

    # Domain comes straight from the operator CSV
    assert student.domains == ["Student"]
    assert ssa.domains == ["Enrollment"]
    assert by_fqn["edfi.School"].domains == ["EducationOrg"]

    # Sample rows + counts came from the live DB
    assert student.row_count == 2
    assert len(student.sample_rows) == 2
