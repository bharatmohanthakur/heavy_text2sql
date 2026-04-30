"""Q4 — target-DB-driven table descriptions on the CSV path.

Synthesizer leaves `description=""` on every table (operator CSV doesn't
carry prose). The catalog builder must therefore enqueue every table
for LLM gap-fill, and the LLM prompt must include the live-DB sample
rows so the model has something concrete to summarize.

Uses a fake LLM provider that captures every prompt and returns
deterministic JSON. We assert that:
  1. Every CSV-built TableEntry ends up with a non-empty description
     stamped `description_source="llm"` (or "cache" on the second
     identical build).
  2. The prompt the LLM saw includes the sample-row markers — proves
     the data path from `_sample_rows()` flows through to the model.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa

from text2sql.catalog_inputs import (
    CatalogInputs,
    synthesize_inputs_for_builder,
)
from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage
from text2sql.providers.db.sqlite import SqliteEngine
from text2sql.table_catalog import DescriptionGenerator, build_table_catalog


class _FakeLLM:
    """Minimal LLMProvider stand-in. Echos table fqn into the
    description so we can prove table identity flowed through, and
    captures every prompt for assertion."""

    model_id = "fake-llm-q4"
    capabilities = LLMCapabilities(
        strict_json_schema=True, token_streaming=False,
        openai_tool_calling=False, anthropic_tool_use=False,
    )

    def __init__(self) -> None:
        self.calls: list[str] = []

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        prompt = "\n".join(m.content for m in messages)
        self.calls.append(prompt)
        # Pull the fqn out of the prompt's first line: "TABLE: edfi.X"
        first = prompt.splitlines()[0] if prompt else ""
        fqn = first.removeprefix("TABLE: ").strip() if first.startswith("TABLE: ") else "?"
        # Schema asks for table_description and/or column_descriptions —
        # answer with both, deterministic, so the catalog ends up filled.
        col_descs: dict[str, str] = {}
        props = (schema or {}).get("properties", {}).get("column_descriptions", {})
        for col_name in props.get("properties", {}).keys():
            col_descs[col_name] = f"{col_name} of {fqn}"
        body: dict[str, Any] = {}
        if "table_description" in (schema or {}).get("properties", {}):
            body["table_description"] = f"Records pertaining to {fqn} (from sample rows)."
        if col_descs:
            body["column_descriptions"] = col_descs
        return json.dumps(body)

    def stream(self, messages):  # pragma: no cover - unused on this path
        raise NotImplementedError


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    db = tmp_path / "q4.sqlite"
    raw = sa.create_engine(f"sqlite:///{db}", future=True)
    with raw.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE Student (StudentUSI INTEGER PRIMARY KEY, FirstName TEXT)"
        ))
        conn.execute(sa.text("CREATE TABLE School (SchoolId INTEGER PRIMARY KEY, Name TEXT)"))
        conn.execute(sa.text(
            "CREATE TABLE StudentSchoolAssoc ("
            "  StudentUSI INTEGER NOT NULL, SchoolId INTEGER NOT NULL,"
            "  PRIMARY KEY (StudentUSI, SchoolId),"
            "  FOREIGN KEY (StudentUSI) REFERENCES Student(StudentUSI),"
            "  FOREIGN KEY (SchoolId) REFERENCES School(SchoolId)"
            ")"
        ))
        conn.execute(sa.text("INSERT INTO Student VALUES (1, 'Ana'), (2, 'Bilal')"))
        conn.execute(sa.text("INSERT INTO School VALUES (1, 'Northridge HS')"))
        conn.execute(sa.text("INSERT INTO StudentSchoolAssoc VALUES (1, 1)"))
    raw.dispose()
    return SqliteEngine(ProviderEntry.model_validate(
        {"kind": "sqlite", "path": str(db), "read_only": False}
    ))


def _three_table_inputs() -> CatalogInputs:
    schema = textwrap.dedent("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,Student,edfi,Student,StudentUSI,Yes
        0,Student,edfi,Student,FirstName,Yes
        0,EducationOrg,edfi,School,SchoolId,Yes
        0,Enrollment,edfi,StudentSchoolAssoc,StudentUSI,Yes
        0,Enrollment,edfi,StudentSchoolAssoc,SchoolId,Yes
    """).strip() + "\n"
    rels = textwrap.dedent("""
        FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema
        FK_SSA_Student,StudentSchoolAssoc,StudentUSI,Student,StudentUSI,edfi,edfi
        FK_SSA_School,StudentSchoolAssoc,SchoolId,School,SchoolId,edfi,edfi
    """).strip() + "\n"
    return CatalogInputs.from_csvs(schema, rels)


def test_csv_path_fills_table_descriptions_from_live_db_samples(
    sqlite_engine, tmp_path: Path,
):
    """End-to-end CSV → live SQLite → catalog with a fake LLM. Every
    table entry must end up with a non-empty description that came
    from the LLM (description_source="llm"), and the prompt the LLM
    saw must reference SAMPLE ROWS — proving the target-DB rows
    actually flow into the description prompt."""
    fake = _FakeLLM()
    desc_gen = DescriptionGenerator(fake, cache_path=tmp_path / ".desc_cache.json")

    idx, classifications, manifest = synthesize_inputs_for_builder(
        _three_table_inputs(), sql_engine=sqlite_engine,
    )
    catalog = build_table_catalog(
        classifications=classifications,
        catalog_index=idx,
        manifest=manifest,
        sql_engine=sqlite_engine,
        description_generator=desc_gen,
        sample_row_count=2,
        include_unknown_tables=False,
    )

    by_fqn = {e.fqn: e for e in catalog.entries}
    for fqn in ("edfi.Student", "edfi.School", "edfi.StudentSchoolAssoc"):
        e = by_fqn[fqn]
        assert e.description, f"{fqn} description should be filled by LLM"
        assert e.description_source in ("llm", "cache"), (
            f"{fqn} description_source={e.description_source!r}"
        )

    # The prompt-capture is what proves the *target DB* enrichment ran.
    # At least one prompt must contain the live-DB SAMPLE ROWS marker
    # for the populated tables (Student/School both have data).
    student_prompts = [p for p in fake.calls if "TABLE: edfi.Student" in p]
    assert student_prompts, "DescriptionGenerator never saw edfi.Student"
    p = student_prompts[0]
    assert "SAMPLE ROWS" in p
    # Concrete sample row content — proves the DB read flowed through.
    assert "Ana" in p or "Bilal" in p, p
