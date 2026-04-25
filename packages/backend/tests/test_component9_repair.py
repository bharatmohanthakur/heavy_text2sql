"""Component 9 — validation + repair loop.

Three classes of test:

  * Unit (offline): validate_sql returns the right error for parse / non-SELECT
    failures and None for clean SQL. Uses a stub SqlEngine.
  * Integration (real Postgres + Azure GPT-4o): the loop actually repairs a
    deliberately broken query.
  * Negative: gives up after max_attempts on something the LLM can't fix.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.pipeline.context import PromptContext
from text2sql.pipeline.repair import RepairLoop, validate_sql


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


def _has_db() -> bool:
    cfg = load_config()
    try:
        from text2sql.providers import build_sql_engine
        eng = build_sql_engine(cfg.target_db_provider())
        eng.execute("SELECT 1 AS ok")
        return True
    except Exception:
        return False


# ── Stub engine for offline unit tests ─────────────────────────────────────────


class _StubEngine:
    dialect = "postgresql"

    def __init__(self, explain_raises: Exception | None = None) -> None:
        self._explain_raises = explain_raises

    def execute(self, *a: Any, **k: Any) -> list[dict]:
        return []

    def explain(self, sql: str) -> str:
        if self._explain_raises is not None:
            raise self._explain_raises
        return "Seq Scan"

    def list_tables(self) -> list[tuple[str, str]]:
        return []

    def list_columns(self, *a: Any) -> list[tuple[str, str, bool]]:
        return []

    def quote_identifier(self, name: str) -> str:
        return f'"{name.lower()}"'


# ── Unit ─────────────────────────────────────────────────────────────────────


def test_validate_sql_accepts_clean_select() -> None:
    eng = _StubEngine()
    assert validate_sql("SELECT 1 AS n", eng) is None


def test_validate_sql_rejects_empty() -> None:
    assert validate_sql("", _StubEngine()) is not None
    assert validate_sql("   ", _StubEngine()) is not None


def test_validate_sql_rejects_non_select() -> None:
    eng = _StubEngine()
    assert "non-SELECT" in (validate_sql("DELETE FROM foo", eng) or "")
    assert "non-SELECT" in (validate_sql("UPDATE foo SET x=1", eng) or "")
    assert "non-SELECT" in (validate_sql("INSERT INTO foo VALUES (1)", eng) or "")


def test_validate_sql_propagates_engine_error() -> None:
    eng = _StubEngine(explain_raises=RuntimeError("relation \"x\" does not exist"))
    err = validate_sql("SELECT * FROM x", eng)
    assert err and "relation" in err


def test_repair_loop_short_circuits_on_clean_first_sql() -> None:
    eng = _StubEngine()
    llm = MagicMock()       # should never be invoked
    loop = RepairLoop(llm, eng, max_attempts=3)
    prompt = PromptContext(
        nl_question="...", dialect="postgresql", domain_routing=[],
        selected_tables=[], m_schema_block="", join_clauses=[],
        resolved_bindings=[], few_shots=[], rules=[],
    )
    result = loop.run(prompt, "SELECT 1 AS n")
    assert result.accepted
    assert result.final_sql == "SELECT 1 AS n"
    assert len(result.attempts) == 1
    llm.complete.assert_not_called()


def test_repair_loop_calls_llm_when_first_fails() -> None:
    """First attempt fails parse; second attempt is clean. Repair fires once."""
    eng = _StubEngine()
    llm = MagicMock()
    llm.complete.return_value = '{"sql": "SELECT 1 AS n", "rationale": "fixed"}'
    loop = RepairLoop(llm, eng, max_attempts=3)
    prompt = PromptContext(
        nl_question="...", dialect="postgresql", domain_routing=[],
        selected_tables=[], m_schema_block="", join_clauses=[],
        resolved_bindings=[], few_shots=[], rules=[],
    )
    # Garbage SQL → fails sqlglot parse.
    result = loop.run(prompt, "SELEC oops broken")
    assert result.accepted
    assert result.final_sql == "SELECT 1 AS n"
    assert len(result.attempts) == 2
    llm.complete.assert_called_once()


def test_repair_loop_gives_up_after_max_attempts() -> None:
    eng = _StubEngine()
    llm = MagicMock()
    # LLM keeps returning unparseable SQL; loop should bail after 2 attempts.
    llm.complete.return_value = '{"sql": "@@@ not valid sql @@@", "rationale": "?"}'
    loop = RepairLoop(llm, eng, max_attempts=2)
    prompt = PromptContext(
        nl_question="...", dialect="postgresql", domain_routing=[],
        selected_tables=[], m_schema_block="", join_clauses=[],
        resolved_bindings=[], few_shots=[], rules=[],
    )
    result = loop.run(prompt, "SELEC oops broken")
    assert not result.accepted
    assert len(result.attempts) == 2
    assert result.attempts[-1].error is not None


# ── Integration: real LLM + real Postgres ─────────────────────────────────────


@pytest.mark.skipif(not (_has_azure() and _has_db()), reason="azure or DB unavailable")
def test_repair_fixes_real_explain_error() -> None:
    """Hand the loop an SQL that references a column that doesn't exist
    (live Postgres EXPLAIN will fail). Verify the LLM produces a fix that
    EXPLAINs cleanly."""
    from text2sql.providers import build_embedding, build_llm, build_sql_engine
    from text2sql.pipeline.context import ContextBuilder
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    sql_engine = build_sql_engine(cfg.target_db_provider())
    llm = build_llm(cfg.llm_for_task("sql_generation"))
    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
    builder = ContextBuilder(catalog=catalog, dialect="postgresql")

    # Build a real prompt that includes school + educationorganization so the
    # LLM has the right tables to repair toward.
    prompt = builder.build(
        nl_question="Count students per school",
        domain_routing=type("_", (), {"domains": ["Enrollment", "EducationOrganization"]})(),
        retrieved_tables=[],
        steiner=type("_", (), {"nodes": [
            "edfi.StudentSchoolAssociation", "edfi.School", "edfi.EducationOrganization",
        ], "edges": [], "to_join_clauses": lambda self_, dialect="postgresql": [
            'JOIN "edfi"."studentschoolassociation" ON "edfi"."studentschoolassociation"."schoolid" = "edfi"."school"."schoolid"',
            'JOIN "edfi"."school" ON "edfi"."school"."schoolid" = "edfi"."educationorganization"."educationorganizationid"',
        ]})(),
        resolution=type("_", (), {"phrases": []})(),
        few_shots=[],
    )

    # SQL that references school.nameofinstitution — a column that DOES NOT
    # exist on edfi.school in DS 5.x. Postgres EXPLAIN will fail.
    broken = (
        'SELECT "edfi"."school"."nameofinstitution" AS school, '
        'COUNT(*) AS n FROM "edfi"."studentschoolassociation" '
        'JOIN "edfi"."school" ON "edfi"."studentschoolassociation"."schoolid" = "edfi"."school"."schoolid" '
        'GROUP BY "edfi"."school"."nameofinstitution"'
    )
    loop = RepairLoop(llm, sql_engine, max_attempts=3, dialect="postgresql")
    result = loop.run(prompt, broken)

    print(f"\nattempts: {len(result.attempts)}, accepted: {result.accepted}")
    for i, a in enumerate(result.attempts):
        print(f"  [{i}] err={a.error}")
        print(f"      sql={a.sql[:120]}")
    print(f"\nfinal SQL:\n{result.final_sql}")
    assert result.accepted, f"loop did not converge after {len(result.attempts)} tries"
    assert "educationorganization" in result.final_sql.lower()
