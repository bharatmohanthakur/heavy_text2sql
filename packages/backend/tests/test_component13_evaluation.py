"""Component 13 — evaluation harness tests.

Two layers:
  * Unit (offline): grade_case / _aggregate produce the right judgments on
    constructed inputs.
  * Integration: run_evaluation against the live pipeline + bootstrap gold
    store. Asserts the run produces a coherent report and meets a soft
    execution-accuracy floor.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.evaluation.harness import (
    CaseResult,
    Metrics,
    _aggregate,
    grade_case,
    run_evaluation,
)
from text2sql.gold.schema import GoldRecord
from text2sql.pipeline import PipelineResult


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


# ── Stub engine for unit tests ────────────────────────────────────────────────


class _StubEngine:
    dialect = "postgresql"

    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = rows or []

    def execute(self, sql: str, *, limit: int | None = None, **_) -> list[dict]:
        return list(self._rows)[: limit or 1000]

    def explain(self, sql: str) -> str:
        return ""

    def list_tables(self) -> list[tuple[str, str]]:
        return []

    def list_columns(self, *_) -> list[tuple[str, str, bool]]:
        return []

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'


def _gold(nl: str, sql: str, tables: list[str]) -> GoldRecord:
    return GoldRecord(
        id=uuid.uuid4(),
        nl_question=nl,
        sql_text=sql,
        tables_used=tables,
        domains_used=[],
        approval_status="approved",
        exec_check_passed=True,
    )


# ── Unit ─────────────────────────────────────────────────────────────────────


def test_grade_case_schema_and_join_hits() -> None:
    from text2sql.classification.query_classifier import QueryClassification
    from text2sql.embedding.retriever import TableHit
    from text2sql.graph.steiner import SteinerTree

    rec = _gold(
        "How many students per school?",
        "SELECT COUNT(*) FROM edfi.studentschoolassociation",
        ["edfi.StudentSchoolAssociation", "edfi.School"],
    )
    rows = [{"n": 1959}]
    eng = _StubEngine(rows)
    result = PipelineResult(
        nl_question=rec.nl_question,
        sql="SELECT COUNT(*) AS n FROM edfi.studentschoolassociation",
        rationale="",
        rows=rows, row_count=1,
        executed=True, validated=True,
        domains=QueryClassification(query=rec.nl_question, domains=["Enrollment"], reasoning="", source="llm"),
        retrieved_tables=[
            TableHit(fqn="edfi.StudentSchoolAssociation", score=0.9, domains=[], is_descriptor=False, text=""),
            TableHit(fqn="edfi.School", score=0.7, domains=[], is_descriptor=False, text=""),
        ],
        join_tree=SteinerTree(
            targets=("edfi.StudentSchoolAssociation", "edfi.School"),
            nodes=["edfi.StudentSchoolAssociation", "edfi.School"],
            edges=[], total_weight=1.0,
        ),
        timings_ms={"total_ms": 1234.0},
    )
    case = grade_case(rec, result, sql_engine=eng)
    assert case.schema_linking_hit
    assert case.join_path_hit
    assert case.sql_valid
    assert case.execution_match is True
    assert case.leaked_descriptors == []


def test_grade_case_detects_descriptor_leakage() -> None:
    from text2sql.classification.query_classifier import QueryClassification
    from text2sql.embedding.retriever import TableHit
    from text2sql.graph.steiner import SteinerTree

    rec = _gold(
        "all students",
        "SELECT * FROM edfi.student",
        ["edfi.Student"],
    )
    eng = _StubEngine([])
    # Generated SQL pivots through descriptors not used by gold.
    result = PipelineResult(
        nl_question=rec.nl_question,
        sql=(
            'SELECT s.studentusi FROM "edfi"."student" s '
            'JOIN "edfi"."OldEthnicityDescriptor" oed ON s.oldethnicitydescriptorid = oed.oldethnicitydescriptorid'
        ),
        rationale="",
        rows=[], row_count=0,
        executed=True, validated=True,
        retrieved_tables=[],
        domains=QueryClassification(query="...", domains=[], reasoning="", source="llm"),
        join_tree=SteinerTree(targets=(), nodes=[], edges=[], total_weight=0),
        timings_ms={"total_ms": 100.0},
    )
    case = grade_case(rec, result, sql_engine=eng)
    assert "oldethnicitydescriptor" in case.leaked_descriptors


def test_aggregate_metrics_basic() -> None:
    cases = [
        CaseResult(
            nl_question="a", gold_id="1", gold_tables=[],
            generated_sql="", gold_sql="",
            schema_linking_hit=True, join_path_hit=True, sql_valid=True,
            execution_match=True, leaked_descriptors=[],
            timings_ms={"total_ms": 1000.0, "context+llm": 800.0},
        ),
        CaseResult(
            nl_question="b", gold_id="2", gold_tables=[],
            generated_sql="", gold_sql="",
            schema_linking_hit=True, join_path_hit=False, sql_valid=False,
            execution_match=False, leaked_descriptors=["x"],
            timings_ms={"total_ms": 3000.0, "context+llm": 2500.0},
        ),
    ]
    m = _aggregate(cases)
    assert m.n_cases == 2
    assert m.schema_linking_recall == 1.0
    assert m.join_path_exactness == 0.5
    assert m.sql_syntactic_validity == 0.5
    assert m.execution_accuracy == 0.5
    assert m.descriptor_leakage_rate == 0.5
    assert m.latency_total_ms_p50 in (1000.0, 3000.0)


def test_eval_report_writes_json_and_markdown(tmp_path: Path) -> None:
    from text2sql.evaluation.harness import EvalReport

    rep = EvalReport(
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        metrics=Metrics(n_cases=1, schema_linking_recall=1.0, latency_total_ms_p50=42.0),
        cases=[CaseResult(
            nl_question="q", gold_id="1", gold_tables=[],
            generated_sql="", gold_sql="",
            schema_linking_hit=True, join_path_hit=True, sql_valid=True,
            execution_match=True,
        )],
    )
    j = tmp_path / "r.json"
    md = tmp_path / "r.md"
    rep.write_json(j)
    rep.write_markdown(md)
    assert j.exists() and md.exists()
    blob = j.read_text()
    assert '"n_cases": 1' in blob
    md_text = md.read_text()
    assert "schema linking recall" in md_text


# ── Integration: live pipeline + bootstrap gold store ────────────────────────


@pytest.mark.skipif(not (_has_azure() and _has_db()), reason="azure or DB unavailable")
def test_live_eval_produces_coherent_report() -> None:
    from text2sql.cli import _build_pipeline
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
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
    store = GoldStore(url, embedder, catalog=catalog)
    if store.count(approval_status="approved") < 3:
        pytest.skip("gold store not seeded — run `text2sql gold-seed` first")

    pipeline = _build_pipeline()
    # Cap at 5 cases so the test doesn't run for minutes.
    report = run_evaluation(pipeline, store, max_cases=5)

    m = report.metrics
    print(
        f"\nschema_linking={m.schema_linking_recall:.0%}  "
        f"join_path={m.join_path_exactness:.0%}  "
        f"valid={m.sql_syntactic_validity:.0%}  "
        f"exec={m.execution_accuracy:.0%}  "
        f"leakage={m.descriptor_leakage_rate:.0%}  "
        f"p50={m.latency_total_ms_p50:.0f}ms"
    )
    assert m.n_cases >= 3
    # Sanity floors — purposely lax. Real floors enforced via CI gate.
    assert m.sql_syntactic_validity >= 0.6
    assert m.latency_total_ms_p50 > 0
