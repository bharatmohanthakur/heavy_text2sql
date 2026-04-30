"""Eval harness — runs the pipeline over the gold corpus and grades six metrics.

Each gold record contributes one CaseResult. CaseResults aggregate into a
top-level Metrics record; Metrics + per-case detail go into EvalReport.

Six metrics (spec section 14.1):
  1. schema_linking_recall    — gold tables subset of retrieved tables
  2. join_path_exactness      — gold tables subset of Steiner-tree nodes
  3. sql_syntactic_validity   — generated SQL passed parse + EXPLAIN
  4. execution_accuracy       — generated rows match gold rows (set-equal)
  5. latency                  — per-stage p50/p95/p99
  6. descriptor_leakage_rate  — generated SQL JOINs unused descriptor types

The harness is sync; we run cases serially so we get deterministic timing
breakdowns. Concurrency is a Phase 6 hardening task.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from text2sql.gold.schema import GoldRecord
from text2sql.gold.store import GoldStore
from text2sql.pipeline import PipelineResult, Text2SqlPipeline
from text2sql.providers.base import SqlEngine

log = logging.getLogger(__name__)


# ── Per-case outcome ──────────────────────────────────────────────────────────


@dataclass
class CaseResult:
    nl_question: str
    gold_id: str
    gold_tables: list[str]
    generated_sql: str
    gold_sql: str

    # Pipeline outputs
    retrieved_tables: list[str] = field(default_factory=list)
    join_tree_nodes: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    timings_ms: dict[str, float] = field(default_factory=dict)

    # Per-metric judgments
    schema_linking_hit: bool = False
    join_path_hit: bool = False
    sql_valid: bool = False
    execution_match: bool | None = None      # None when comparison was skipped
    leaked_descriptors: list[str] = field(default_factory=list)

    error: str | None = None
    description: str = ""


# ── Aggregate metrics ─────────────────────────────────────────────────────────


@dataclass
class Metrics:
    n_cases: int = 0
    schema_linking_recall: float = 0.0
    join_path_exactness: float = 0.0
    sql_syntactic_validity: float = 0.0
    execution_accuracy: float = 0.0
    descriptor_leakage_rate: float = 0.0
    latency_total_ms_p50: float = 0.0
    latency_total_ms_p95: float = 0.0
    latency_total_ms_p99: float = 0.0
    per_stage_p50: dict[str, float] = field(default_factory=dict)


@dataclass
class EvalReport:
    generated_at: str
    metrics: Metrics
    cases: list[CaseResult] = field(default_factory=list)

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "generated_at": self.generated_at,
            "metrics": asdict(self.metrics),
            "cases": [asdict(c) for c in self.cases],
        }, indent=2, default=str))

    def write_markdown(self, path: Path) -> None:
        m = self.metrics
        lines = [
            f"# Eval report — {self.generated_at}",
            "",
            f"- cases: **{m.n_cases}**",
            f"- schema linking recall: **{m.schema_linking_recall:.0%}**",
            f"- join path exactness:   **{m.join_path_exactness:.0%}**",
            f"- SQL validity:          **{m.sql_syntactic_validity:.0%}**",
            f"- execution accuracy:    **{m.execution_accuracy:.0%}**",
            f"- descriptor leakage:    **{m.descriptor_leakage_rate:.0%}**",
            f"- latency p50/p95/p99:   "
            f"**{m.latency_total_ms_p50:.0f} / {m.latency_total_ms_p95:.0f} / {m.latency_total_ms_p99:.0f} ms**",
            "",
            "## Stage p50 (ms)",
            "| stage | p50 |",
            "|---|---|",
        ]
        for k, v in sorted(m.per_stage_p50.items(), key=lambda kv: -kv[1]):
            lines.append(f"| `{k}` | {v:.0f} |")
        lines.append("")
        lines.append("## Failures")
        fails = [c for c in self.cases if c.error or not c.execution_match]
        if not fails:
            lines.append("_(none)_")
        else:
            for c in fails:
                why = c.error or (
                    "exec mismatch" if c.execution_match is False else "validation failed"
                )
                lines.append(f"- {c.nl_question!r:80s} — {why}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines), encoding="utf-8")

    def regression(self, prev: "EvalReport | None") -> dict[str, float]:
        """Delta vs a previous report; positive = improvement."""
        if not prev:
            return {}
        a, b = self.metrics, prev.metrics
        return {
            "schema_linking_recall": a.schema_linking_recall - b.schema_linking_recall,
            "join_path_exactness": a.join_path_exactness - b.join_path_exactness,
            "sql_syntactic_validity": a.sql_syntactic_validity - b.sql_syntactic_validity,
            "execution_accuracy": a.execution_accuracy - b.execution_accuracy,
            "descriptor_leakage_rate": -(a.descriptor_leakage_rate - b.descriptor_leakage_rate),
            "latency_total_ms_p50": -(a.latency_total_ms_p50 - b.latency_total_ms_p50),
        }


# ── Per-case grading ──────────────────────────────────────────────────────────


def _normalize_tables(tables: Iterable[str]) -> set[str]:
    return {t.strip().lower() for t in tables if t}


_DESCRIPTOR_PATTERN = re.compile(r'"?edfi"?\."?([a-z_]+descriptor)"?', re.IGNORECASE)


def _descriptors_referenced(sql: str) -> set[str]:
    return {m.group(1).lower() for m in _DESCRIPTOR_PATTERN.finditer(sql)}


def _rows_set_equal(a: list[dict], b: list[dict]) -> bool:
    """Compare result rows as multisets, ignoring row order."""
    def hash_row(r: dict) -> tuple:
        return tuple(sorted((k, str(v)) for k, v in r.items()))
    return sorted(hash_row(r) for r in a) == sorted(hash_row(r) for r in b)


def grade_case(
    record: GoldRecord,
    result: PipelineResult,
    *,
    sql_engine: SqlEngine,
    compare_execution: bool = True,
) -> CaseResult:
    case = CaseResult(
        nl_question=record.nl_question,
        gold_id=str(record.id),
        gold_tables=list(record.tables_used),
        gold_sql=record.sql_text,
        generated_sql=result.sql,
        domains=result.domains.domains if result.domains else [],
        retrieved_tables=[h.fqn for h in result.retrieved_tables],
        join_tree_nodes=list(result.join_tree.nodes) if result.join_tree else [],
        timings_ms=dict(result.timings_ms),
        description=result.description,
        error=result.error,
    )

    gold = _normalize_tables(record.tables_used)
    if gold:
        case.schema_linking_hit = gold.issubset(_normalize_tables(case.retrieved_tables))
        case.join_path_hit = gold.issubset(_normalize_tables(case.join_tree_nodes))

    case.sql_valid = bool(result.validated and result.sql)

    # Descriptor leakage: any descriptor table referenced in generated SQL
    # that doesn't appear in the gold SQL is a leak.
    gen_desc = _descriptors_referenced(case.generated_sql)
    gold_desc = _descriptors_referenced(case.gold_sql)
    case.leaked_descriptors = sorted(gen_desc - gold_desc)

    # Execution: run gold SQL too and compare.
    if compare_execution and case.sql_valid and result.executed:
        try:
            gold_rows = sql_engine.execute(record.sql_text, limit=1000)
            case.execution_match = _rows_set_equal(result.rows, gold_rows)
        except Exception as e:
            case.execution_match = None
            case.error = (case.error or "") + f"; gold exec failed: {e}"
    return case


# ── Aggregation ───────────────────────────────────────────────────────────────


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[k]


def _aggregate(cases: list[CaseResult]) -> Metrics:
    n = len(cases)
    if n == 0:
        return Metrics()
    schema_hits = sum(1 for c in cases if c.schema_linking_hit) / n
    join_hits = sum(1 for c in cases if c.join_path_hit) / n
    valid = sum(1 for c in cases if c.sql_valid) / n
    matched = sum(1 for c in cases if c.execution_match is True)
    matched_total = sum(1 for c in cases if c.execution_match is not None)
    exec_acc = matched / matched_total if matched_total else 0.0
    leakage = sum(1 for c in cases if c.leaked_descriptors) / n

    totals = [c.timings_ms.get("total_ms", 0.0) for c in cases if c.timings_ms]
    per_stage_p50: dict[str, float] = {}
    if cases:
        stages: set[str] = set()
        for c in cases:
            stages.update(c.timings_ms.keys())
        for s in stages:
            vals = [c.timings_ms.get(s, 0.0) for c in cases]
            per_stage_p50[s] = _percentile(vals, 0.5)
    return Metrics(
        n_cases=n,
        schema_linking_recall=schema_hits,
        join_path_exactness=join_hits,
        sql_syntactic_validity=valid,
        execution_accuracy=exec_acc,
        descriptor_leakage_rate=leakage,
        latency_total_ms_p50=_percentile(totals, 0.5),
        latency_total_ms_p95=_percentile(totals, 0.95),
        latency_total_ms_p99=_percentile(totals, 0.99),
        per_stage_p50=per_stage_p50,
    )


# ── Top-level runner ─────────────────────────────────────────────────────────


def run_evaluation(
    pipeline: Text2SqlPipeline,
    store: GoldStore,
    *,
    max_cases: int | None = None,
    approval_status: str = "approved",
) -> EvalReport:
    records = store.list(approval_status=approval_status, limit=max_cases or 1000)
    if max_cases:
        records = records[:max_cases]
    cases: list[CaseResult] = []
    for i, rec in enumerate(records):
        log.info("[%d/%d] %s", i + 1, len(records), rec.nl_question[:80])
        try:
            t0 = time.perf_counter()
            result = pipeline.answer(rec.nl_question, execute=True, max_rows=1000)
            log.info("  → %.1fs", time.perf_counter() - t0)
        except Exception as e:
            log.warning("  pipeline crashed: %s", e)
            cases.append(CaseResult(
                nl_question=rec.nl_question,
                gold_id=str(rec.id),
                gold_tables=list(rec.tables_used),
                generated_sql="",
                gold_sql=rec.sql_text,
                error=f"pipeline crash: {e}",
            ))
            continue
        case = grade_case(rec, result, sql_engine=pipeline.sql_engine)
        cases.append(case)
    return EvalReport(
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        metrics=_aggregate(cases),
        cases=cases,
    )
