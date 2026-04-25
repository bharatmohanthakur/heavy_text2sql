"""Head-to-head LLM benchmark for SQL generation.

Builds the same prompt context once per question (using the existing
pipeline up to context assembly), then sends it to N LLMs and grades
the resulting SQL on the live Postgres:
  - validated  (parse + EXPLAIN)
  - executed   (returns rows without error)
  - row_count
  - tokens & latency reported by each provider

Usage:
    .venv/bin/python scripts/bench_llms.py \
        --questions data/eval/tough_questions.yaml \
        --providers azure-gpt-4o openrouter-glm-5.1 \
        --out-md data/eval/runs/bench_llms.md
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "packages/backend/src"))

from text2sql.classification import (
    QueryDomainClassifier,
    load_domain_catalog,
    read_table_mapping,
)
from text2sql.config import load_config
from text2sql.embedding import TableRetriever
from text2sql.entity_resolution import EntityResolver, build_value_index
from text2sql.gold import GoldStore
from text2sql.graph import build_graph, parse_fks
from text2sql.ingestion.edfi_fetcher import IngestionConfig, IngestionManifest
from text2sql.pipeline import Text2SqlPipeline
from text2sql.providers import build_embedding, build_llm, build_sql_engine, build_vector_store
from text2sql.table_catalog import load_table_catalog


@dataclass
class CaseRun:
    provider: str
    sql: str = ""
    rationale: str = ""
    validated: bool = False
    executed: bool = False
    row_count: int | None = None
    elapsed_ms: float = 0.0
    error: str | None = None
    repair_attempts: int = 0


@dataclass
class CaseReport:
    qid: str
    question: str
    runs: dict[str, CaseRun] = field(default_factory=dict)


def _build_pipeline(llm_provider_name: str) -> Text2SqlPipeline:
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = IngestionManifest.from_json((ic.cache_dir / "manifest.json").read_text())
    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
    edges = []
    for art in manifest.artifacts:
        edges.extend(parse_fks(art.foreign_keys_sql_path))
    classifications = read_table_mapping(
        REPO_ROOT / "data/artifacts/table_classification.json"
    ).classifications
    graph = build_graph(edges, classifications=classifications)
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    sql_engine = build_sql_engine(cfg.target_db_provider())
    domain_classifier_llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    domain_catalog = load_domain_catalog(manifest)
    domain_classifier = QueryDomainClassifier(
        domain_classifier_llm, domain_catalog, cache_path=None,
    )
    retriever = TableRetriever(embedder, store)
    value_index = build_value_index(catalog)
    entity_resolver = EntityResolver(
        value_index, embedder=embedder, store=store, llm=domain_classifier_llm,
    )

    gold_store = None
    try:
        import os
        spec = cfg.metadata_db.model_dump()
        pw = os.environ.get("METADATA_DB_PASSWORD") or os.environ.get("TARGET_DB_PASSWORD") or "edfi"
        url = (
            f"postgresql+psycopg://{spec['user']}:{pw}"
            f"@{spec['host']}:{spec['port']}/{spec['database']}"
        )
        gold_store = GoldStore(url, embedder, catalog=catalog)
    except Exception:
        pass

    # The provider under test only swaps SQL-generation + repair.
    sql_gen_llm = build_llm(cfg.llm.providers[llm_provider_name])

    return Text2SqlPipeline(
        catalog=catalog,
        graph=graph,
        domain_classifier=domain_classifier,
        retriever=retriever,
        entity_resolver=entity_resolver,
        gold_store=gold_store,
        sql_engine=sql_engine,
        llm=sql_gen_llm,
    )


def run_bench(
    questions: list[dict],
    providers: list[str],
    out_json: Path,
    out_md: Path,
) -> None:
    # Build one pipeline per provider — they share embeddings / retrieval / FK
    # graph / etc. only the SQL-gen LLM differs.
    pipelines: dict[str, Text2SqlPipeline] = {}
    for p in providers:
        print(f"Building pipeline for {p}…")
        pipelines[p] = _build_pipeline(p)

    reports: list[CaseReport] = []
    for i, spec in enumerate(questions, 1):
        qid = spec.get("id", f"q{i}")
        q = spec["q"]
        rep = CaseReport(qid=qid, question=q)
        print(f"\n[{i}/{len(questions)}] {qid}: {q!r}")
        for p, pipe in pipelines.items():
            t0 = time.perf_counter()
            try:
                result = pipe.answer(q, execute=True, max_rows=20)
                run = CaseRun(
                    provider=p,
                    sql=result.sql,
                    rationale=result.rationale,
                    validated=result.validated,
                    executed=result.executed,
                    row_count=result.row_count,
                    elapsed_ms=(time.perf_counter() - t0) * 1000,
                    error=result.error,
                    repair_attempts=max(0, len(result.repair_attempts) - 1),
                )
            except Exception as e:
                run = CaseRun(
                    provider=p,
                    elapsed_ms=(time.perf_counter() - t0) * 1000,
                    error=str(e),
                )
            mark = "✓" if (run.validated and run.executed) else "✗"
            print(
                f"  {mark} {p:38s} validated={run.validated} executed={run.executed} "
                f"rows={run.row_count} repairs={run.repair_attempts} ({run.elapsed_ms:.0f} ms)"
            )
            rep.runs[p] = run
        reports.append(rep)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps([
        {
            "qid": r.qid,
            "question": r.question,
            "runs": {p: r.runs[p].__dict__ for p in r.runs},
        }
        for r in reports
    ], indent=2, default=str))

    write_md(reports, providers, out_md)
    summarize(reports, providers)
    print(f"\nReports:\n  {out_json}\n  {out_md}")


def write_md(reports: list[CaseReport], providers: list[str], path: Path) -> None:
    lines: list[str] = ["# LLM head-to-head benchmark", ""]
    # Top-line summary table
    lines.append("| Provider | Validated | Executed | Avg latency (ms) | Repair-loop fires |")
    lines.append("|---|---|---|---|---|")
    for p in providers:
        valid = sum(1 for r in reports if r.runs[p].validated)
        exec_ok = sum(1 for r in reports if r.runs[p].executed)
        avg_ms = sum(r.runs[p].elapsed_ms for r in reports) / max(1, len(reports))
        repairs = sum(r.runs[p].repair_attempts for r in reports)
        lines.append(
            f"| `{p}` | {valid}/{len(reports)} | {exec_ok}/{len(reports)} "
            f"| {avg_ms:.0f} | {repairs} |"
        )
    lines.append("")
    for rep in reports:
        lines.append(f"## `{rep.qid}`")
        lines.append(f"> _{rep.question}_")
        lines.append("")
        for p in providers:
            run = rep.runs[p]
            ok = "✅" if (run.validated and run.executed) else "❌"
            lines.append(
                f"### {ok} {p}  "
                f"validated={run.validated} executed={run.executed} "
                f"rows={run.row_count} repairs={run.repair_attempts} "
                f"latency={run.elapsed_ms:.0f}ms"
            )
            if run.error:
                lines.append(f"- error: `{run.error[:300]}`")
            if run.sql:
                lines.append("```sql")
                lines.append(run.sql)
                lines.append("```")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def summarize(reports: list[CaseReport], providers: list[str]) -> None:
    print("\n=== summary ===")
    print(f"{'provider':40s}  valid   exec   avg-ms   repairs")
    for p in providers:
        valid = sum(1 for r in reports if r.runs[p].validated)
        exec_ok = sum(1 for r in reports if r.runs[p].executed)
        avg_ms = sum(r.runs[p].elapsed_ms for r in reports) / max(1, len(reports))
        repairs = sum(r.runs[p].repair_attempts for r in reports)
        print(
            f"{p:40s}  {valid:>2}/{len(reports):<2}    {exec_ok:>2}/{len(reports):<2}    "
            f"{avg_ms:>5.0f}    {repairs}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--questions",
        default=str(REPO_ROOT / "data/eval/tough_questions.yaml"),
    )
    ap.add_argument(
        "--providers",
        nargs="+",
        default=["azure-gpt-4o", "openrouter-glm-5.1"],
    )
    ap.add_argument("--out-json", default=str(REPO_ROOT / "data/eval/runs/bench_llms.json"))
    ap.add_argument("--out-md", default=str(REPO_ROOT / "data/eval/runs/bench_llms.md"))
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    qs = yaml.safe_load(Path(args.questions).read_text())["queries"]
    if args.limit:
        qs = qs[: args.limit]
    run_bench(qs, args.providers, Path(args.out_json), Path(args.out_md))


if __name__ == "__main__":
    main()
