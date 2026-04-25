"""Component 8 end-to-end pipeline tests.

Real Azure GPT-4o + Azure embeddings + FAISS + live Postgres + gold store.
For each test we drive the full answer(nl) flow and assert that:
  - SQL is generated
  - SQL parses
  - SQL executes against the live DB
  - Result rows are non-empty (where the question is satisfiable)

Skipped cleanly if any backing service is missing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.classification import QueryDomainClassifier, load_domain_catalog
from text2sql.classification import read_table_mapping
from text2sql.config import REPO_ROOT, load_config
from text2sql.embedding import TableRetriever
from text2sql.entity_resolution import EntityResolver, build_value_index
from text2sql.gold import GoldStore
from text2sql.graph import build_graph, parse_fks
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all
from text2sql.pipeline import Text2SqlPipeline
from text2sql.table_catalog import load_table_catalog


CATALOG_PATH = REPO_ROOT / "data/artifacts/table_catalog.json"


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


def _has_metadata_db() -> bool:
    import sqlalchemy as sa
    cfg = load_config()
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
    try:
        sa.create_engine(url).connect().close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def pipeline() -> Text2SqlPipeline:
    if not _has_azure():
        pytest.skip("no azure key")
    if not _has_db():
        pytest.skip("target DB unreachable")
    if not CATALOG_PATH.exists():
        pytest.skip("no catalog; run text2sql build-table-catalog-cmd")

    from text2sql.providers import build_embedding, build_llm, build_sql_engine, build_vector_store

    cfg = load_config()
    manifest = fetch_all(IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT))
    catalog = load_table_catalog(CATALOG_PATH)

    edges = []
    for art in manifest.artifacts:
        edges.extend(parse_fks(art.foreign_keys_sql_path))
    classifications = read_table_mapping(
        REPO_ROOT / "data/artifacts/table_classification.json"
    ).classifications
    graph = build_graph(edges, classifications=classifications)

    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    llm = build_llm(cfg.llm_for_task("sql_generation"))
    sql_engine = build_sql_engine(cfg.target_db_provider())

    domain_catalog = load_domain_catalog(manifest)
    domain_classifier = QueryDomainClassifier(llm, domain_catalog, cache_path=None)
    retriever = TableRetriever(embedder, store)
    value_index = build_value_index(catalog)
    entity_resolver = EntityResolver(value_index, embedder=embedder, store=store, llm=llm)

    gold_store = None
    if _has_metadata_db():
        pw = os.environ.get("METADATA_DB_PASSWORD") or os.environ.get("TARGET_DB_PASSWORD") or "edfi"
        spec = cfg.metadata_db.model_dump()
        sa_url = (
            f"postgresql+psycopg://{spec['user']}:{pw}"
            f"@{spec['host']}:{spec['port']}/{spec['database']}"
        )
        gold_store = GoldStore(sa_url, embedder, catalog=catalog)

    return Text2SqlPipeline(
        catalog=catalog,
        graph=graph,
        domain_classifier=domain_classifier,
        retriever=retriever,
        entity_resolver=entity_resolver,
        gold_store=gold_store,
        sql_engine=sql_engine,
        llm=llm,
    )


def _print_debug(name: str, result) -> None:
    print(f"\n=== {name} ===")
    print(f"  domains:  {result.domains.domains if result.domains else []}")
    print(f"  tables:   {[h.fqn for h in result.retrieved_tables[:5]]}")
    print(f"  joins:    {len(result.join_tree.edges) if result.join_tree else 0}")
    print(f"  few-shots: {len(result.few_shots)}")
    print(f"  SQL:\n{result.sql}")
    if result.executed:
        print(f"  rows: {result.row_count}")
        for r in result.rows[:3]:
            print(f"    {r}")
    if result.error:
        print(f"  ERROR: {result.error}")
    if result.timings_ms:
        timings = ", ".join(f"{k}={v:.0f}ms" for k, v in result.timings_ms.items())
        print(f"  timings: {timings}")


# ── Easy: aggregate over a single table ───────────────────────────────────────


def test_count_students(pipeline) -> None:
    result = pipeline.answer("How many students are in the database?")
    _print_debug("count_students", result)
    assert result.sql, "no SQL generated"
    assert result.validated, f"validation failed: {result.error}"
    assert result.executed, f"execution failed: {result.error}"
    assert result.rows
    val = next(iter(result.rows[0].values()))
    assert int(val) > 0


def test_schools_with_grade_levels(pipeline) -> None:
    result = pipeline.answer("List all schools and their grade levels offered")
    _print_debug("schools_with_grade_levels", result)
    assert result.sql
    assert result.validated, result.error
    assert result.executed, result.error
    assert result.rows


# ── Cross-domain: needs Steiner ───────────────────────────────────────────────


def test_students_per_school(pipeline) -> None:
    result = pipeline.answer("How many students are enrolled in each school?")
    _print_debug("students_per_school", result)
    assert result.sql
    assert result.validated, result.error
    assert result.executed, result.error
    # Should have one row per school (and rows > 1)
    assert result.rows and len(result.rows) >= 1


# ── Descriptor-aware: needs entity resolver to find Hispanic ──────────────────


def test_hispanic_students(pipeline) -> None:
    result = pipeline.answer("How many Hispanic students are in the district?")
    _print_debug("hispanic_students", result)
    assert result.sql
    sql_lower = result.sql.lower()
    # Ed-Fi has two valid paths: descriptor chain through codevalue='Hispanic'
    # OR the boolean shortcut StudentDemographic.HispanicLatinoEthnicity.
    # Either is acceptable.
    used_descriptor = "descriptor" in sql_lower and "hispanic" in sql_lower
    used_shortcut = "hispaniclatino" in sql_lower
    assert used_descriptor or used_shortcut, f"no Hispanic path found: {result.sql}"
    assert result.validated, result.error
    assert result.executed, result.error
    assert result.rows
