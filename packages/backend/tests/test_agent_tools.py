"""Per-tool integration tests.

For every tool we:
  1. Build the underlying component directly.
  2. Build the same context, call the tool wrapper.
  3. Assert the tool's data matches what the component returned — proving the
     wrapper preserves the original intent and behavior.

Skipped cleanly if Azure / DB / metadata DB aren't reachable.
"""

from __future__ import annotations

import json
import os

import pytest

from text2sql.agent import ToolContext, default_registry
from text2sql.config import REPO_ROOT, load_config


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


def _has_target_db() -> bool:
    cfg = load_config()
    try:
        from text2sql.providers import build_sql_engine
        e = build_sql_engine(cfg.target_db_provider())
        e.execute("SELECT 1 AS ok")
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def ctx() -> ToolContext:
    if not _has_azure():
        pytest.skip("no azure key")
    if not _has_target_db():
        pytest.skip("target DB unreachable")
    catalog_path = REPO_ROOT / "data/artifacts/table_catalog.json"
    if not catalog_path.exists():
        pytest.skip("no table catalog; run build-table-catalog-cmd first")

    cfg = load_config()
    from text2sql.classification import (
        QueryDomainClassifier,
        load_domain_catalog,
        read_table_mapping,
    )
    from text2sql.embedding import TableRetriever
    from text2sql.entity_resolution import EntityResolver, build_value_index
    from text2sql.gold import GoldStore
    from text2sql.graph import build_graph, parse_fks
    from text2sql.ingestion.edfi_fetcher import IngestionConfig, IngestionManifest
    from text2sql.providers import build_embedding, build_llm, build_sql_engine, build_vector_store
    from text2sql.table_catalog import load_table_catalog

    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = IngestionManifest.from_json((ic.cache_dir / "manifest.json").read_text())
    catalog = load_table_catalog(catalog_path)
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
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    domain_classifier = QueryDomainClassifier(
        llm, load_domain_catalog(manifest), cache_path=None,
    )
    retriever = TableRetriever(embedder, store)
    value_index = build_value_index(catalog)
    entity_resolver = EntityResolver(
        value_index, embedder=embedder, store=store, llm=llm,
    )
    gold_store = None
    try:
        pw = (
            os.environ.get("METADATA_DB_PASSWORD")
            or os.environ.get("TARGET_DB_PASSWORD") or "edfi"
        )
        spec = cfg.metadata_db.model_dump()
        sa_url = (
            f"postgresql+psycopg://{spec['user']}:{pw}"
            f"@{spec['host']}:{spec['port']}/{spec['database']}"
        )
        gold_store = GoldStore(sa_url, embedder, catalog=catalog)
        # Defensive: another test suite (component7) might have torn down
        # the schema. Idempotent ensure_schema fixes that without disturbing
        # any rows the operator seeded via `text2sql gold-seed`.
        gold_store.ensure_schema()
    except Exception:
        pass

    return ToolContext(
        catalog=catalog,
        graph=graph,
        retriever=retriever,
        entity_resolver=entity_resolver,
        sql_engine=sql_engine,
        domain_classifier=domain_classifier,
        gold_store=gold_store,
    )


@pytest.fixture(scope="module")
def reg():
    return default_registry()


# ── classify_domains ─────────────────────────────────────────────────────────


def test_classify_domains_wrapper_preserves_intent(ctx: ToolContext, reg) -> None:
    direct = ctx.domain_classifier.classify("How many students enrolled last year?")
    out = reg.execute(
        "classify_domains",
        {"question": "How many students enrolled last year?"},
        ctx,
    )
    assert out.ok
    assert out.data["domains"] == direct.domains
    assert out.data["primary"] == direct.primary
    assert "Enrollment" in out.data["domains"]


# ── search_tables ────────────────────────────────────────────────────────────


def test_search_tables_wrapper_matches_retriever(ctx: ToolContext, reg) -> None:
    query = "students absent attendance event"
    direct = ctx.retriever.search(query, k=5, hybrid=True)
    out = reg.execute(
        "search_tables",
        {"query": query, "k": 5, "domains": []},
        ctx,
    )
    assert out.ok
    direct_fqns = [h.fqn for h in direct]
    wrapped_fqns = [h["fqn"] for h in out.data["hits"]]
    assert direct_fqns == wrapped_fqns


def test_search_tables_domain_filter(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "search_tables",
        {"query": "attendance", "k": 5, "domains": ["StudentAttendance"]},
        ctx,
    )
    assert out.ok
    for h in out.data["hits"]:
        assert "StudentAttendance" in h["domains"] or h["domains"] == []


# ── inspect_table ────────────────────────────────────────────────────────────


def test_inspect_table_returns_columns(ctx: ToolContext, reg) -> None:
    out = reg.execute("inspect_table", {"fqn": "edfi.Student"}, ctx)
    assert out.ok
    assert out.data["fqn"] == "edfi.Student"
    assert out.data["columns"]
    assert any("StudentUSI" in (c["name"] or "") for c in out.data["columns"])
    assert "StudentUSI" in (out.data["primary_key"] or [])


def test_inspect_table_404(ctx: ToolContext, reg) -> None:
    out = reg.execute("inspect_table", {"fqn": "edfi.NotARealTable"}, ctx)
    assert not out.ok


# ── resolve_entity ───────────────────────────────────────────────────────────


def test_resolve_entity_finds_descriptor_code(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "resolve_entity",
        {"phrase": "Hispanic", "domains": []},
        ctx,
    )
    assert out.ok
    chosen = out.data.get("chosen")
    assert chosen is not None
    assert "Hispanic" in chosen["value"]


# ── find_join_path ───────────────────────────────────────────────────────────


def test_find_join_path_two_tables(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "find_join_path",
        {"tables": ["edfi.Student", "edfi.School"], "dialect": "mssql"},
        ctx,
    )
    assert out.ok
    assert "edfi.Student" in out.data["tree_nodes"]
    assert "edfi.School" in out.data["tree_nodes"]
    assert out.data["join_clauses"]
    # MSSQL dialect uses [bracket] quoting
    assert any("[" in c and "]" in c for c in out.data["join_clauses"])


def test_find_join_path_rejects_singleton(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "find_join_path",
        {"tables": ["edfi.Student"], "dialect": "mssql"},
        ctx,
    )
    assert not out.ok


# ── find_similar_queries ─────────────────────────────────────────────────────


def test_find_similar_queries_returns_examples(ctx: ToolContext, reg) -> None:
    if ctx.gold_store is None:
        pytest.skip("gold store not configured")
    try:
        if ctx.gold_store.count() == 0:
            pytest.skip("gold store empty — run text2sql gold-seed first")
    except Exception as e:
        # gold_sql table missing (e.g. torn down by another suite) — skip
        # cleanly rather than blow up the whole agent_tools suite.
        pytest.skip(f"gold store unavailable: {e}")
    out = reg.execute(
        "find_similar_queries",
        {"question": "How many students enrolled in each school?", "k": 2, "domains": []},
        ctx,
    )
    assert out.ok
    assert out.data["examples"]
    for ex in out.data["examples"]:
        assert ex["nl"]
        assert ex["sql"]


# ── run_sql ──────────────────────────────────────────────────────────────────


def test_run_sql_executes_simple_select(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "run_sql",
        {"sql": "SELECT TOP 1 [StudentUSI] FROM [edfi].[Student]", "max_rows": 1},
        ctx,
    )
    assert out.ok, out.error
    assert out.data["row_count"] == 1


def test_run_sql_rejects_invalid(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "run_sql",
        {"sql": "SELECT * FROM edfi.NotARealTable123", "max_rows": 1},
        ctx,
    )
    assert not out.ok
    assert "edfi.NotARealTable123".lower() in (out.error or "").lower() or "explain" in (out.error or "").lower()


def test_run_sql_blocks_non_select(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "run_sql",
        {"sql": "DELETE FROM [edfi].[Student]", "max_rows": 1},
        ctx,
    )
    assert not out.ok
    assert "non-SELECT" in (out.error or "")


# ── final_answer ─────────────────────────────────────────────────────────────


def test_final_answer_terminates(ctx: ToolContext, reg) -> None:
    out = reg.execute(
        "final_answer",
        {"summary": "There are 21,628 students.", "sql": "SELECT COUNT(*) FROM edfi.Student", "row_count": 1},
        ctx,
    )
    assert out.ok
    assert out.is_terminal
    assert out.data["summary"] == "There are 21,628 students."


def test_final_answer_requires_summary(ctx: ToolContext, reg) -> None:
    out = reg.execute("final_answer", {"summary": "", "sql": None, "row_count": None}, ctx)
    assert not out.ok


# ── Registry sanity ──────────────────────────────────────────────────────────


def test_registry_emits_openai_tools_shape(reg) -> None:
    tools = reg.to_openai_tools()
    assert len(tools) == 8
    names = {t["function"]["name"] for t in tools}
    assert names == {
        "classify_domains", "search_tables", "inspect_table", "resolve_entity",
        "find_join_path", "find_similar_queries", "run_sql", "final_answer",
    }
    for t in tools:
        assert t["type"] == "function"
        assert "parameters" in t["function"]
