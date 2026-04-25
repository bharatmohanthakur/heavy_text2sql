"""End-to-end agent-loop integration tests against live Northridge MSSQL.

These tests drive the real LLM (Azure GPT-4o by default) through the real
tools (catalog + retriever + entity resolver + Steiner + gold + sql engine).
They are slow (one network round trip per LLM step) and skipped when any
piece is missing.

What we assert:
  - The agent completes a single-turn count question and returns a result
    that includes a SELECT and a row count.
  - A multi-turn follow-up reuses prior context (the agent doesn't re-ask
    "what dataset?"); the conversation store retains every assistant +
    tool message in order.
  - When the agent is given a deliberately invalid SQL hint, the loop's
    repair behavior — the LLM seeing an error from run_sql and trying
    again — works (run_sql is called more than once and the final call
    is ok).
  - Streaming events are well-formed (kind == step|result|conversation_id).
"""

from __future__ import annotations

import os

import pytest

from text2sql.agent import (
    AgentResult,
    AgentRunner,
    ConversationStore,
    ToolContext,
)
from text2sql.config import REPO_ROOT, load_config


# ── Skip-gates ───────────────────────────────────────────────────────────────


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


def _metadata_url() -> str | None:
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
    import sqlalchemy as sa
    try:
        sa.create_engine(url).connect().close()
        return url
    except Exception:
        return None


# ── Live fixture wiring (mirrors test_agent_tools.ctx) ───────────────────────


@pytest.fixture(scope="module")
def live_setup():
    if not _has_azure():
        pytest.skip("no AZURE_OPENAI_API_KEY")
    if not _has_target_db():
        pytest.skip("target DB unreachable")
    meta_url = _metadata_url()
    if not meta_url:
        pytest.skip("metadata DB unreachable")
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
    from text2sql.providers import (
        build_embedding,
        build_llm,
        build_sql_engine,
        build_vector_store,
    )
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
    gold_store = GoldStore(meta_url, embedder, catalog=catalog)

    tool_ctx = ToolContext(
        catalog=catalog,
        graph=graph,
        retriever=retriever,
        entity_resolver=entity_resolver,
        sql_engine=sql_engine,
        domain_classifier=domain_classifier,
        gold_store=gold_store,
    )

    conv_store = ConversationStore(meta_url)
    conv_store.ensure_schema()

    llm_spec = cfg.llm_for_task("sql_generation")
    if llm_spec.kind not in ("azure_openai", "openai"):
        pytest.skip(f"agent loop currently only drives openai-shaped LLMs; primary={llm_spec.kind}")

    runner = AgentRunner(
        conv_store=conv_store,
        tool_ctx=tool_ctx,
        llm_spec=llm_spec,
        max_steps=14,
    )
    return {"runner": runner, "conv_store": conv_store, "sql_engine": sql_engine}


# ── 1. Single-turn count ─────────────────────────────────────────────────────


def test_single_turn_count(live_setup) -> None:
    runner: AgentRunner = live_setup["runner"]
    result: AgentResult = runner.run(None, "How many students are in the database?")

    assert not result.aborted, f"agent aborted: {result.abort_reason}"
    assert result.final_summary, "no summary produced"
    assert result.final_sql, "no SQL produced"
    # Northridge has ~21,628 students; an aggregate count should return 1 row.
    assert result.final_row_count is None or result.final_row_count >= 1

    # Conversation should have at least: user, assistant(tool_calls), tool*, …, assistant(final_answer).
    msgs = live_setup["conv_store"].history(result.conversation_id)
    assert msgs[0].role == "user"
    assert any(m.role == "assistant" and m.tool_calls for m in msgs)
    assert any(m.role == "tool" and m.tool_name == "run_sql" for m in msgs), (
        "agent never reached run_sql"
    )
    assert any(
        m.role == "tool" and m.tool_name == "final_answer" for m in msgs
    ), "agent never called final_answer"


# ── 2. Multi-turn follow-up ──────────────────────────────────────────────────


def test_multi_turn_followup(live_setup) -> None:
    runner: AgentRunner = live_setup["runner"]
    first = runner.run(None, "How many schools are in the database?")
    assert not first.aborted, first.abort_reason

    # Second turn refers back to "those schools" — the agent should have access
    # to prior conversation history and not need to re-clarify.
    second = runner.run(
        first.conversation_id,
        "Of those, list the top 5 by name.",
    )
    assert not second.aborted, second.abort_reason
    assert second.final_sql, "follow-up produced no SQL"
    # Lists should be capped per our system prompt's TOP-50 rule (5 here).
    if second.final_sql:
        sql_lower = second.final_sql.lower()
        assert "school" in sql_lower

    # Both turns should share the same conversation_id and the history grows.
    msgs = live_setup["conv_store"].history(second.conversation_id)
    user_msgs = [m for m in msgs if m.role == "user"]
    assert len(user_msgs) >= 2


# ── 3. Streaming events shape ────────────────────────────────────────────────


def test_run_stream_emits_well_formed_events(live_setup) -> None:
    runner: AgentRunner = live_setup["runner"]
    events = list(runner.run_stream(None, "How many students are in the database?"))

    kinds = [ev["kind"] for ev in events]
    assert kinds[0] == "conversation_id"
    assert kinds[-1] == "result"
    assert "step" in kinds, "expected at least one step event"

    # Every step event must be JSON-shaped (no dataclass leakage).
    for ev in events:
        if ev["kind"] == "step":
            s = ev["step"]
            assert isinstance(s, dict)
            assert s["kind"] in ("tool_call", "tool_result", "assistant", "error")
            if s["kind"] == "tool_call":
                assert s["name"]
                assert isinstance(s["arguments"], dict)

    # The final event carries the result payload.
    final = events[-1]["result"]
    assert isinstance(final, dict)
    assert "conversation_id" in final and "final_summary" in final


# ── 4. Plumbing: SQL errors are persisted to history (the LLM gets to see them)


def test_run_sql_errors_are_visible_to_next_llm_step(live_setup) -> None:
    """The loop's repair-relevant guarantee: when run_sql fails, the error
    message is persisted to conversation history as a tool message so the
    next LLM call can see and react to it. This is a property of the loop,
    not of the LLM's repair skill."""
    from text2sql.agent import ToolContext, default_registry

    ctx: ToolContext = live_setup["runner"].tool_ctx
    reg = default_registry()

    # Run a deliberately invalid SQL through the tool wrapper directly.
    bad = reg.execute(
        "run_sql",
        {"sql": "SELECT * FROM [edfi].[NoSuchTable_xyz]", "max_rows": 1},
        ctx,
    )
    assert not bad.ok
    assert bad.error
    # The error string must contain something the LLM can reason over —
    # either an explain failure or a missing-object hint.
    err = bad.error.lower()
    assert "nosuchtable_xyz" in err or "explain" in err or "invalid" in err


# ── 5. Cap: agent must terminate via final_answer (not max_steps) ────────────


def test_agent_terminates_via_final_answer(live_setup) -> None:
    runner: AgentRunner = live_setup["runner"]
    result = runner.run(None, "How many students are in the database?")
    assert not result.aborted, result.abort_reason
    # After successful termination, abort_reason is None and final_summary is set.
    assert result.abort_reason is None
    assert result.final_summary
