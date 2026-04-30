"""text2sql CLI — Typer entry point."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

# Windows ships with cp1252 as the default stdout/stderr encoding, which
# explodes on the first non-ASCII byte (curly quotes, →, emojis,
# accented chars in error messages from libraries we don't control).
# Force utf-8 so `text2sql ingest`, `serve`, `chat`, and every other
# subcommand can print human-friendly output everywhere. errors="replace"
# means even a stray binary byte renders as `?` instead of crashing.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

from text2sql.config import REPO_ROOT, load_config
from text2sql.ingestion.edfi_fetcher import IngestionConfig, IngestionManifest, fetch_all, verify_manifest

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command()
def ingest(force: bool = False, verify: bool = True) -> None:
    """Fetch Ed-Fi artifacts (ApiModel.json + 0030-ForeignKeys.sql) into data/edfi/."""
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    typer.echo(f"DS {ic.data_standard_version} ({ic.sql_dialect}) → {ic.cache_dir}")
    manifest = fetch_all(ic, force=force)
    typer.echo(f"\nFetched {len(manifest.artifacts)} artifact set(s):")
    for a in manifest.artifacts:
        c = manifest.counts[a.source]
        typer.echo(f"  {a.source}: {c['entities']} entities, {c['fks']} FKs, "
                   f"{c['aggregates']} aggregates, {c['domains']} domains, "
                   f"{c['descriptors']} descriptors")
    if verify:
        try:
            verify_manifest(manifest)
            typer.echo("\n✅ verification passed")
        except Exception as e:
            typer.echo(f"\n❌ verification failed:\n{e}", err=True)
            sys.exit(1)
    typer.echo(f"\nManifest: {ic.cache_dir / 'manifest.json'}")


@app.command()
def show_config() -> None:
    """Print resolved AppConfig (secrets redacted)."""
    cfg = load_config()
    blob = cfg.model_dump()
    blob_str = json.dumps(blob, default=str, indent=2)
    typer.echo(blob_str)


@app.command()
def map_tables_cmd(
    out: Path = typer.Option(Path("data/artifacts/table_classification.json")),
) -> None:
    """Component 2a: parse ApiModel.json → table → domain mapping (no LLM in 99% of cases)."""
    from text2sql.classification import load_domain_catalog, map_tables, write_table_mapping
    from text2sql.classification.metadata import CatalogIndex
    from text2sql.providers import build_llm

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest_path = ic.cache_dir / "manifest.json"
    if not manifest_path.exists():
        typer.echo("No ingest manifest. Run `text2sql ingest` first.", err=True)
        sys.exit(1)
    manifest = IngestionManifest.from_json(manifest_path.read_text(encoding="utf-8"))
    catalog = load_domain_catalog(manifest)
    index = CatalogIndex.from_manifest(manifest)

    # Provide an LLM for the (rare) residuals — only fires if needed.
    llm = None
    try:
        llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    except Exception as e:
        typer.echo(f"(no LLM available — residuals will be marked 'Other'): {e}")

    classifications = map_tables(
        index, catalog,
        llm=llm,
        overrides_path=REPO_ROOT / "configs" / "domain_overrides.yaml",
    )
    out_path = REPO_ROOT / out
    output = write_table_mapping(
        out_path, classifications,
        data_standard_version=manifest.data_standard_version,
        catalog=catalog,
    )
    typer.echo(f"\nWrote {out_path}")
    typer.echo(f"  total: {output.summary['total']}")
    typer.echo(f"  by source: {output.summary['by_source']}")
    typer.echo(f"  with secondary domain: {output.summary['with_secondary']}")


@app.command()
def build_fk_graph(
    classification: Path = typer.Option(Path("data/artifacts/table_classification.json")),
    out: Path = typer.Option(Path("data/artifacts/graph")),
) -> None:
    """Component 3: parse FKs → build graph → APSP → persist artifacts."""
    from text2sql.classification import read_table_mapping
    from text2sql.graph import build_graph, parse_fks, save_graph

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest_path = ic.cache_dir / "manifest.json"
    if not manifest_path.exists():
        typer.echo("No ingest manifest. Run `text2sql ingest` first.", err=True)
        sys.exit(1)
    manifest = IngestionManifest.from_json(manifest_path.read_text(encoding="utf-8"))

    edges = []
    for art in manifest.artifacts:
        edges.extend(parse_fks(art.foreign_keys_sql_path))
    typer.echo(f"Parsed {len(edges)} FK edges")

    classifications = []
    cl_path = REPO_ROOT / classification
    if cl_path.exists():
        classifications = read_table_mapping(cl_path).classifications
        typer.echo(f"Using {len(classifications)} classifications for edge weighting")
    else:
        typer.echo("(no classification file — using default edge weights)")

    g = build_graph(edges, classifications=classifications)
    typer.echo(f"Graph: {len(g.nodes)} nodes, {len(g.edges)} undirected edges")
    save_graph(g, REPO_ROOT / out)
    typer.echo(f"Wrote artifacts to {REPO_ROOT / out}")


@app.command()
def build_table_catalog_cmd(
    classification: Path = typer.Option(Path("data/artifacts/table_classification.json")),
    out: Path = typer.Option(Path("data/artifacts/table_catalog.json")),
    skip_db: bool = typer.Option(False, help="Skip live value sampling (faster, offline)"),
    skip_llm: bool = typer.Option(False, help="Skip LLM gap-fill of missing column descriptions"),
    only: str | None = typer.Option(None, help="Comma-separated list of fqns to build (default: all)"),
) -> None:
    """Component 4: build the table catalog (one record per table; domains as tags)."""
    from text2sql.classification import read_table_mapping
    from text2sql.classification.metadata import CatalogIndex
    from text2sql.providers import build_llm, build_sql_engine
    from text2sql.table_catalog import (
        DescriptionGenerator,
        build_table_catalog,
        save_table_catalog,
    )

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest_path = ic.cache_dir / "manifest.json"
    if not manifest_path.exists():
        typer.echo("No ingest manifest. Run `text2sql ingest` first.", err=True)
        sys.exit(1)
    manifest = IngestionManifest.from_json(manifest_path.read_text(encoding="utf-8"))

    cl_path = REPO_ROOT / classification
    if not cl_path.exists():
        typer.echo(f"No classification file at {cl_path}. Run `text2sql map-tables-cmd` first.", err=True)
        sys.exit(1)
    classifications = read_table_mapping(cl_path).classifications
    catalog_index = CatalogIndex.from_manifest(manifest)

    engine = None
    if not skip_db:
        try:
            engine = build_sql_engine(cfg.target_db_provider())
            typer.echo(f"SQL engine: {engine.dialect}")
        except Exception as e:
            typer.echo(f"(no DB connection — skipping value sampling): {e}")

    desc_gen = None
    if not skip_llm:
        try:
            # Component 4 — catalog description generation has its own task
            # slot so users can route a cheaper model here (it runs once
            # per ~800 tables at build time).
            llm = build_llm(cfg.llm_for_task("catalog_description"))
            desc_gen = DescriptionGenerator(
                llm,
                cache_path=REPO_ROOT / "data/artifacts/.description_cache.json",
            )
            typer.echo(f"Description LLM (gap-fill only): {llm.model_id}")
        except Exception as e:
            typer.echo(f"(no LLM — skipping gap-fill): {e}")

    only_set = set(only.split(",")) if only else None

    catalog = build_table_catalog(
        classifications, catalog_index, manifest,
        sql_engine=engine,
        description_generator=desc_gen,
        only_fqns=only_set,
    )
    save_table_catalog(catalog, REPO_ROOT / out)

    n_total = len(catalog.entries)
    n_with_desc = sum(1 for e in catalog.entries if e.description)
    n_cols = sum(len(e.columns) for e in catalog.entries)
    n_col_desc = sum(1 for e in catalog.entries for c in e.columns if c.description)
    n_col_samples = sum(1 for e in catalog.entries for c in e.columns if c.sample_values)
    n_llm_filled = sum(1 for e in catalog.entries for c in e.columns if c.description_source in ("llm", "cache"))
    typer.echo(f"\nWrote table catalog → {REPO_ROOT / out}")
    typer.echo(f"  tables:            {n_total}")
    typer.echo(f"  with description:  {n_with_desc} / {n_total}")
    typer.echo(f"  columns total:     {n_cols}")
    typer.echo(f"  with description:  {n_col_desc} / {n_cols}")
    typer.echo(f"     ↳ llm-filled:   {n_llm_filled}")
    typer.echo(f"  with sample vals:  {n_col_samples}")
    typer.echo()
    typer.echo("Top 8 domain tag counts (each table can have many):")
    counts = sorted(catalog.domain_counts().items(), key=lambda kv: -kv[1])
    for domain, n in counts[:8]:
        typer.echo(f"  {domain:42s} {n}")


@app.command()
def index_catalog(
    catalog: Path = typer.Option(Path("data/artifacts/table_catalog.json")),
    skip_column_values: bool = typer.Option(False, help="Skip column_values collection"),
) -> None:
    """Component 5: embed the table catalog into the vector store."""
    from text2sql.embedding import index_column_values, index_table_catalog
    from text2sql.providers import build_embedding, build_vector_store
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    cat_path = REPO_ROOT / catalog
    if not cat_path.exists():
        typer.echo(f"No catalog at {cat_path}. Run `text2sql build-table-catalog-cmd` first.", err=True)
        sys.exit(1)
    catalog_obj = load_table_catalog(cat_path)
    typer.echo(f"Loaded catalog: {len(catalog_obj.entries)} tables")

    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    typer.echo(f"Embedder: {embedder.dim}-dim    Store: {type(store).__name__}")

    t_stats = index_table_catalog(catalog_obj, embedder, store)
    typer.echo(f"\n[tables] indexed:        {t_stats.tables_indexed}")
    typer.echo(f"          input chars:    {t_stats.total_input_chars:,}")

    if not skip_column_values:
        cv_stats = index_column_values(catalog_obj, embedder, store)
        typer.echo(f"[column_values] indexed: {cv_stats.column_values_indexed}")
        typer.echo(f"                input chars: {cv_stats.total_input_chars:,}")


@app.command()
def search_tables(
    query: str,
    k: int = typer.Option(8),
    domains: str | None = typer.Option(None, help="Comma-separated domain filter"),
    hybrid: bool = typer.Option(True),
) -> None:
    """Search the indexed table catalog."""
    from text2sql.embedding import TableRetriever
    from text2sql.providers import build_embedding, build_vector_store

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    retriever = TableRetriever(embedder, store)
    domain_list = domains.split(",") if domains else None
    hits = retriever.search(query, k=k, domains=domain_list, hybrid=hybrid)
    typer.echo(f"\nQuery: {query!r}")
    if domain_list:
        typer.echo(f"Domain filter: {domain_list}")
    typer.echo(f"Top {len(hits)}:")
    for h in hits:
        domain_str = ", ".join(h.domains[:3]) + ("…" if len(h.domains) > 3 else "")
        typer.echo(f"  {h.score:.3f}  {h.fqn:55s}  [{domain_str}]")


@app.command()
def evaluate(
    out_json: Path = typer.Option(Path("data/eval/runs/last.json")),
    out_md: Path = typer.Option(Path("data/eval/runs/last.md")),
    max_cases: int | None = typer.Option(None, help="Cap number of gold cases"),
    fail_on_regression: float | None = typer.Option(
        None, help="Exit non-zero if execution_accuracy drops below this threshold"
    ),
) -> None:
    """Component 13: run the eval harness over the gold store."""
    from text2sql.evaluation import run_evaluation
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
    _ap, _ad = _active_provider_info(cfg)
    store = GoldStore(_metadata_sa_url(cfg), embedder, catalog=catalog,
                       active_provider=_ap, active_dialect=_ad)
    pipeline = _build_pipeline()

    typer.echo("Running evaluation…")
    report = run_evaluation(pipeline, store, max_cases=max_cases)

    out_json_p = REPO_ROOT / out_json
    out_md_p = REPO_ROOT / out_md
    report.write_json(out_json_p)
    report.write_markdown(out_md_p)

    m = report.metrics
    typer.echo(f"\nCases:                {m.n_cases}")
    typer.echo(f"Schema-linking recall: {m.schema_linking_recall:.0%}")
    typer.echo(f"Join-path exactness:   {m.join_path_exactness:.0%}")
    typer.echo(f"SQL validity:          {m.sql_syntactic_validity:.0%}")
    typer.echo(f"Execution accuracy:    {m.execution_accuracy:.0%}")
    typer.echo(f"Descriptor leakage:    {m.descriptor_leakage_rate:.0%}")
    typer.echo(
        f"Latency p50/p95/p99:   "
        f"{m.latency_total_ms_p50:.0f} / {m.latency_total_ms_p95:.0f} / {m.latency_total_ms_p99:.0f} ms"
    )
    typer.echo(f"\nReport: {out_json_p}")
    typer.echo(f"        {out_md_p}")

    if fail_on_regression is not None and m.execution_accuracy < fail_on_regression:
        typer.echo(
            f"\n❌ Execution accuracy {m.execution_accuracy:.0%} < gate {fail_on_regression:.0%}",
            err=True,
        )
        sys.exit(1)


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    reload: bool = typer.Option(False),
) -> None:
    """Component 11: launch the FastAPI server."""
    import uvicorn
    from text2sql.api import build_app
    from text2sql.config import resolve_artifact_path
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    try:
        pipeline = _build_pipeline()
    except Exception as e:
        # Pipeline construction reads manifest.json + table_catalog.json
        # + table_classification.json. On a fresh repo none of those
        # exist; we still want the API up so Settings → Rebuild is usable.
        typer.echo(f"(pipeline unavailable: {e})", err=True)
        pipeline = None

    # Provider-aware catalog loader. /tables, /tables/{fqn}, /domains,
    # /health all call this per-request so a Settings → primary switch
    # via the runtime overlay is reflected without a server restart.
    # Cached by (path, mtime) so the JSON parse cost stays one-shot per
    # rebuild, not per request.
    _cat_cache: dict[tuple[str, float], object] = {}

    def _load_active_catalog():
        """Re-resolve the catalog for whatever target_db.primary is now.
        Returns None when no artifact has been built yet — the API boots
        in that state so the operator can use Settings → Rebuild from a
        fresh repo without the chicken-and-egg of "catalog needed to
        serve UI; UI needed to build catalog"."""
        live_cfg = load_config()
        cat_path = resolve_artifact_path(live_cfg, "table_catalog.json")
        try:
            mtime = cat_path.stat().st_mtime
        except FileNotFoundError:
            return None
        key = (str(cat_path), mtime)
        cached = _cat_cache.get(key)
        if cached is None:
            _cat_cache.clear()
            try:
                cached = load_table_catalog(cat_path)
            except Exception as e:
                typer.echo(f"(catalog at {cat_path} unreadable: {e})", err=True)
                return None
            _cat_cache[key] = cached
        return cached  # type: ignore[return-value]

    catalog = _load_active_catalog()
    if catalog is None:
        typer.echo(
            "No catalog yet — open the Settings page and run Rebuild "
            "(ingest → classify → graph → catalog → index → gold-seed).",
            err=True,
        )
    gold_store = None
    try:
        embedder = build_embedding(cfg.embedding_provider())
        _ap, _ad = _active_provider_info(cfg)
        gold_store = GoldStore(_metadata_sa_url(cfg), embedder, catalog=catalog,
                                 active_provider=_ap, active_dialect=_ad)
    except Exception as e:
        typer.echo(f"(no gold store available: {e})", err=True)

    agent_runner, conv_store = _build_agent_runner()
    fastapi_app = build_app(
        pipeline=pipeline,
        catalog=catalog,
        gold_store=gold_store,
        agent_runner=agent_runner,
        conv_store=conv_store,
        catalog_loader=_load_active_catalog,
    )
    uvicorn.run(fastapi_app, host=host, port=port, reload=reload, log_level="info")


@app.command()
def chat(
    message: str = typer.Argument(..., help="User message to send to the agent"),
    conversation_id: str = typer.Option(None, help="Continue an existing conversation"),
    stream: bool = typer.Option(False, help="Print events as the agent runs"),
) -> None:
    """Single-shot chat with the agent (multi-turn via --conversation-id)."""
    import uuid as _uuid

    runner, _conv_store = _build_agent_runner()
    if runner is None:
        typer.echo("agent is unavailable (see error above)", err=True)
        sys.exit(1)

    conv_uuid = _uuid.UUID(conversation_id) if conversation_id else None
    if stream:
        for ev in runner.run_stream(conv_uuid, message):
            typer.echo(json.dumps(ev, default=str))
    else:
        result = runner.run(conv_uuid, message)
        typer.echo(f"\n=== Conversation {result.conversation_id} ===")
        for s in result.steps:
            if s.kind == "tool_call":
                typer.echo(f"  → {s.name}({json.dumps(s.arguments, default=str)[:200]})")
            elif s.kind == "tool_result":
                snippet = (
                    json.dumps(s.result, default=str)[:200]
                    if s.result is not None else (s.error or "")[:200]
                )
                typer.echo(f"  ← {s.name}: {snippet}")
        typer.echo(f"\n=== Answer ===\n{result.final_summary}")
        if result.final_sql:
            typer.echo(f"\n=== SQL ===\n{result.final_sql}")
        if result.aborted:
            typer.echo(f"\n(aborted: {result.abort_reason})", err=True)


@app.command()
def ask(
    question: str,
    no_execute: bool = typer.Option(False),
    max_rows: int = typer.Option(20),
    show_sql_only: bool = typer.Option(False),
) -> None:
    """Component 8: end-to-end NL→SQL→rows pipeline."""
    pipeline = _build_pipeline()
    result = pipeline.answer(question, execute=not no_execute, max_rows=max_rows)

    if show_sql_only:
        typer.echo(result.sql)
        return

    typer.echo(f"\n=== Question ===\n{result.nl_question}")
    typer.echo(f"\n=== Routed domains ===\n{result.domains.domains if result.domains else []}")
    chosen = [c for p in (result.resolved.phrases if result.resolved else []) if (c := p.chosen)]
    if chosen:
        typer.echo("\n=== Resolved entities ===")
        for c in chosen:
            extra = f"  [{c.descriptor_type}]" if c.descriptor_type else ""
            typer.echo(f"  {c.value!r:40s} → {c.fqn}.{c.column}{extra}")
    typer.echo(f"\n=== Selected tables ({len(result.retrieved_tables)}) ===")
    for h in result.retrieved_tables[:6]:
        typer.echo(f"  {h.score:.3f}  {h.fqn}")
    if result.join_tree:
        typer.echo(f"\n=== Join tree ({len(result.join_tree.nodes)} tables) ===")
        for n in result.join_tree.nodes:
            typer.echo(f"  {n}")
    typer.echo(f"\n=== Generated SQL ===\n{result.sql}")
    if result.rationale:
        typer.echo(f"\nRationale: {result.rationale}")
    if result.error:
        typer.echo(f"\nERROR: {result.error}", err=True)
    if result.executed:
        typer.echo(f"\n=== Rows ({result.row_count}) ===")
        for r in result.rows[:max_rows]:
            typer.echo(f"  {r}")
        if result.description:
            typer.echo(f"\n=== Description ===\n{result.description}")
        if result.viz:
            v = result.viz
            xy = f"  x={v.x}  y={v.y}" + (f"  color={v.color}" if v.color else "")
            typer.echo(f"\n=== Chart ===\n  kind={v.kind}  title={v.title!r}\n{xy}")
            if result.viz_vega_lite:
                typer.echo("  (vega-lite spec available on result.viz_vega_lite)")
    typer.echo(f"\n=== Timings ===")
    for k, v in sorted(result.timings_ms.items(), key=lambda kv: -kv[1]):
        typer.echo(f"  {k:20s}  {v:8.1f} ms")


def _build_pipeline():
    """Wire all components from config."""
    import os
    from text2sql.classification import QueryDomainClassifier, load_domain_catalog
    from text2sql.embedding import TableRetriever
    from text2sql.entity_resolution import EntityResolver, build_value_index
    from text2sql.gold import GoldStore
    from text2sql.graph import build_graph, parse_fks
    from text2sql.pipeline import Text2SqlPipeline
    from text2sql.providers import build_embedding, build_llm, build_sql_engine, build_vector_store
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = IngestionManifest.from_json((ic.cache_dir / "manifest.json").read_text(encoding="utf-8"))

    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
    edges = []
    for art in manifest.artifacts:
        edges.extend(parse_fks(art.foreign_keys_sql_path))
    from text2sql.classification import read_table_mapping
    classifications = read_table_mapping(REPO_ROOT / "data/artifacts/table_classification.json").classifications
    graph = build_graph(edges, classifications=classifications)

    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    sql_engine = build_sql_engine(cfg.target_db_provider())

    # One LLM per task slot, honoring configs/default.yaml's task_routing.
    # When two slots resolve to the same provider entry, build_llm is
    # idempotent enough that the duplicate-instantiation cost is just two
    # cheap API client objects — no extra connections.
    sql_llm        = build_llm(cfg.llm_for_task("sql_generation"))
    repair_llm     = build_llm(cfg.llm_for_task("repair_loop"))
    viz_llm        = build_llm(cfg.llm_for_task("visualization"))
    description_llm = build_llm(cfg.llm_for_task("description"))
    classifier_llm = build_llm(cfg.llm_for_task("classifier_fallback"))

    domain_catalog = load_domain_catalog(manifest)
    domain_classifier = QueryDomainClassifier(
        classifier_llm, domain_catalog,
        cache_path=REPO_ROOT / "data/artifacts/.query_classification_cache.json",
    )
    retriever = TableRetriever(embedder, store)
    value_index = build_value_index(catalog)
    entity_resolver = EntityResolver(
        value_index, embedder=embedder, store=store, llm=classifier_llm,
    )

    gold_store = None
    try:
        sa_url = _metadata_sa_url(cfg)
        _ap, _ad = _active_provider_info(cfg)
        gold_store = GoldStore(sa_url, embedder, catalog=catalog,
                                 active_provider=_ap, active_dialect=_ad)
    except Exception:
        pass

    return Text2SqlPipeline(
        catalog=catalog,
        graph=graph,
        domain_classifier=domain_classifier,
        retriever=retriever,
        entity_resolver=entity_resolver,
        gold_store=gold_store,
        sql_engine=sql_engine,
        llm=sql_llm,
        repair_llm=repair_llm,
        viz_llm=viz_llm,
        description_llm=description_llm,
    )


def _build_agent_runner():
    """Wire the agent stack: ToolContext, ConversationStore, AgentRunner.

    Returns (runner, conv_store) on success, or (None, None) if the metadata DB
    is unreachable (the agent loop needs Postgres for conversation history).
    """
    from text2sql.agent import AgentRunner, ConversationStore, ToolContext
    from text2sql.classification import (
        QueryDomainClassifier,
        load_domain_catalog,
        read_table_mapping,
    )
    from text2sql.embedding import TableRetriever
    from text2sql.entity_resolution import EntityResolver, build_value_index
    from text2sql.gold import GoldStore
    from text2sql.graph import build_graph, parse_fks
    from text2sql.providers import (
        build_embedding,
        build_llm,
        build_sql_engine,
        build_vector_store,
    )
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = IngestionManifest.from_json((ic.cache_dir / "manifest.json").read_text(encoding="utf-8"))

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
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))

    # Filter the catalog down to what actually exists in the live DB —
    # mirrors what Text2SqlPipeline does at startup. Without this the agent's
    # inspect_table happily returns metadata for tables that the populated
    # container never had (e.g. StudentDemographic in Northridge).
    try:
        from text2sql.pipeline.orchestrator import _filter_catalog_to_live_db
        catalog = _filter_catalog_to_live_db(catalog, sql_engine)
    except Exception as e:
        typer.echo(f"(live-db catalog filter failed: {e}; using full catalog)", err=True)
    domain_classifier = QueryDomainClassifier(
        llm, load_domain_catalog(manifest), cache_path=None,
    )
    retriever = TableRetriever(embedder, store)
    value_index = build_value_index(catalog)
    entity_resolver = EntityResolver(value_index, embedder=embedder, store=store, llm=llm)

    try:
        sa_url = _metadata_sa_url(cfg)
        _ap, _ad = _active_provider_info(cfg)
        gold_store = GoldStore(sa_url, embedder, catalog=catalog,
                                 active_provider=_ap, active_dialect=_ad)
        conv_store = ConversationStore(sa_url)
        conv_store.ensure_schema()
    except Exception as e:
        typer.echo(f"(agent unavailable: metadata DB error: {e})", err=True)
        return None, None

    from text2sql.pipeline.viz import VizDescriber
    viz_llm = build_llm(cfg.llm_for_task("visualization"))
    viz_describer = VizDescriber(llm=viz_llm)

    tool_ctx = ToolContext(
        catalog=catalog,
        graph=graph,
        retriever=retriever,
        entity_resolver=entity_resolver,
        sql_engine=sql_engine,
        domain_classifier=domain_classifier,
        gold_store=gold_store,
        viz_describer=viz_describer,
    )

    llm_spec = cfg.llm_for_task("sql_generation")
    runner = AgentRunner(conv_store=conv_store, tool_ctx=tool_ctx, llm_spec=llm_spec)
    return runner, conv_store


@app.command()
def gold_init() -> None:
    """Component 7: create the gold_sql table in the metadata DB."""
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    sa_url = _metadata_sa_url(cfg)
    _ap, _ad = _active_provider_info(cfg)
    store = GoldStore(sa_url, embedder,
                       active_provider=_ap, active_dialect=_ad)
    store.ensure_schema()
    typer.echo(f"gold_sql schema ensured ({sa_url}).")


@app.command()
def gold_seed(
    yaml_path: Path = typer.Option(Path("data/eval/gold_queries_bootstrap.yaml")),
    approve: bool = typer.Option(True, help="Mark seeded rows as approved"),
    author: str = typer.Option("bootstrap"),
    skip_exec_check: bool = typer.Option(False, help="Skip executing each seed against live DB"),
    drop_existing: bool = typer.Option(False, help="Drop & recreate the gold table first"),
) -> None:
    """Seed the gold store from a YAML file of NL/SQL pairs.

    Each pair is run against the live target DB first; only those that execute
    cleanly are inserted. This stops bad few-shots from poisoning Component 8.
    """
    import yaml
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding, build_sql_engine
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    catalog_path = REPO_ROOT / "data/artifacts/table_catalog.json"
    catalog = load_table_catalog(catalog_path) if catalog_path.exists() else None
    sa_url = _metadata_sa_url(cfg)
    _ap, _ad = _active_provider_info(cfg)
    store = GoldStore(sa_url, embedder, catalog=catalog,
                       active_provider=_ap, active_dialect=_ad)
    if drop_existing:
        store.drop_schema()
    store.ensure_schema()

    sql_engine = None
    if not skip_exec_check:
        try:
            sql_engine = build_sql_engine(cfg.target_db_provider())
        except Exception as e:
            typer.echo(f"(no target DB — skipping exec checks): {e}")

    pairs = yaml.safe_load((REPO_ROOT / yaml_path).read_text(encoding="utf-8"))["queries"]
    typer.echo(f"Seeding {len(pairs)} pairs from {yaml_path}…")
    n_created = 0
    n_skipped = 0
    for p in pairs:
        nl = p["nl"]
        sql = p["sql"].strip()
        # Validate against live DB first
        if sql_engine is not None:
            try:
                sql_engine.execute(sql, limit=1)
            except Exception as e:
                typer.echo(f"  ✗ skip {nl[:60]!r} — exec failed: {str(e)[:120]}")
                n_skipped += 1
                continue
        rec = store.create(
            nl_question=nl,
            sql_text=sql,
            tables_used=list(p.get("tables") or []),
            author=author,
            approval_status="approved" if approve else "pending",
        )
        if sql_engine is not None:
            store.mark_exec_passed(rec.id, ok=True)
        n_created += 1
        typer.echo(f"  + {rec.nl_question[:60]!r}")
    typer.echo(
        f"\nSeeded: {n_created}, skipped: {n_skipped}.  "
        f"Total: {store.count()}  (approved: {store.count(approval_status='approved')})"
    )


@app.command()
def gold_search(
    query: str,
    domains: str | None = typer.Option(None, help="Comma-separated domain hints"),
    k: int = typer.Option(3),
) -> None:
    """Search the gold store for top-K few-shot examples for a NL query."""
    from text2sql.gold import GoldStore
    from text2sql.providers import build_embedding
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    catalog_path = REPO_ROOT / "data/artifacts/table_catalog.json"
    catalog = load_table_catalog(catalog_path) if catalog_path.exists() else None
    sa_url = _metadata_sa_url(cfg)
    _ap, _ad = _active_provider_info(cfg)
    store = GoldStore(sa_url, embedder, catalog=catalog,
                       active_provider=_ap, active_dialect=_ad)
    domain_list = domains.split(",") if domains else None
    hits = store.retrieve_top_k(query, domains=domain_list, k=k)
    typer.echo(f"\nQuery: {query}")
    if domain_list:
        typer.echo(f"Domain hints: {domain_list}")
    typer.echo(f"\nTop {len(hits)}:")
    for h in hits:
        typer.echo(f"  {h.score:.3f}  {h.record.nl_question[:80]!r}")
        typer.echo(f"          tables: {h.record.tables_used}")


def _metadata_sa_url(cfg) -> str:
    """Thin alias kept for tests and existing call-sites. Real logic lives
    in `text2sql.config.metadata_sa_url` so admin endpoints can import it
    without pulling Typer + the rest of the CLI module."""
    from text2sql.config import metadata_sa_url
    return metadata_sa_url(cfg)


def _active_provider_info(cfg) -> tuple[str, str]:
    """Return (active_provider_name, active_dialect) for tagging /
    scoping per-provider artifacts. Resolves provider kind defensively
    — a missing entry yields empty strings rather than KeyError."""
    name = cfg.active_target_provider_name() or ""
    dialect = ""
    try:
        dialect = cfg.target_db_provider().kind
    except Exception:
        pass
    return name, dialect


@app.command()
def resolve_entities(
    query: str,
    domains: str | None = typer.Option(None, help="Comma-separated domain scope"),
) -> None:
    """Component 6: extract NL phrases and resolve to (table, column, value) candidates."""
    from text2sql.entity_resolution import EntityResolver, build_value_index
    from text2sql.providers import build_embedding, build_llm, build_vector_store
    from text2sql.table_catalog import load_table_catalog

    cfg = load_config()
    catalog = load_table_catalog(REPO_ROOT / "data/artifacts/table_catalog.json")
    index = build_value_index(catalog)
    typer.echo(f"Value index: {len(index)} (table, column, value) records")

    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    llm = None
    try:
        llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    except Exception:
        pass

    resolver = EntityResolver(index, embedder=embedder, store=store, llm=llm)
    domain_list = domains.split(",") if domains else None
    result = resolver.resolve(query, domains=domain_list)

    typer.echo(f"\nQUERY: {query}")
    if domain_list:
        typer.echo(f"Domain scope: {domain_list}")
    if not result.phrases:
        typer.echo("(no candidate phrases extracted)")
        return
    for ph in result.phrases:
        typer.echo(f"\nPhrase: {ph.phrase!r}")
        if ph.chosen:
            c = ph.chosen
            typer.echo(f"  ✓ {c.tier:6s} {c.score:.2f}  {c.fqn}.{c.column} = {c.value!r}")
        else:
            typer.echo("  (unresolved)")
        for cand in ph.candidates[:3]:
            mark = "    "
            typer.echo(f"  {mark}{cand.tier:6s} {cand.score:.2f}  {cand.fqn}.{cand.column} = {cand.value!r}")


@app.command()
def classify_query(query: str) -> None:
    """Component 2b: classify an NL query into ranked domains (runtime path)."""
    from text2sql.classification import QueryDomainClassifier, load_domain_catalog
    from text2sql.providers import build_llm

    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest_path = ic.cache_dir / "manifest.json"
    if not manifest_path.exists():
        typer.echo("No ingest manifest. Run `text2sql ingest` first.", err=True)
        sys.exit(1)
    manifest = IngestionManifest.from_json(manifest_path.read_text(encoding="utf-8"))
    catalog = load_domain_catalog(manifest)

    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    qc = QueryDomainClassifier(
        llm, catalog,
        cache_path=REPO_ROOT / "data/artifacts/.query_classification_cache.json",
    )
    out = qc.classify(query)
    typer.echo(f"\nQUERY: {out.query}")
    typer.echo(f"SOURCE: {out.source}")
    typer.echo(f"REASONING: {out.reasoning}")
    typer.echo("\nDOMAINS (ranked):")
    labels = ["primary  ", "secondary", "tertiary "]
    for i, d in enumerate(out.domains):
        label = labels[i] if i < len(labels) else "extra    "
        typer.echo(f"  {label}  {d}")


if __name__ == "__main__":
    app()
