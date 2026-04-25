"""LLM-callable tool registry.

Each tool is a thin wrapper over an existing component. The wrapper preserves
the component's intent and behavior; it only translates from JSON-schema input
back to the typed call.

Public types:
  ToolDefinition  — name + JSON schema + handler.
  ToolContext     — handles to the live components (catalog, retriever, …).
                    One ToolContext per agent loop; shared across tool calls.
  ToolResult      — what we return to the LLM after a tool runs (success or
                    error, both go back as JSON the LLM can keep reasoning over).

Tools wired:
  classify_domains       (Component 2b — QueryDomainClassifier)
  search_tables          (Component 5  — TableRetriever)
  inspect_table          (TableCatalog + live engine.list_columns)
  resolve_entity         (Component 6  — 4-tier EntityResolver)
  find_join_path         (Component 3  — Steiner solver over FK graph)
  find_similar_queries   (Component 7  — GoldStore few-shot retrieval)
  run_sql                (Component 9  — validate + repair + execute)
  final_answer           (terminate the agent loop)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger(__name__)


# ── Generic types ────────────────────────────────────────────────────────────


@dataclass
class ToolContext:
    """Live handles the agent loop hands to each tool. Built once per agent run."""
    catalog: Any                              # text2sql.table_catalog.TableCatalog
    graph: Any | None = None                  # text2sql.graph.FKGraph
    retriever: Any | None = None              # text2sql.embedding.TableRetriever
    entity_resolver: Any | None = None        # text2sql.entity_resolution.EntityResolver
    sql_engine: Any | None = None             # text2sql.providers.base.SqlEngine
    domain_classifier: Any | None = None      # text2sql.classification.QueryDomainClassifier
    gold_store: Any | None = None             # text2sql.gold.GoldStore
    repair_loop: Any | None = None            # text2sql.pipeline.repair.RepairLoop
    viz_describer: Any | None = None          # text2sql.pipeline.viz.VizDescriber


@dataclass
class ToolResult:
    """A single tool's return value, JSON-serializable for replay to the LLM."""
    ok: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    is_terminal: bool = False                 # True for final_answer

    def to_json(self) -> str:
        if self.ok:
            return json.dumps({"ok": True, **self.data}, default=str)
        return json.dumps({"ok": False, "error": self.error or "unknown error"})


@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: dict[str, Any]                # JSON-schema for arguments
    handler: Callable[[dict[str, Any], ToolContext], ToolResult]

    def to_openai_tool(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


# ── Registry ─────────────────────────────────────────────────────────────────


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}

    def register(self, td: ToolDefinition) -> None:
        if td.name in self._tools:
            raise ValueError(f"tool {td.name!r} already registered")
        self._tools[td.name] = td

    def get(self, name: str) -> ToolDefinition | None:
        return self._tools.get(name)

    def all(self) -> list[ToolDefinition]:
        return list(self._tools.values())

    def to_openai_tools(self) -> list[dict[str, Any]]:
        return [t.to_openai_tool() for t in self._tools.values()]

    def execute(
        self,
        name: str,
        arguments: dict[str, Any] | str,
        ctx: ToolContext,
    ) -> ToolResult:
        td = self._tools.get(name)
        if td is None:
            return ToolResult(ok=False, error=f"unknown tool: {name}")
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments) if arguments.strip() else {}
            except Exception as e:
                return ToolResult(ok=False, error=f"invalid args JSON: {e}")
        try:
            return td.handler(arguments, ctx)
        except Exception as e:
            log.exception("tool %s crashed", name)
            return ToolResult(ok=False, error=f"{type(e).__name__}: {e}")


# ── Tool 1: classify_domains  (wraps Component 2b) ───────────────────────────


CLASSIFY_DOMAINS_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "question": {
            "type": "string",
            "description": "The user's natural-language question.",
        },
    },
    "required": ["question"],
}


def _classify_domains_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.domain_classifier is None:
        return ToolResult(ok=False, error="domain_classifier not configured")
    q = (args.get("question") or "").strip()
    if not q:
        return ToolResult(ok=False, error="question is empty")
    out = ctx.domain_classifier.classify(q)
    return ToolResult(
        ok=True,
        data={
            "domains": list(out.domains),
            "primary": out.primary,
            "secondary": out.secondary,
            "tertiary": out.tertiary,
            "reasoning": out.reasoning,
            "source": out.source,
        },
    )


CLASSIFY_DOMAINS = ToolDefinition(
    name="classify_domains",
    description=(
        "Map a natural-language question to up to 3 ranked Ed-Fi domains "
        "(e.g. Enrollment, StudentAttendance). Use this FIRST before "
        "search_tables so you can scope retrieval; pass the returned domains "
        "to search_tables."
    ),
    parameters=CLASSIFY_DOMAINS_SCHEMA,
    handler=_classify_domains_handler,
)


# ── Tool 2: search_tables  (wraps Component 5) ───────────────────────────────


SEARCH_TABLES_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "query": {
            "type": "string",
            "description": "Phrase describing what tables you need.",
        },
        "k": {
            "type": "integer",
            "description": "Number of results (1-20).",
            "minimum": 1, "maximum": 20,
        },
        "domains": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Optional domain filter. Tables tagged with any of these "
                "domains are eligible; others are excluded."
            ),
        },
    },
    "required": ["query", "k", "domains"],
}


def _search_tables_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.retriever is None:
        return ToolResult(ok=False, error="retriever not configured")
    query = (args.get("query") or "").strip()
    k = int(args.get("k") or 8)
    domains = args.get("domains") or None
    if not query:
        return ToolResult(ok=False, error="query is empty")
    # Pull k+10 raw hits, then drop any that aren't in the live-DB-filtered
    # catalog (the embeddings index is built from the full metadata catalog,
    # so it still surfaces tables that were dropped by the live-DB filter
    # like StudentDemographic).
    raw_hits = ctx.retriever.search(query, k=k + 10, domains=domains, hybrid=True)
    by_fqn = ctx.catalog.by_fqn() if ctx.catalog is not None else {}
    hits = [h for h in raw_hits if not by_fqn or h.fqn in by_fqn][:k]

    # Mirror old pipeline stages [3]+[4]: run Steiner over the top-k hits
    # (plus inheritance bases / Descriptor bridge) so the agent receives a
    # CONNECTED join tree even for tables it didn't think to mention. This
    # is how the old pipeline surfaces StudentEducationOrganizationAssociation
    # for an "ethnicity" question — without it the agent only sees the top
    # text-similarity hits, which often miss the bridge table.
    join_tree_data: dict[str, Any] | None = None
    if ctx.graph is not None and hits:
        try:
            from text2sql.graph import steiner
            from text2sql.pipeline.orchestrator import (
                _dedupe_preserve,
                _inheritance_chain_extras,
            )
            targets: list[str] = [h.fqn for h in hits]
            if ctx.catalog is not None:
                by_fqn = ctx.catalog.by_fqn()
                if any((by_fqn.get(t) and by_fqn[t].is_descriptor) for t in targets):
                    targets.append("edfi.Descriptor")
                try:
                    targets.extend(_inheritance_chain_extras(ctx.graph, ctx.catalog, targets))
                except Exception as e:
                    log.debug("inheritance walk failed in search_tables: %s", e)
            targets = _dedupe_preserve(targets)
            if len(targets) >= 2:
                tree = steiner(ctx.graph, targets)
                dialect = ctx.sql_engine.dialect if ctx.sql_engine else "mssql"
                join_tree_data = {
                    "expanded_targets": targets,
                    "tree_nodes": list(tree.nodes),
                    "edge_count": len(tree.edges),
                    "total_weight": tree.total_weight,
                    "join_clauses": tree.to_join_clauses(dialect=dialect),
                }
        except Exception as e:
            log.debug("auto-Steiner in search_tables failed: %s", e)

    return ToolResult(
        ok=True,
        data={
            "query": query,
            "domain_filter": list(domains) if domains else None,
            "hits": [
                {
                    "fqn": h.fqn,
                    "score": round(h.score, 4),
                    "domains": list(h.domains)[:5],
                    "is_descriptor": h.is_descriptor,
                }
                for h in hits
            ],
            "join_tree": join_tree_data,
        },
    )


SEARCH_TABLES = ToolDefinition(
    name="search_tables",
    description=(
        "Hybrid (vector + BM25) search over the table catalog, scoped by "
        "domains. Returns top-k hits AND a pre-computed Steiner join tree "
        "connecting them through the FK graph (including inheritance bases "
        "like EducationOrganization and the Descriptor bridge). Use the "
        "returned `join_tree.tree_nodes` as the table set for your SELECT "
        "and `join_tree.join_clauses` as the JOINs — these are the same "
        "tables and joins the canonical pipeline would build. "
        "Set k=8 or higher for non-trivial questions; the bridge table "
        "you need may not be in the top 5."
    ),
    parameters=SEARCH_TABLES_SCHEMA,
    handler=_search_tables_handler,
)


# ── Tool 3: inspect_table  (wraps catalog + live engine.list_columns) ────────


INSPECT_TABLE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "fqn": {
            "type": "string",
            "description": "Fully qualified table name (e.g. 'edfi.Student').",
        },
    },
    "required": ["fqn"],
}


def _inspect_table_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.catalog is None:
        return ToolResult(ok=False, error="catalog not configured")
    fqn = (args.get("fqn") or "").strip()
    entry = ctx.catalog.by_fqn().get(fqn)
    if not entry:
        # Case-insensitive retry — LLM sometimes lowercases
        for f, e in ctx.catalog.by_fqn().items():
            if f.lower() == fqn.lower():
                entry = e
                fqn = f
                break
    if not entry:
        return ToolResult(ok=False, error=f"table not in catalog: {fqn}")
    # Filter columns to those that exist in the live DB. If the live DB has
    # no such table at all, FAIL HARD instead of silently falling back to
    # catalog metadata — the agent must know that tables present in the
    # Ed-Fi metadata schema may not be present in this populated DB.
    cols = list(entry.columns)
    if ctx.sql_engine is not None:
        try:
            live = {
                c[0].lower()
                for c in ctx.sql_engine.list_columns(entry.schema, entry.table)
            }
            if not live:
                return ToolResult(
                    ok=False,
                    error=(
                        f"table {fqn!r} exists in the Ed-Fi metadata schema but "
                        f"NOT in the live database. Pick a different table — "
                        f"the value you need lives somewhere else in this DB."
                    ),
                )
            filtered = [c for c in cols if c.name.lower() in live]
            if filtered:
                cols = filtered
        except Exception as e:
            log.warning("inspect_table: live column probe failed for %s: %s", fqn, e)
    return ToolResult(
        ok=True,
        data={
            "fqn": entry.fqn,
            "description": entry.description,
            "domains": list(entry.domains),
            "is_descriptor": entry.is_descriptor,
            "is_association": entry.is_association,
            "primary_key": list(entry.primary_key),
            "parent_neighbors": list(entry.parent_neighbors)[:15],
            "child_neighbors": list(entry.child_neighbors)[:15],
            "row_count": entry.row_count,
            "columns": [
                {
                    "name": c.name,
                    "type": c.data_type,
                    "nullable": c.nullable,
                    "is_pk": c.is_identifying,
                    "description": c.description,
                    "samples": (c.sample_values or [])[:6],
                }
                for c in cols[:60]
            ],
        },
    )


INSPECT_TABLE = ToolDefinition(
    name="inspect_table",
    description=(
        "Get the full schema for one table: columns, types, descriptions, "
        "sample values, primary key, FK neighbors. Use this to verify a "
        "candidate table from search_tables before composing SQL."
    ),
    parameters=INSPECT_TABLE_SCHEMA,
    handler=_inspect_table_handler,
)


# ── Tool 4: resolve_entity  (wraps Component 6) ──────────────────────────────


RESOLVE_ENTITY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "phrase": {
            "type": "string",
            "description": (
                "A noun-phrase from the user's question (e.g. 'Hispanic', "
                "'Pre-K', 'Algebra I')."
            ),
        },
        "domains": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional domain scope to narrow the value search.",
        },
    },
    "required": ["phrase", "domains"],
}


def _resolve_entity_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.entity_resolver is None:
        return ToolResult(ok=False, error="entity_resolver not configured")
    phrase = (args.get("phrase") or "").strip()
    domains = args.get("domains") or None
    if not phrase:
        return ToolResult(ok=False, error="phrase is empty")
    result = ctx.entity_resolver.resolve_phrase(phrase, domains=domains)
    chosen = None
    if result.chosen is not None:
        c = result.chosen
        chosen = {
            "fqn": c.fqn,
            "column": c.column,
            "value": c.value,
            "score": round(c.score, 3),
            "tier": c.tier,
            "descriptor_type": c.descriptor_type or None,
            "child_fqn": c.child_fqn or None,
            "descriptor_id": c.descriptor_id,
        }
    return ToolResult(
        ok=True,
        data={
            "phrase": phrase,
            "chosen": chosen,
            "candidates": [
                {
                    "fqn": cand.fqn, "column": cand.column, "value": cand.value,
                    "score": round(cand.score, 3), "tier": cand.tier,
                    "descriptor_type": cand.descriptor_type or None,
                }
                for cand in result.candidates[:5]
            ],
        },
    )


RESOLVE_ENTITY = ToolDefinition(
    name="resolve_entity",
    description=(
        "Resolve a noun-phrase to a database value. Returns the table, column, "
        "and exact value; for descriptor codes also returns the descriptor "
        "type and the bridge child-descriptor table needed for the join chain. "
        "Call this for any value the user mentions (Hispanic, Pre-K, etc.) "
        "before writing the WHERE clause."
    ),
    parameters=RESOLVE_ENTITY_SCHEMA,
    handler=_resolve_entity_handler,
)


# ── Tool 5: find_join_path  (wraps Component 3 Steiner) ──────────────────────


FIND_JOIN_PATH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "tables": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 2,
            "description": "Fully qualified table names to connect (≥ 2).",
        },
        "dialect": {
            "type": "string",
            "enum": ["mssql", "postgresql"],
            "description": "Target dialect for rendered JOIN clauses.",
        },
    },
    "required": ["tables", "dialect"],
}


def _find_join_path_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.graph is None:
        return ToolResult(ok=False, error="graph not configured")
    from text2sql.graph import steiner
    from text2sql.pipeline.orchestrator import (
        _dedupe_preserve,
        _inheritance_chain_extras,
    )

    targets = list(args.get("tables") or [])
    if len(targets) < 2:
        return ToolResult(ok=False, error="need at least 2 target tables")
    dialect = args.get("dialect") or (
        ctx.sql_engine.dialect if ctx.sql_engine is not None else "mssql"
    )
    # Mirror the old pipeline's pre-Steiner expansion exactly:
    #   1. If any descriptor table is in the targets, also include
    #      edfi.Descriptor so the bridge from <child>DescriptorId →
    #      Descriptor.DescriptorId is part of the tree.
    #   2. Walk Ed-Fi-style PK-shared FKs to surface inheritance bases
    #      (School → EducationOrganization, LEA → EducationOrganization)
    #      so NameOfInstitution is reachable from the join tree.
    expanded = list(targets)
    if ctx.catalog is not None:
        by_fqn = ctx.catalog.by_fqn()
        for fqn in targets:
            entry = by_fqn.get(fqn)
            if entry is not None and entry.is_descriptor:
                expanded.append("edfi.Descriptor")
                break
    if ctx.catalog is not None:
        try:
            expanded.extend(
                _inheritance_chain_extras(ctx.graph, ctx.catalog, expanded)
            )
        except Exception as e:
            log.debug("inheritance walk failed: %s", e)
    expanded = _dedupe_preserve(expanded)
    tree = steiner(ctx.graph, expanded)
    return ToolResult(
        ok=True,
        data={
            "targets": targets,
            "expanded_targets": expanded,
            "tree_nodes": list(tree.nodes),
            "edge_count": len(tree.edges),
            "total_weight": tree.total_weight,
            "join_clauses": tree.to_join_clauses(dialect=dialect),
        },
    )


FIND_JOIN_PATH = ToolDefinition(
    name="find_join_path",
    description=(
        "Compute the minimum-weight join tree connecting two or more tables "
        "using the precomputed FK graph. Returns ready-to-paste JOIN clauses "
        "with composite-FK column pairs preserved. Use this before writing "
        "any SQL that touches more than one table."
    ),
    parameters=FIND_JOIN_PATH_SCHEMA,
    handler=_find_join_path_handler,
)


# ── Tool 6: find_similar_queries  (wraps Component 7 gold store) ─────────────


FIND_SIMILAR_QUERIES_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "question": {
            "type": "string",
            "description": "The user's NL question (or a paraphrase).",
        },
        "k": {
            "type": "integer", "minimum": 1, "maximum": 10,
            "description": "Number of similar gold pairs to return.",
        },
        "domains": {
            "type": "array", "items": {"type": "string"},
            "description": "Optional domain hints (boost overlap score).",
        },
    },
    "required": ["question", "k", "domains"],
}


def _find_similar_queries_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.gold_store is None:
        return ToolResult(ok=False, error="gold store not configured")
    q = (args.get("question") or "").strip()
    k = int(args.get("k") or 3)
    domains = args.get("domains") or None
    if not q:
        return ToolResult(ok=False, error="question is empty")
    hits = ctx.gold_store.retrieve_top_k(q, domains=domains, k=k)
    return ToolResult(
        ok=True,
        data={
            "question": q,
            "examples": [
                {
                    "score": round(h.score, 3),
                    "nl": h.record.nl_question,
                    "sql": h.record.sql_text,
                    "tables_used": h.record.tables_used,
                    "exec_check_passed": h.record.exec_check_passed,
                }
                for h in hits
            ],
        },
    )


FIND_SIMILAR_QUERIES = ToolDefinition(
    name="find_similar_queries",
    description=(
        "Retrieve the top-k most semantically similar approved NL/SQL pairs "
        "from the gold store. Use these as few-shots when composing SQL — "
        "they show the exact dialect, casing, and join shape that works."
    ),
    parameters=FIND_SIMILAR_QUERIES_SCHEMA,
    handler=_find_similar_queries_handler,
)


# ── Tool 7: run_sql  (wraps Component 9 validate + repair + execute) ─────────


RUN_SQL_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "sql": {
            "type": "string",
            "description": (
                "A complete SELECT statement in the target dialect. Will be "
                "validated (parse + EXPLAIN) before executing. If validation "
                "fails the engine error is returned; if execution succeeds "
                "the rows are returned (capped at max_rows)."
            ),
        },
        "max_rows": {
            "type": "integer",
            "minimum": 1, "maximum": 1000,
            "description": "Row cap for the executor (default 50).",
        },
    },
    "required": ["sql", "max_rows"],
}


def _run_sql_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    if ctx.sql_engine is None:
        return ToolResult(ok=False, error="sql_engine not configured")
    sql = (args.get("sql") or "").strip()
    max_rows = int(args.get("max_rows") or 50)
    if not sql:
        return ToolResult(ok=False, error="sql is empty")
    # Validate first — same gate Component 9 uses
    from text2sql.pipeline.repair import validate_sql
    err = validate_sql(sql, ctx.sql_engine)
    if err:
        return ToolResult(ok=False, error=err)
    try:
        rows = ctx.sql_engine.execute(sql, limit=max_rows)
    except Exception as e:
        return ToolResult(ok=False, error=f"execute: {e}")
    return ToolResult(
        ok=True,
        data={
            "sql": sql,
            "row_count": len(rows),
            "rows": rows,
        },
    )


RUN_SQL = ToolDefinition(
    name="run_sql",
    description=(
        "Validate a SELECT statement and execute it against the target "
        "database. Returns rows on success or a precise error string on "
        "failure (parse error, missing column, EXPLAIN failure, etc.). "
        "When you get an error, fix the SQL and call run_sql again — DO NOT "
        "give up after the first failure."
    ),
    parameters=RUN_SQL_SCHEMA,
    handler=_run_sql_handler,
)


# ── Tool 8: final_answer  (terminate the agent loop) ─────────────────────────


FINAL_ANSWER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {
            "type": "string",
            "description": "Plain-English answer for the user (1-2 sentences).",
        },
        "sql": {
            "type": ["string", "null"],
            "description": "Final SQL that produced the answer (null if no SQL was needed).",
        },
        "row_count": {
            "type": ["integer", "null"],
            "description": "Number of rows returned by the SQL, if applicable.",
        },
    },
    "required": ["summary", "sql", "row_count"],
}


def _final_answer_handler(args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    summary = (args.get("summary") or "").strip()
    if not summary:
        return ToolResult(ok=False, error="final_answer requires a summary")
    return ToolResult(
        ok=True,
        is_terminal=True,
        data={
            "summary": summary,
            "sql": args.get("sql"),
            "row_count": args.get("row_count"),
        },
    )


FINAL_ANSWER = ToolDefinition(
    name="final_answer",
    description=(
        "Terminate the agent and return the final answer to the user. "
        "Call this exactly once when you have completed the task. "
        "If the task can't be completed, call this with a summary explaining "
        "what blocked you."
    ),
    parameters=FINAL_ANSWER_SCHEMA,
    handler=_final_answer_handler,
)


# ── Default registry ─────────────────────────────────────────────────────────


def default_registry() -> ToolRegistry:
    """Registry pre-populated with every tool wired so far."""
    r = ToolRegistry()
    r.register(CLASSIFY_DOMAINS)
    r.register(SEARCH_TABLES)
    r.register(INSPECT_TABLE)
    r.register(RESOLVE_ENTITY)
    r.register(FIND_JOIN_PATH)
    r.register(FIND_SIMILAR_QUERIES)
    r.register(RUN_SQL)
    r.register(FINAL_ANSWER)
    return r
