"""Text2SqlPipeline: NL question → SQL → (optionally) executed rows.

Constructed once per process; every component dependency is injected so
tests can stub anything without monkey-patching.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from text2sql.classification import QueryDomainClassifier
from text2sql.classification.query_classifier import QueryClassification
from text2sql.embedding.retriever import TableHit, TableRetriever
from text2sql.entity_resolution.resolver import EntityResolver, ResolutionResult
from text2sql.gold.store import GoldHit, GoldStore
from text2sql.graph import FKGraph, SteinerTree, steiner
from text2sql.pipeline.context import ContextBuilder, PromptContext
from text2sql.pipeline.repair import RepairAttempt, RepairLoop, RepairResult
from text2sql.pipeline.viz import VizDescriber, VizResult, VizSpec
from text2sql.providers.base import LLMProvider, SqlEngine
from text2sql.table_catalog import TableCatalog

log = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    nl_question: str
    sql: str
    rationale: str = ""
    rows: list[dict] = field(default_factory=list)
    row_count: int | None = None
    executed: bool = False
    validated: bool = False
    error: str | None = None

    # Component 10 outputs (run in parallel after execute)
    viz: VizSpec | None = None
    viz_vega_lite: dict | None = None
    description: str = ""

    # Debug artifacts — every intermediate step
    domains: QueryClassification | None = None
    retrieved_tables: list[TableHit] = field(default_factory=list)
    join_tree: SteinerTree | None = None
    resolved: ResolutionResult | None = None
    few_shots: list[GoldHit] = field(default_factory=list)
    prompt: PromptContext | None = None
    repair_attempts: list[RepairAttempt] = field(default_factory=list)
    timings_ms: dict[str, float] = field(default_factory=dict)


class Text2SqlPipeline:
    def __init__(
        self,
        *,
        catalog: TableCatalog,
        graph: FKGraph,
        domain_classifier: QueryDomainClassifier,
        retriever: TableRetriever,
        entity_resolver: EntityResolver,
        gold_store: GoldStore | None,
        sql_engine: SqlEngine,
        llm: LLMProvider,
        dialect: str | None = None,
        max_repair_attempts: int = 3,
    ) -> None:
        self.catalog = catalog
        self.graph = graph
        self.domain_classifier = domain_classifier
        self.retriever = retriever
        self.entity_resolver = entity_resolver
        self.gold_store = gold_store
        self.sql_engine = sql_engine
        self.llm = llm
        self.dialect = dialect or sql_engine.dialect
        # Filter the catalog down to tables/columns that actually exist in the
        # live DB. Catalog metadata may reflect a newer DS version than the
        # populated container — the LLM must only see what's present.
        try:
            self.catalog = _filter_catalog_to_live_db(catalog, sql_engine)
        except Exception as e:
            log.warning("live-db catalog filter failed (%s); using full catalog", e)
            self.catalog = catalog
        self.context_builder = ContextBuilder(catalog=self.catalog, dialect=self.dialect)
        self.repair = RepairLoop(
            llm=llm, sql_engine=sql_engine,
            max_attempts=max_repair_attempts, dialect=self.dialect,
        )
        self.viz_describer = VizDescriber(llm=llm)

    # ── Public entry point ────────────────────────────────────────────────────

    def answer(
        self,
        nl_question: str,
        *,
        execute: bool = True,
        max_rows: int = 100,
        retrieved_k: int = 8,
        few_shot_k: int = 3,
    ) -> PipelineResult:
        result = PipelineResult(nl_question=nl_question, sql="")
        t = _Timer(result.timings_ms)

        # [1] Domain classification
        with t("domain_route"):
            result.domains = self.domain_classifier.classify(nl_question)

        # [2] Entity resolution
        with t("entity_resolve"):
            result.resolved = self.entity_resolver.resolve(
                nl_question, domains=result.domains.domains or None,
            )

        # [3] Table retrieval (filtered by routed domains)
        with t("table_retrieve"):
            domain_filter = result.domains.domains or None
            result.retrieved_tables = self.retriever.search(
                nl_question, k=retrieved_k, domains=domain_filter, hybrid=True,
            )

        # [4] Steiner tree over retrieved tables
        with t("steiner"):
            target_fqns = [h.fqn for h in result.retrieved_tables]
            # Add bridge tables from descriptor resolutions so the join chain
            # actually reaches the descriptor codevalue filter.
            for cand in result.resolved.all_chosen():
                if cand.child_fqn:
                    target_fqns.append(cand.child_fqn)
                    target_fqns.append("edfi.Descriptor")
            # Inheritance: walk Ed-Fi-style PK-shared FKs to surface base-class
            # tables (e.g. EducationOrganization for any School/LEA/SEA) so the
            # LLM has access to inherited columns and the join chain that reaches
            # them. Detected from FK graph structure, not hardcoded.
            target_fqns.extend(
                _inheritance_chain_extras(self.graph, self.catalog, target_fqns)
            )
            target_fqns = _dedupe_preserve(target_fqns)
            result.join_tree = steiner(self.graph, target_fqns)

        # [5] Few-shot retrieval (gold store)
        with t("few_shots"):
            if self.gold_store is not None:
                try:
                    result.few_shots = self.gold_store.retrieve_top_k(
                        nl_question, domains=domain_filter, k=few_shot_k,
                    )
                except Exception as e:
                    log.debug("gold retrieval failed: %s", e)
                    result.few_shots = []

        # [6] Context assembly + LLM
        with t("context+llm"):
            result.prompt = self.context_builder.build(
                nl_question=nl_question,
                domain_routing=result.domains,
                retrieved_tables=result.retrieved_tables,
                steiner=result.join_tree,
                resolution=result.resolved,
                few_shots=result.few_shots,
            )
            sql, rationale = self._generate(result.prompt)
            result.sql = sql
            result.rationale = rationale

        # [7] Validation + repair loop (up to N attempts of "fix the error")
        with t("validate+repair"):
            repair: RepairResult = self.repair.run(
                prompt=result.prompt, initial_sql=result.sql, initial_rationale=result.rationale,
            )
            result.sql = repair.final_sql
            result.rationale = repair.final_rationale or result.rationale
            result.validated = repair.accepted
            result.repair_attempts = repair.attempts
            if not repair.accepted:
                result.error = (repair.attempts[-1].error if repair.attempts else "validation failed")

        # [8] Execute
        if execute and result.validated and result.sql:
            with t("execute"):
                try:
                    rows = self.sql_engine.execute(result.sql, limit=max_rows)
                    result.rows = rows
                    result.row_count = len(rows)
                    result.executed = True
                except Exception as e:
                    result.error = f"execute: {e}"

        # [9-10] Visualization + description (parallel inside annotate())
        if result.executed:
            with t("viz+desc"):
                viz_result: VizResult = self.viz_describer.annotate(
                    nl_question=nl_question, rows=result.rows, sql=result.sql,
                )
                result.viz = viz_result.spec
                result.description = viz_result.description
                if viz_result.spec is not None:
                    result.viz_vega_lite = viz_result.spec.to_vega_lite(result.rows)

        result.timings_ms["total_ms"] = t.total_ms()
        return result

    # ── Internals ─────────────────────────────────────────────────────────────

    def _generate(self, prompt: PromptContext) -> tuple[str, str]:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "sql": {"type": "string"},
                "rationale": {"type": "string"},
            },
            "required": ["sql", "rationale"],
        }
        raw = self.llm.complete(
            prompt.to_messages(),
            schema=schema,
            temperature=0.0,
            max_tokens=1200,
        )
        # `schema=` makes the provider request `response_format=json_schema strict`,
        # so the model is constrained to return valid JSON conforming to the
        # schema. Don't swallow parse errors here — a failure here means the
        # provider is misbehaving and should surface loud.
        payload = json.loads(raw)
        return payload["sql"].strip(), payload.get("rationale", "")

def _inheritance_parents(graph: FKGraph, catalog: TableCatalog, fqn: str) -> list[str]:
    """Walk Ed-Fi-style inheritance: a child's FK to its parent base class
    uses the child's own PK column. (e.g. School.SchoolId → EducationOrganization
    .EducationOrganizationId — School-IS-A-EducationOrganization.)

    Returns parent fqns reachable via such inheritance edges.
    """
    by_fqn = catalog.by_fqn()
    entry = by_fqn.get(fqn)
    if not entry or fqn not in graph.node_index:
        return []
    pk_set = {p.lower() for p in entry.primary_key}
    if not pk_set:
        return []
    node_id = graph.node_index[fqn]
    parents: list[str] = []
    for nb in graph.neighbors(node_id):
        edge = graph.edge_between(node_id, nb)
        if not edge:
            continue
        for fk in edge.fks:
            if fk.src_fqn != fqn:
                continue
            src_cols = {sc.lower() for sc, _ in fk.column_pairs}
            # Inheritance edge: every src column is part of the child's PK.
            if src_cols and src_cols.issubset(pk_set):
                parents.append(fk.dst_fqn)
                break
    return parents


def _inheritance_chain_extras(
    graph: FKGraph, catalog: TableCatalog, fqns: list[str]
) -> list[str]:
    """For each fqn, walk inheritance parents transitively. Returns the
    additional fqns to inject as Steiner targets so the LLM sees base-class
    columns (NameOfInstitution etc.) and the join path that reaches them."""
    out: list[str] = []
    seen: set[str] = set(fqns)
    frontier = list(fqns)
    while frontier:
        cur = frontier.pop()
        for parent in _inheritance_parents(graph, catalog, cur):
            if parent in seen:
                continue
            seen.add(parent)
            out.append(parent)
            frontier.append(parent)
    return out


def _filter_catalog_to_live_db(catalog: TableCatalog, sql_engine: SqlEngine) -> TableCatalog:
    """Return a new TableCatalog containing only tables/columns the engine sees."""
    from text2sql.table_catalog import ColumnInfo, TableEntry

    live_tables = {(s.lower(), t.lower()) for s, t in sql_engine.list_tables()}
    kept_entries: list[TableEntry] = []
    for entry in catalog.entries:
        if (entry.schema.lower(), entry.table.lower()) not in live_tables:
            continue
        try:
            live_cols = {c[0].lower(): c for c in sql_engine.list_columns(entry.schema, entry.table)}
        except Exception:
            live_cols = {}
        if not live_cols:
            kept_entries.append(entry)
            continue
        new_cols: list[ColumnInfo] = []
        existing_by_lower = {c.name.lower(): c for c in entry.columns}
        # Prefer catalog ColumnInfo (descriptions, samples) when names match;
        # add live-only columns with empty descriptions.
        for lower_name, (col_name, dtype, nullable) in live_cols.items():
            old = existing_by_lower.get(lower_name)
            if old is not None:
                new_cols.append(ColumnInfo(
                    name=col_name,
                    data_type=old.data_type or dtype,
                    nullable=old.nullable if old.nullable is not None else nullable,
                    description=old.description,
                    description_source=old.description_source,
                    is_identifying=old.is_identifying,
                    sample_values=old.sample_values,
                    distinct_count=old.distinct_count,
                ))
            else:
                new_cols.append(ColumnInfo(
                    name=col_name, data_type=dtype, nullable=nullable,
                ))
        kept_entries.append(TableEntry(
            schema=entry.schema, table=entry.table,
            description=entry.description, description_source=entry.description_source,
            domains=list(entry.domains),
            is_descriptor=entry.is_descriptor, is_association=entry.is_association,
            is_extension=entry.is_extension,
            primary_key=list(entry.primary_key),
            parent_neighbors=list(entry.parent_neighbors),
            child_neighbors=list(entry.child_neighbors),
            aggregate_root=entry.aggregate_root,
            columns=new_cols,
            sample_rows=list(entry.sample_rows),
            row_count=entry.row_count,
        ))
    return TableCatalog(
        data_standard_version=catalog.data_standard_version,
        generated_at=catalog.generated_at,
        entries=kept_entries,
        descriptor_codes=list(catalog.descriptor_codes),
    )


def _dedupe_preserve(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for s in seq:
        if s and s not in seen:
            out.append(s)
            seen.add(s)
    return out


class _Timer:
    """Tiny stopwatch helper that records ms per phase into a shared dict."""

    def __init__(self, sink: dict[str, float]) -> None:
        self._start_total = time.perf_counter()
        self._sink = sink

    def __call__(self, name: str):
        return _TimerCtx(self._sink, name)

    def total_ms(self) -> float:
        return (time.perf_counter() - self._start_total) * 1000.0


class _TimerCtx:
    def __init__(self, sink: dict[str, float], name: str) -> None:
        self._sink = sink
        self._name = name

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        self._sink[self._name] = (time.perf_counter() - self._start) * 1000.0
