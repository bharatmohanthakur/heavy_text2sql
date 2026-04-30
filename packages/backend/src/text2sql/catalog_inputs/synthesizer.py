"""Synthesize TableMetadata + TableClassification from CatalogInputs.

The existing `build_table_catalog()` consumes three things:
  * `Iterable[TableClassification]` — per-table domain mapping
  * `CatalogIndex`                  — TableMetadata indexed by fqn
  * `manifest`                      — IngestionManifest with cached
                                      ApiModel JSONs (used to grab raw
                                      column data and entity flags)

This module synthesizes the first two from a CatalogInputs bundle and
returns a stub manifest with empty artifacts. The catalog builder's
existing fallback path (no apimodel columns → reflect from live DB)
becomes the *primary* path; that's exactly what we want once Ed-Fi
GitHub is gone.

PK and FK neighbor synthesis:
  * `parent_neighbors` / `child_neighbors` come from CatalogInputs.fk_edges
    — no live-DB call needed at synthesis time.
  * `identifying_columns` (PK) is left empty here. The catalog builder
    falls through to `_columns_from_db()` which uses SA Inspector's
    `get_pk_constraint()`. One source of truth for PKs is enough.

Pivot: Q3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from text2sql.catalog_inputs.loader import CatalogInputs
from text2sql.classification.metadata import CatalogIndex, TableMetadata
from text2sql.classification.table_mapping import TableClassification
from text2sql.providers.base import SqlEngine

log = logging.getLogger(__name__)


# Heuristic flag detection that doesn't require Ed-Fi-specific knowledge.
# Operators may name conventions differently; these are deliberately mild.

def _is_descriptor_table(name: str) -> bool:
    """A table whose name ends in 'Descriptor' is treated as a lookup
    table per Ed-Fi convention. Operators outside Ed-Fi can override
    via Q8's domain_overrides.yaml in a later step."""
    return name.endswith("Descriptor")


def _is_association_table(fk_edges_for_this_table: int) -> bool:
    """A table that's the parent (source) side of ≥2 FK edges is an
    association table by structural definition. Heuristic only —
    deliberately permissive to avoid mis-flagging at synthesis time."""
    return fk_edges_for_this_table >= 2


@dataclass(frozen=True)
class _StubArtifact:
    """Empty stand-in for a manifest artifact. The builder's
    `_entity_lookup(manifest.artifacts)` consumes it as an empty dict
    of entity data — the CSV path doesn't have raw ApiModel records."""
    api_model_path: Path | None = None
    source: str = "csv"


@dataclass(frozen=True)
class _StubManifest:
    """Build-time stand-in for IngestionManifest. The catalog builder
    reads two things from a manifest:

      * `.artifacts` — list of cached ApiModel JSONs. Empty on the CSV
        path is correct: every column comes from the live DB.
      * `.data_standard_version` — stamped onto the output TableCatalog
        for provenance. CSV path uses the operator-supplied label.
    """
    artifacts: tuple[_StubArtifact, ...] = ()
    data_standard_version: str = "operator-csv"


def synthesize_metadata(
    inputs: CatalogInputs,
    *,
    sql_engine: SqlEngine | None = None,
) -> tuple[list[TableMetadata], list[TableClassification]]:
    """Build (TableMetadata list, TableClassification list) from
    CatalogInputs. Same fqn ordering as `inputs.tables_in_order`.

    Each TableClassification carries exactly one domain — the
    operator's authoritative routing bucket. The Stage-1/2/3
    inheritance ladder collapses for the CSV path because the operator
    already declared the answer.

    When `sql_engine` is supplied, primary keys are reflected from the
    live DB and stamped into `identifying_columns`. Without an engine,
    PKs end up empty and downstream `_reflect_pk` falls through to the
    same Inspector call later — synthesizing here just spares the round
    trip when the engine's already at hand.
    """
    # Imported here to avoid a circular-import chain through the
    # catalog_builder module at package load time.
    from text2sql.table_catalog.catalog_builder import _reflect_pk

    columns_by_table = inputs.columns_by_table()

    # Pre-compute neighbor sets once. parent_neighbors[fqn] = tables this
    # one references via FK; child_neighbors[fqn] = tables that reference
    # this one. These match TableMetadata's existing semantics.
    parents: dict[str, list[str]] = {fqn: [] for fqn in inputs.tables_in_order}
    children: dict[str, list[str]] = {fqn: [] for fqn in inputs.tables_in_order}
    out_edge_count: dict[str, int] = {fqn: 0 for fqn in inputs.tables_in_order}
    for edge in inputs.fk_edges:
        # Initialize buckets even when one endpoint is absent from the
        # operator CSV — reflection (P7) will pick them up.
        parents.setdefault(edge.src_fqn, [])
        children.setdefault(edge.dst_fqn, [])
        out_edge_count.setdefault(edge.src_fqn, 0)
        if edge.dst_fqn not in parents[edge.src_fqn]:
            parents[edge.src_fqn].append(edge.dst_fqn)
        if edge.src_fqn not in children[edge.dst_fqn]:
            children[edge.dst_fqn].append(edge.src_fqn)
        out_edge_count[edge.src_fqn] += 1

    metas: list[TableMetadata] = []
    classifications: list[TableClassification] = []

    for fqn in inputs.tables_in_order:
        schema, _, table = fqn.partition(".")
        cols = columns_by_table.get(fqn) or []
        column_names = tuple(c.column for c in cols)

        is_descriptor = _is_descriptor_table(table)
        is_association = _is_association_table(out_edge_count.get(fqn, 0))
        # Without Ed-Fi spec we have no notion of "extension" — every
        # table the operator listed is treated as in-scope, and tables
        # the operator omitted but the live DB has are flagged
        # is_extension=True later, in `reflect_unknown_tables`.
        is_extension = False

        identifying: tuple[str, ...] = ()
        if sql_engine is not None:
            try:
                identifying = tuple(_reflect_pk(sql_engine, schema, table))
            except Exception as e:
                log.debug("PK reflection failed for %s: %s", fqn, e)

        meta = TableMetadata(
            schema=schema,
            name=table,
            is_abstract=False,
            is_descriptor=is_descriptor,
            is_association=is_association,
            is_extension=is_extension,
            description="",                       # filled by LLM at build time
            column_names=column_names,
            identifying_columns=identifying,
            parent_neighbors=tuple(parents.get(fqn, ())),
            child_neighbors=tuple(children.get(fqn, ())),
            aggregate_root=None,                  # not modeled in the CSV format
            apimodel_domain_hints=tuple(),        # not used on the CSV path
        )
        metas.append(meta)

        domain = inputs.domain_for_table(fqn)
        domains_list = [domain] if domain else []
        classifications.append(TableClassification(
            schema=schema,
            table=table,
            domains=domains_list,
            is_descriptor=is_descriptor,
            is_association=is_association,
            is_extension=is_extension,
            aggregate_root=None,
            source="operator_csv",
            confidence=1.0,
        ))

    return metas, classifications


def synthesize_inputs_for_builder(
    inputs: CatalogInputs,
    *,
    sql_engine: SqlEngine | None = None,
) -> tuple[CatalogIndex, list[TableClassification], _StubManifest]:
    """One-call helper: produce the trio `build_table_catalog()` needs.
    Useful for tests and the new `text2sql ingest-csvs` CLI entry point.

    Pass `sql_engine` to reflect primary keys from the live DB at
    synthesis time (one round trip per table, vs. having
    `build_table_catalog()` do it later).
    """
    metas, cls = synthesize_metadata(inputs, sql_engine=sql_engine)
    idx = CatalogIndex(tables=metas)
    idx.by_fqn = {m.fqn: m for m in metas}
    for m in metas:
        idx.by_name.setdefault(m.name, []).append(m)
    return idx, cls, _StubManifest()
