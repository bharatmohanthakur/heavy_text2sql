"""CatalogInputs — single aggregator the catalog builder consumes.

Bundles the parsed Schema CSV + Relationships CSV into one bag of
typed records so downstream code never branches on "where did this
data come from?". Pivot: Q3.

Contract (kept narrow on purpose):
  inputs.columns       — list[ColumnRow]   (one per (schema, table, column))
  inputs.fk_edges      — list[FKEdge]      (composite-aware)
  inputs.tables_in_order — list[str]       (file-order fqns; preserves
                                            the operator's intended
                                            ranking signal)
  inputs.domains       — list[str]         (distinct values from the CSV,
                                            sorted by lowest-Ranking-first)

Anything more — descriptions, sample values, populated-flag rollups —
is a *derivation* the catalog builder produces from inputs + the live
target DB; it doesn't belong on this object.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from text2sql.catalog_inputs.relationships_csv import parse_relationships_csv
from text2sql.catalog_inputs.schema_csv import (
    ColumnRow,
    distinct_domains,
    group_by_table,
    parse_schema_csv,
)
from text2sql.graph.fk_parser import FKEdge


@dataclass(frozen=True, slots=True)
class CatalogInputs:
    """Frozen bundle of operator-supplied catalog inputs."""

    columns: tuple[ColumnRow, ...]
    fk_edges: tuple[FKEdge, ...]
    # Cached derivations — populated by `from_csvs()` so callers don't
    # have to recompute. Order matters: tables_in_order preserves the
    # operator's CSV order; domains are ranked.
    tables_in_order: tuple[str, ...] = field(default_factory=tuple)
    domains: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_csvs(
        cls,
        schema_csv: str | Path | Iterable[str],
        relationships_csv: str | Path | Iterable[str],
    ) -> "CatalogInputs":
        """Parse both CSVs and assemble the bundle. Parser-level
        validation errors propagate (SchemaCsvError /
        RelationshipsCsvError) — the Settings UI catches them."""
        cols = parse_schema_csv(schema_csv)
        fks = parse_relationships_csv(relationships_csv)
        seen: dict[str, None] = {}
        for c in cols:
            seen.setdefault(c.fqn, None)
        return cls(
            columns=tuple(cols),
            fk_edges=tuple(fks),
            tables_in_order=tuple(seen.keys()),
            domains=tuple(distinct_domains(cols)),
        )

    # ── Convenience views the catalog builder actually wants ─────────

    def columns_by_table(self) -> dict[str, list[ColumnRow]]:
        """`schema.table` fqn → ordered list of ColumnRow. Identical to
        schema_csv.group_by_table but exposed here so the builder
        doesn't need to import from two places."""
        return group_by_table(list(self.columns))

    def domain_for_table(self, fqn: str) -> str | None:
        """Authoritative domain for a table = the lowest-Ranking
        distinct domain across that table's columns. Same row may show
        up multiple times (one per column); we pick the highest-priority
        one. Returns None when the table has no rows in the CSV (the
        reflection path will pick that up as 'Other')."""
        cols = self.columns_by_table().get(fqn) or []
        if not cols:
            return None
        ranked = [c for c in cols if c.domain]
        if not ranked:
            return None
        return min(ranked, key=lambda c: (c.ranking, c.domain)).domain

    def fk_edges_for_table(self, fqn: str) -> list[FKEdge]:
        """All FK edges where this table is the parent (the side that
        owns the FK column). Used by the graph builder to seed
        out-edges per node."""
        return [e for e in self.fk_edges if e.src_fqn == fqn]

    @property
    def table_count(self) -> int:
        return len(self.tables_in_order)

    @property
    def column_count(self) -> int:
        return len(self.columns)

    @property
    def fk_count(self) -> int:
        return len(self.fk_edges)
