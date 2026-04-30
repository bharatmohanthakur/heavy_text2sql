"""Per-table metadata extraction from an IngestionManifest.

The LLM classifier needs concrete context per table: column names, key
neighbors via FKs, description, and any pre-existing domain hints. This
module loads ApiModel.json once and emits one TableMetadata per entity.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from text2sql.ingestion.edfi_fetcher import IngestionManifest


@dataclass(frozen=True)
class TableMetadata:
    schema: str                      # "edfi" | "tpdm" | …
    name: str                        # entity name (matches table name)
    is_abstract: bool
    is_descriptor: bool
    is_association: bool
    is_extension: bool
    description: str
    column_names: tuple[str, ...]
    identifying_columns: tuple[str, ...]   # primary-key column names
    parent_neighbors: tuple[str, ...]   # tables this one references via FK
    child_neighbors: tuple[str, ...]    # tables that reference this one via FK
    aggregate_root: str | None
    apimodel_domain_hints: tuple[str, ...]   # ApiModel's domains[] — used as a hint, not gospel

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class CatalogIndex:
    """Pre-computed indexes over a manifest — built once, queried many times."""
    tables: list[TableMetadata]
    by_fqn: dict[str, TableMetadata] = field(default_factory=dict)
    by_name: dict[str, list[TableMetadata]] = field(default_factory=dict)

    @classmethod
    def from_manifest(cls, manifest: IngestionManifest) -> "CatalogIndex":
        tables = list(extract_tables(manifest))
        idx = cls(tables=tables)
        idx.by_fqn = {t.fqn: t for t in tables}
        for t in tables:
            idx.by_name.setdefault(t.name, []).append(t)
        return idx


def extract_tables(manifest: IngestionManifest) -> Iterable[TableMetadata]:
    """Yield TableMetadata objects for every entity in every artifact set."""

    # Pass 1: load all entities, build aggregate-root map and FK neighbor map.
    raw_entities: list[dict] = []
    schemas_per_source: dict[str, str] = {}      # source -> default schema for un-schema'd entries
    parent_map: dict[str, set[str]] = defaultdict(set)   # child fqn -> {parent fqn}
    child_map: dict[str, set[str]] = defaultdict(set)    # parent fqn -> {child fqn}
    aggregate_root_for: dict[str, str] = {}              # fqn -> root fqn

    for art in manifest.artifacts:
        data = json.loads(art.api_model_path.read_text(encoding="utf-8"))
        is_extension = art.source.startswith("ext:")
        for ent in data.get("entityDefinitions", []):
            ent["_is_extension"] = is_extension
            raw_entities.append(ent)

        # FKs (associations): each has identifyingProperties + nonIdentifying with refs.
        for assoc in data.get("associationDefinitions", []):
            primary = assoc.get("primaryEntityFullName", {})
            secondary = assoc.get("secondaryEntityFullName", {})
            if primary and secondary:
                p_fqn = f"{primary['schema']}.{primary['name']}"
                s_fqn = f"{secondary['schema']}.{secondary['name']}"
                # secondary references primary
                parent_map[s_fqn].add(p_fqn)
                child_map[p_fqn].add(s_fqn)

        # Aggregate roots
        for agg in data.get("aggregateDefinitions", []):
            root = agg.get("aggregateRootEntityName", {})
            if not root:
                continue
            root_fqn = f"{root['schema']}.{root['name']}"
            for member in agg.get("aggregateEntityNames", []):
                m_fqn = f"{member['schema']}.{member['name']}"
                aggregate_root_for[m_fqn] = root_fqn

    # Pass 2: emit metadata records.
    for ent in raw_entities:
        schema = ent.get("schema", "edfi")
        name = ent["name"]
        fqn = f"{schema}.{name}"
        cols = tuple(p["propertyName"] for p in ent.get("locallyDefinedProperties", []) if p.get("propertyName"))
        primary = next((i for i in ent.get("identifiers", []) if i.get("isPrimary")), None)
        pk_cols = tuple(primary.get("identifyingPropertyNames", []) if primary else ())
        yield TableMetadata(
            schema=schema,
            name=name,
            is_abstract=bool(ent.get("isAbstract")),
            is_descriptor=name.endswith("Descriptor"),
            is_association=name.endswith("Association"),
            is_extension=bool(ent.get("_is_extension")),
            description=ent.get("description", "") or "",
            column_names=cols,
            identifying_columns=pk_cols,
            parent_neighbors=tuple(sorted(parent_map.get(fqn, set()))),
            child_neighbors=tuple(sorted(child_map.get(fqn, set()))),
            aggregate_root=aggregate_root_for.get(fqn),
            apimodel_domain_hints=tuple(ent.get("domains", []) or []),
        )
