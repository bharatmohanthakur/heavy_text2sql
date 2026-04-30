"""Build a single TableCatalog (one entry per table).

Inputs:
  * IngestionManifest (Component 1) — gives us cached ApiModel.json files
  * TableClassification (Component 2a) — gives us domains[] per table
  * SqlEngine (optional) — gives us live row counts, sample values, sample rows
  * DescriptionGenerator (optional) — fills missing column descriptions only

Output: a TableCatalog persisted as data/artifacts/table_catalog.json — one
record per table. No domain-pack duplication. Domains are tags.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from text2sql.classification.metadata import CatalogIndex, TableMetadata
from text2sql.classification.table_mapping import TableClassification
from text2sql.providers.base import SqlEngine
from text2sql.table_catalog.description_generator import (
    DescriptionGenerator,
    TableSampleData,
)

log = logging.getLogger(__name__)

DEFAULT_LOW_CARD_THRESHOLD = 50
DEFAULT_MAX_VALUES_PER_COLUMN = 25
DEFAULT_MAX_SCAN_ROWS = 5000
DEFAULT_SAMPLE_ROW_COUNT = 5

# Tables that are *themselves* lookups: every row is a value worth indexing.
# We sample them with much higher caps so entity resolution can find
# "Hispanic", "Pre-K", "Algebra I" etc.
LOOKUP_TABLE_OVERRIDES: dict[str, dict[str, int]] = {}

# `edfi.Descriptor` is the master abstract table that holds every descriptor's
# CodeValue + ShortDescription + Namespace. Each child `*Descriptor` table just
# holds the typed ID (FK to descriptor). For entity resolution we treat it
# specially: pull every (codevalue, namespace) pair, then route the value to
# the *child* descriptor table whose FK is what other tables reference.
DESCRIPTOR_MASTER_FQN = "edfi.Descriptor"

# Stock descriptions for Ed-Fi standard audit / plumbing columns. Used as a
# final fallback so common columns never end up empty even when the LLM call
# was blocked or failed.
STOCK_COLUMN_DESCRIPTIONS: dict[str, str] = {
    "CreateDate": "Timestamp when this row was first created.",
    "LastModifiedDate": "Timestamp when this row was last modified.",
    "Id": "Globally unique identifier (UUID) for this row, surfaced by the Ed-Fi API.",
    "ChangeVersion": "Monotonic change-tracking version assigned by the ODS.",
    "Discriminator": "Internal type discriminator used for inheritance hierarchies.",
    "AggregateId": "Internal aggregate identifier used by the Ed-Fi data model.",
}


@dataclass
class ColumnInfo:
    name: str
    data_type: str | None = None
    nullable: bool | None = None
    description: str = ""
    description_source: str = ""           # "apimodel" | "llm" | "cache" | "" (none)
    is_identifying: bool = False
    sample_values: list[str] = field(default_factory=list)
    distinct_count: int | None = None


@dataclass
class TableEntry:
    schema: str
    table: str
    description: str
    description_source: str                # "apimodel" | "llm" | "fallback"
    domains: list[str]                     # multi-label tag, ordered, from ApiModel
    is_descriptor: bool
    is_association: bool
    is_extension: bool
    primary_key: list[str]
    parent_neighbors: list[str]
    child_neighbors: list[str]
    aggregate_root: str | None
    columns: list[ColumnInfo]
    sample_rows: list[dict] = field(default_factory=list)
    row_count: int | None = None

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    def has_domain(self, domain: str) -> bool:
        return domain in self.domains


@dataclass
class DescriptorCode:
    """One row from edfi.Descriptor, resolved to the child descriptor table.

    Used by Component 6 (entity resolver) and Component 8 (schema linker) so
    a phrase like "Hispanic" maps to a complete join chain:
      WHERE edfi.descriptor.codevalue = 'Hispanic'
      via   edfi.<type_name>.<type_name>id  (the child)
            referenced by domain tables that filter by that descriptor type.
    """
    descriptor_id: int
    code_value: str
    short_description: str
    description: str
    namespace: str
    type_name: str           # e.g. "OldEthnicityDescriptor" — last segment of namespace
    child_fqn: str           # e.g. "edfi.OldEthnicityDescriptor"; may be "" if unknown


@dataclass
class TableCatalog:
    data_standard_version: str
    generated_at: str
    entries: list[TableEntry]
    descriptor_codes: list["DescriptorCode"] = field(default_factory=list)
    # Per-provider provenance (N2). When absent (legacy flat catalogs),
    # both fields are empty strings — readers tolerate that for backwards
    # compat. New writes always populate both.
    provider_name: str = ""
    target_dialect: str = ""

    def by_fqn(self) -> dict[str, TableEntry]:
        return {e.fqn: e for e in self.entries}

    def in_domain(self, domain: str) -> list[TableEntry]:
        return [e for e in self.entries if e.has_domain(domain)]

    def domain_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.entries:
            for d in e.domains:
                out[d] = out.get(d, 0) + 1
        return out


# ── Live-DB helpers ───────────────────────────────────────────────────────────


def _qual(engine: SqlEngine, schema: str, table: str) -> str:
    if engine.dialect == "sqlite":
        # Single schema (`main`); the Ed-Fi catalog's schema (e.g. "edfi")
        # doesn't exist on SQLite. Emit the unqualified table — SQLite
        # resolves it against `main` automatically.
        return engine.quote_identifier(table)
    return f"{engine.quote_identifier(schema)}.{engine.quote_identifier(table)}"


def _row_count(engine: SqlEngine, schema: str, table: str) -> int | None:
    try:
        rows = engine.execute(f"SELECT COUNT(*) AS n FROM {_qual(engine, schema, table)}")
        return int(rows[0]["n"]) if rows else 0
    except Exception:
        return None


def _sample_rows(engine: SqlEngine, schema: str, table: str, n: int) -> list[dict]:
    qual = _qual(engine, schema, table)
    try:
        if engine.dialect == "mssql":
            return engine.execute(f"SELECT TOP {n} * FROM {qual}")
        return engine.execute(f"SELECT * FROM {qual} LIMIT {n}")
    except Exception:
        return []


def _column_distinct(
    engine: SqlEngine, schema: str, table: str, column: str,
    *, max_values: int, max_scan_rows: int, low_card_threshold: int,
) -> tuple[list[str], int | None]:
    qual = _qual(engine, schema, table)
    qcol = engine.quote_identifier(column)
    if engine.dialect == "mssql":
        probe = f"SELECT COUNT(DISTINCT {qcol}) AS n FROM (SELECT TOP {max_scan_rows} {qcol} FROM {qual}) s"
    else:
        probe = f"SELECT COUNT(DISTINCT {qcol}) AS n FROM (SELECT {qcol} FROM {qual} LIMIT {max_scan_rows}) s"
    try:
        rows = engine.execute(probe)
    except Exception:
        return [], None
    distinct = int(rows[0]["n"]) if rows else 0
    if distinct == 0:
        return [], 0
    if distinct > low_card_threshold:
        return [], distinct
    if engine.dialect == "mssql":
        sample_sql = (
            f"SELECT DISTINCT TOP {max_values} {qcol} AS v "
            f"FROM {qual} WHERE {qcol} IS NOT NULL"
        )
    else:
        sample_sql = (
            f"SELECT DISTINCT {qcol} AS v FROM {qual} "
            f"WHERE {qcol} IS NOT NULL LIMIT {max_values}"
        )
    try:
        rows = engine.execute(sample_sql)
    except Exception:
        return [], distinct
    return [str(r["v"]) for r in rows if r.get("v") is not None], distinct


# ── Column source: ApiModel first, live DB as fallback for descriptors ────────


def _columns_from_apimodel(t: TableMetadata, raw_entity: dict) -> list[ColumnInfo]:
    pk = set(t.identifying_columns)
    out: list[ColumnInfo] = []
    for prop in raw_entity.get("locallyDefinedProperties", []):
        name = prop.get("propertyName")
        if not name:
            continue
        ptype = prop.get("propertyType", {}) or {}
        desc = (prop.get("description") or "").strip()
        out.append(ColumnInfo(
            name=name,
            data_type=ptype.get("dbType"),
            nullable=ptype.get("isNullable"),
            description=desc,
            description_source="apimodel" if desc else "",
            is_identifying=name in pk,
        ))
    return out


def _columns_from_db(
    engine: SqlEngine, schema: str, table: str, pk: list[str]
) -> list[ColumnInfo]:
    pk_set = set(pk)
    return [
        ColumnInfo(
            name=name,
            data_type=dtype,
            nullable=nullable,
            description="",
            description_source="",
            is_identifying=name in pk_set,
        )
        for name, dtype, nullable in engine.list_columns(schema, table)
    ]


# ── Public API ────────────────────────────────────────────────────────────────


def _entity_lookup(manifest_artifacts: list) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for art in manifest_artifacts:
        data = json.loads(Path(art.api_model_path).read_text(encoding="utf-8"))
        for ent in data.get("entityDefinitions", []):
            fqn = f"{ent.get('schema', 'edfi')}.{ent['name']}"
            out[fqn] = ent
    return out


def build_table_catalog(
    classifications: Iterable[TableClassification],
    catalog_index: CatalogIndex,
    manifest,
    *,
    sql_engine: SqlEngine | None = None,
    description_generator: DescriptionGenerator | None = None,
    enrich_values: bool = True,
    max_values_per_column: int = DEFAULT_MAX_VALUES_PER_COLUMN,
    max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS,
    sample_row_count: int = DEFAULT_SAMPLE_ROW_COUNT,
    low_card_threshold: int = DEFAULT_LOW_CARD_THRESHOLD,
    only_fqns: set[str] | None = None,
    provider_name: str = "",
) -> TableCatalog:
    """Build the catalog. One record per table."""
    cls_by_fqn = {c.fqn: c for c in classifications}
    raw_entities = _entity_lookup(manifest.artifacts)
    by_fqn_meta = catalog_index.by_fqn

    targets = (
        [m for f, m in by_fqn_meta.items() if f in only_fqns]
        if only_fqns is not None
        else list(by_fqn_meta.values())
    )

    entries: list[TableEntry] = []
    samples_for_llm: list[TableSampleData] = []

    for meta in targets:
        c = cls_by_fqn.get(meta.fqn)
        if not c:
            continue
        raw = raw_entities.get(meta.fqn) or {}

        # Columns: ApiModel first, fall back to live DB for tables w/o
        # locallyDefinedProperties (descriptors mostly).
        columns = _columns_from_apimodel(meta, raw)
        if not columns and sql_engine is not None:
            try:
                columns = _columns_from_db(
                    sql_engine, meta.schema, meta.name, list(meta.identifying_columns)
                )
            except Exception as e:
                log.debug("live-db column discovery failed for %s: %s", meta.fqn, e)

        row_count: int | None = None
        sample_rows: list[dict] = []

        if sql_engine is not None and enrich_values:
            row_count = _row_count(sql_engine, meta.schema, meta.name)
            if row_count and row_count > 0:
                # Apply per-table overrides for known lookup tables.
                override = LOOKUP_TABLE_OVERRIDES.get(meta.fqn, {})
                eff_max_values = override.get("max_values_per_column", max_values_per_column)
                eff_max_scan = override.get("max_scan_rows", max_scan_rows)
                eff_low_card = override.get("low_card_threshold", low_card_threshold)

                sample_rows = _sample_rows(
                    sql_engine, meta.schema, meta.name, sample_row_count
                )
                # Skip per-column sampling of the master Descriptor table —
                # we'll fan its codes out to the child descriptor entries below.
                # Also skip child *Descriptor tables: they only hold an opaque
                # integer ID (FK to descriptor) which is useless for entity
                # resolution.
                if meta.fqn == DESCRIPTOR_MASTER_FQN or meta.is_descriptor:
                    pass
                else:
                    for col in columns:
                        vals, dc = _column_distinct(
                            sql_engine, meta.schema, meta.name, col.name,
                            max_values=eff_max_values,
                            max_scan_rows=eff_max_scan,
                            low_card_threshold=eff_low_card,
                        )
                        col.sample_values = vals
                        col.distinct_count = dc

        # Use ApiModel description verbatim — it's already 100% populated for
        # DS 6.1.0. We only fall back when ApiModel left it blank (other DBs).
        desc = (meta.description or "").strip()
        desc_source = "apimodel" if desc else "fallback"

        entry = TableEntry(
            schema=meta.schema,
            table=meta.name,
            description=desc,
            description_source=desc_source,
            domains=list(c.domains),
            is_descriptor=meta.is_descriptor,
            is_association=meta.is_association,
            is_extension=meta.is_extension,
            primary_key=list(meta.identifying_columns),
            parent_neighbors=list(meta.parent_neighbors),
            child_neighbors=list(meta.child_neighbors),
            aggregate_root=meta.aggregate_root,
            columns=columns,
            sample_rows=sample_rows,
            row_count=row_count,
        )
        entries.append(entry)

        # Queue gap-fill request only for genuinely missing descriptions.
        if description_generator is not None:
            cols_to_fill = [
                col.name for col in columns if not col.description
            ]
            need_table_desc = not desc
            if cols_to_fill or need_table_desc:
                samples_for_llm.append(TableSampleData(
                    schema=meta.schema,
                    table=meta.name,
                    apimodel_table_description=desc,
                    columns=[
                        {
                            "name": col.name,
                            "data_type": col.data_type,
                            "nullable": col.nullable,
                            "samples": col.sample_values,
                            "distinct_count": col.distinct_count,
                        }
                        for col in columns
                    ],
                    sample_rows=sample_rows,
                    row_count=row_count,
                    request_table_desc=need_table_desc,
                    columns_to_describe=cols_to_fill,
                ))

    # LLM gap-fill (only fires for missing descriptions; ~0 table descs in
    # DS 6.1.0, ~882 column descs).
    if description_generator is not None and samples_for_llm:
        results = description_generator.generate_many(samples_for_llm, max_workers=8)
        by_fqn = {e.fqn: e for e in entries}
        for fqn, gd in results.items():
            entry = by_fqn.get(fqn)
            if not entry:
                continue
            if gd.table_description and not entry.description:
                entry.description = gd.table_description
                entry.description_source = gd.source
            for col in entry.columns:
                if col.description:
                    continue   # ApiModel already had this one — keep it
                new_desc = gd.column_descriptions.get(col.name)
                if new_desc:
                    col.description = new_desc
                    col.description_source = gd.source

    # Final fallback: stock descriptions for Ed-Fi audit columns that are still
    # blank (e.g., when the LLM was blocked or unavailable).
    for entry in entries:
        for col in entry.columns:
            if col.description:
                continue
            stock = STOCK_COLUMN_DESCRIPTIONS.get(col.name)
            if stock:
                col.description = stock
                col.description_source = "stock"

    # Pull every descriptor code with its namespace once. These are the
    # human-readable values ("Hispanic", "Pre-K", etc.) that entity resolution
    # needs to find — but the codevalue alone isn't enough; the namespace tells
    # downstream layers WHICH child descriptor table the value belongs to,
    # which in turn tells the schema linker what FK column to filter on.
    descriptor_codes: list[DescriptorCode] = []
    if sql_engine is not None and enrich_values:
        descriptor_codes = _pull_descriptor_codes(sql_engine, entries)

    catalog = TableCatalog(
        data_standard_version=manifest.data_standard_version,
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        entries=entries,
        descriptor_codes=descriptor_codes,
        provider_name=provider_name,
        target_dialect=(sql_engine.dialect if sql_engine is not None else ""),
    )
    return catalog


# ── Descriptor capture ────────────────────────────────────────────────────────


def _pull_descriptor_codes(
    sql_engine: SqlEngine, entries: list[TableEntry]
) -> list["DescriptorCode"]:
    """Pull every (codevalue, namespace) pair from edfi.descriptor and resolve
    each to its child descriptor table's fqn.

    Returns [] if edfi.descriptor isn't reachable on this engine.
    """
    qual = _qual(sql_engine, "edfi", "Descriptor")
    sql = (
        f"SELECT {sql_engine.quote_identifier('DescriptorId')} AS descriptor_id, "
        f"{sql_engine.quote_identifier('CodeValue')} AS code_value, "
        f"{sql_engine.quote_identifier('ShortDescription')} AS short_description, "
        f"{sql_engine.quote_identifier('Description')} AS description, "
        f"{sql_engine.quote_identifier('Namespace')} AS namespace "
        f"FROM {qual}"
    )
    try:
        rows = sql_engine.execute(sql)
    except Exception as e:
        log.debug("descriptor pull failed: %s", e)
        return []

    # Map {child_fqn_lower → child_fqn_with_proper_case} so we can resolve
    # namespace tails to a real catalog entry case-insensitively. We index by
    # the trailing path segment of the namespace, which Ed-Fi guarantees is
    # exactly the child descriptor table's name (e.g. "OldEthnicityDescriptor").
    child_by_typename: dict[str, str] = {}
    for e in entries:
        if e.is_descriptor and e.schema == "edfi":
            child_by_typename[e.table.lower()] = e.fqn

    out: list[DescriptorCode] = []
    for r in rows:
        ns = (r.get("namespace") or "").strip()
        type_name = ns.rsplit("/", 1)[-1] if ns else ""
        child_fqn = child_by_typename.get(type_name.lower()) if type_name else None
        out.append(DescriptorCode(
            descriptor_id=int(r["descriptor_id"]),
            code_value=str(r.get("code_value") or ""),
            short_description=str(r.get("short_description") or ""),
            description=str(r.get("description") or ""),
            namespace=ns,
            type_name=type_name,
            child_fqn=child_fqn or "",
        ))
    return out


# ── Persistence ───────────────────────────────────────────────────────────────


def save_table_catalog(catalog: TableCatalog, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "data_standard_version": catalog.data_standard_version,
        "generated_at": catalog.generated_at,
        "provider_name": catalog.provider_name,
        "target_dialect": catalog.target_dialect,
        "entry_count": len(catalog.entries),
        "descriptor_code_count": len(catalog.descriptor_codes),
        "domain_counts": catalog.domain_counts(),
        "entries": [asdict(e) for e in catalog.entries],
        "descriptor_codes": [asdict(d) for d in catalog.descriptor_codes],
    }
    path.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True), encoding="utf-8")


def load_table_catalog(
    path: Path,
    *,
    expected_provider: str | None = None,
) -> TableCatalog:
    """Load a catalog. If `expected_provider` is given, raises a clear
    error when the manifest's provider_name doesn't match — prevents
    silently using one provider's catalog against another's live DB."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    provider_name = raw.get("provider_name", "")
    if expected_provider and provider_name and provider_name != expected_provider:
        raise RuntimeError(
            f"catalog at {path} was built for provider {provider_name!r}, "
            f"but active provider is {expected_provider!r}. Run "
            f"`text2sql rebuild --provider {expected_provider}` to refresh."
        )
    entries = [
        TableEntry(
            **{**e, "columns": [ColumnInfo(**c) for c in e["columns"]]}
        )
        for e in raw["entries"]
    ]
    descriptor_codes = [DescriptorCode(**d) for d in raw.get("descriptor_codes", [])]
    return TableCatalog(
        data_standard_version=raw["data_standard_version"],
        generated_at=raw["generated_at"],
        entries=entries,
        descriptor_codes=descriptor_codes,
        provider_name=provider_name,
        target_dialect=raw.get("target_dialect", ""),
    )
