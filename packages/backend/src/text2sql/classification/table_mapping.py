"""Sub-component 2a: deterministic table → domain mapping.

90% of entities have `domains[]` populated in ApiModel.json — that's the gold
source. Empty mappings are filled by:

  1. aggregate inheritance — auxiliary tables inherit their aggregate root's domains.
  2. descriptor referrer voting — *Descriptor tables with no domain take the most
     common domain among entities that reference them via FK.
  3. LLM fallback — for the few tables left (typically <5), call the LLM once.

YAML overrides apply last and always win.

This module produces the artifact every other component needs:
  data/artifacts/table_classification.json
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from text2sql.classification.catalog import DomainCatalog
from text2sql.classification.metadata import CatalogIndex, TableMetadata
from text2sql.providers.base import LLMMessage, LLMProvider

log = logging.getLogger(__name__)


@dataclass
class TableClassification:
    schema: str
    table: str
    domains: list[str]                # full ordered list straight from ApiModel.json
    is_descriptor: bool
    is_association: bool
    is_extension: bool
    aggregate_root: str | None
    source: str                       # apimodel | aggregate_inheritance | descriptor_voting | llm | override
    confidence: float

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    # Backwards-compat shims for callers that previously asked for primary/secondary.
    @property
    def primary_domain(self) -> str | None:
        return self.domains[0] if self.domains else None

    @property
    def secondary_domain(self) -> str | None:
        return self.domains[1] if len(self.domains) > 1 else None


@dataclass
class TableClassificationOutput:
    data_standard_version: str
    generated_at: str
    catalog: list[str]
    classifications: list[TableClassification]
    summary: dict[str, Any] = field(default_factory=dict)


# ── Stage 1: direct read from ApiModel ────────────────────────────────────────


def _stage1_direct(t: TableMetadata) -> TableClassification | None:
    if not t.apimodel_domain_hints:
        return None
    return TableClassification(
        schema=t.schema,
        table=t.name,
        domains=list(t.apimodel_domain_hints),
        is_descriptor=t.is_descriptor,
        is_association=t.is_association,
        is_extension=t.is_extension,
        aggregate_root=t.aggregate_root,
        source="apimodel",
        confidence=1.0,
    )


# ── Stage 2: aggregate inheritance ────────────────────────────────────────────


def _stage2_aggregate(
    t: TableMetadata, by_fqn: dict[str, TableClassification]
) -> TableClassification | None:
    if t.aggregate_root and t.aggregate_root != t.fqn:
        root = by_fqn.get(t.aggregate_root)
        if root and root.domains:
            return TableClassification(
                schema=t.schema,
                table=t.name,
                domains=list(root.domains),
                is_descriptor=t.is_descriptor,
                is_association=t.is_association,
                is_extension=t.is_extension,
                aggregate_root=t.aggregate_root,
                source="aggregate_inheritance",
                confidence=0.95,
            )
    return None


# ── Stage 3: descriptor referrer voting ───────────────────────────────────────


def _stage3_descriptor(
    t: TableMetadata,
    index: CatalogIndex,
    by_fqn: dict[str, TableClassification],
) -> TableClassification | None:
    if not t.is_descriptor:
        return None
    # Find tables that reference this descriptor (i.e. its child_neighbors are
    # the *referrers* — they cite this descriptor as a parent in FK terms).
    # In our metadata, parent_neighbors = "tables this references"; we want the
    # other direction: who references THIS table.
    referrers = t.child_neighbors    # we registered child_map[parent] earlier; parent is the descriptor
    counter: Counter[str] = Counter()
    for ref_fqn in referrers:
        c = by_fqn.get(ref_fqn)
        if c and c.domains:
            for d in c.domains:
                counter[d] += 1
    if not counter:
        return None
    # Keep all domains the descriptor's referrers vote for, ordered by vote count.
    domains = [d for d, _ in counter.most_common()]
    return TableClassification(
        schema=t.schema,
        table=t.name,
        domains=domains,
        is_descriptor=True,
        is_association=t.is_association,
        is_extension=t.is_extension,
        aggregate_root=t.aggregate_root,
        source="descriptor_voting",
        confidence=0.85,
    )


# ── Stage 4: LLM fallback (rarely fires) ──────────────────────────────────────


_LLM_FALLBACK_SYSTEM = (
    "You assign a database table to ONE primary and at most ONE secondary domain "
    "from a fixed list. Reply with JSON only: "
    '{"primary_domain": "...", "secondary_domain": "..." or null, "confidence": 0..1}.'
)


def _stage4_llm(
    t: TableMetadata, catalog: DomainCatalog, llm: LLMProvider | None
) -> TableClassification:
    if llm is None:
        # No LLM available — emit best-effort with low confidence so a human can review.
        return TableClassification(
            schema=t.schema,
            table=t.name,
            domains=["Other"],
            is_descriptor=t.is_descriptor,
            is_association=t.is_association,
            is_extension=t.is_extension,
            aggregate_root=t.aggregate_root,
            source="llm",
            confidence=0.4,
        )
    catalog_block = "\n".join(f"- {d.name}: {d.description}" for d in catalog.domains)
    user = (
        f"TABLE: {t.fqn}\n"
        f"COLUMNS: {', '.join(t.column_names[:25])}\n"
        f"REFERENCES: {', '.join(t.parent_neighbors[:8]) or '(none)'}\n"
        f"REFERENCED BY: {', '.join(t.child_neighbors[:8]) or '(none)'}\n"
        f"DESCRIPTION: {(t.description or '')[:400]}\n\n"
        f"DOMAINS:\n{catalog_block}\n"
    )
    valid = {*(d.name for d in catalog.domains), "Other"}
    try:
        raw = llm.complete(
            [LLMMessage(role="system", content=_LLM_FALLBACK_SYSTEM),
             LLMMessage(role="user", content=user)],
            schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "primary_domain": {"type": "string"},
                    "secondary_domain": {"type": ["string", "null"]},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                },
                "required": ["primary_domain", "secondary_domain", "confidence"],
            },
            max_tokens=200,
            temperature=0.0,
        )
    except Exception as e:
        log.warning("LLM fallback failed for %s: %s", t.fqn, e)
        return TableClassification(
            schema=t.schema,
            table=t.name,
            domains=["Other"],
            is_descriptor=t.is_descriptor,
            is_association=t.is_association,
            is_extension=t.is_extension,
            aggregate_root=t.aggregate_root,
            source="llm",
            confidence=0.3,
        )
    # strict json_schema → trust the parse
    payload = json.loads(raw)
    primary = payload["primary_domain"] if payload["primary_domain"] in valid else "Other"
    secondary = payload["secondary_domain"]
    if secondary not in valid or secondary == primary:
        secondary = None
    domains = [d for d in (primary, secondary) if d]
    return TableClassification(
        schema=t.schema,
        table=t.name,
        domains=domains,
        is_descriptor=t.is_descriptor,
        is_association=t.is_association,
        is_extension=t.is_extension,
        aggregate_root=t.aggregate_root,
        source="llm",
        confidence=float(payload["confidence"]),
    )


# ── Overrides ─────────────────────────────────────────────────────────────────


def _apply_overrides(
    classifications: list[TableClassification], overrides_path: Path
) -> list[TableClassification]:
    if not overrides_path.exists():
        return classifications
    raw = yaml.safe_load(overrides_path.read_text(encoding="utf-8")) or {}
    by_fqn = {f"{o.get('schema','edfi')}.{o['table']}": o for o in raw.get("overrides", [])}
    out: list[TableClassification] = []
    for c in classifications:
        spec = by_fqn.get(c.fqn)
        if not spec:
            out.append(c)
            continue
        primary = spec.get("primary_domain", c.primary_domain)
        secondary = spec.get("secondary_domain", c.secondary_domain)
        domains = [d for d in (primary, secondary) if d]
        out.append(TableClassification(
            schema=c.schema, table=c.table,
            primary_domain=primary, secondary_domain=secondary,
            all_domains=domains, is_descriptor=c.is_descriptor,
            is_association=c.is_association, is_extension=c.is_extension,
            aggregate_root=c.aggregate_root,
            source="override", confidence=1.0,
        ))
    return out


# ── Public API ────────────────────────────────────────────────────────────────


def map_tables(
    index: CatalogIndex,
    catalog: DomainCatalog,
    *,
    llm: LLMProvider | None = None,
    overrides_path: Path | None = None,
) -> list[TableClassification]:
    """Full table → domain mapping. LLM only fires on residuals (typically <5)."""
    by_fqn: dict[str, TableClassification] = {}

    # Stage 1: direct
    for t in index.tables:
        c = _stage1_direct(t)
        if c:
            by_fqn[t.fqn] = c

    # Stage 2: aggregate inheritance
    for t in index.tables:
        if t.fqn in by_fqn:
            continue
        c = _stage2_aggregate(t, by_fqn)
        if c:
            by_fqn[t.fqn] = c

    # Stage 3: descriptor voting
    for t in index.tables:
        if t.fqn in by_fqn:
            continue
        c = _stage3_descriptor(t, index, by_fqn)
        if c:
            by_fqn[t.fqn] = c

    # Stage 4: LLM (residuals only)
    residuals = [t for t in index.tables if t.fqn not in by_fqn]
    for t in residuals:
        by_fqn[t.fqn] = _stage4_llm(t, catalog, llm)

    out = [by_fqn[t.fqn] for t in index.tables]
    if overrides_path:
        out = _apply_overrides(out, overrides_path)
    return out


def write_table_mapping(
    out_path: Path,
    classifications: list[TableClassification],
    *,
    data_standard_version: str,
    catalog: DomainCatalog,
) -> TableClassificationOutput:
    by_source: Counter[str] = Counter(c.source for c in classifications)
    by_primary: Counter[str] = Counter(c.primary_domain for c in classifications)
    output = TableClassificationOutput(
        data_standard_version=data_standard_version,
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        catalog=catalog.names(),
        classifications=classifications,
        summary={
            "total": len(classifications),
            "by_source": dict(by_source),
            "primary_domain_counts": dict(by_primary.most_common()),
            "with_secondary": sum(1 for c in classifications if c.secondary_domain),
        },
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "data_standard_version": output.data_standard_version,
        "generated_at": output.generated_at,
        "catalog": output.catalog,
        "summary": output.summary,
        "classifications": [asdict(c) for c in output.classifications],
    }, indent=2, sort_keys=True))
    return output


def read_table_mapping(path: Path) -> TableClassificationOutput:
    raw = json.loads(path.read_text(encoding="utf-8"))
    classifications = [TableClassification(**c) for c in raw["classifications"]]
    return TableClassificationOutput(
        data_standard_version=raw["data_standard_version"],
        generated_at=raw["generated_at"],
        catalog=raw["catalog"],
        classifications=classifications,
        summary=raw["summary"],
    )
