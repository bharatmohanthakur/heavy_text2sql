"""In-memory index of every (fqn, column, value) triple from the table catalog.

Loaded once at startup; tiers 1 and 2 query it in microseconds. Tier 3 hits
the FAISS `column_values` collection that Component 5 already built.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from text2sql.table_catalog import TableCatalog


@dataclass(frozen=True)
class ValueRecord:
    fqn: str
    column: str
    value: str
    domains: tuple[str, ...]
    is_descriptor: bool
    # Populated only when the value is a descriptor code; tells Component 8
    # which child descriptor table acts as the bridge in the join chain.
    descriptor_type: str = ""        # e.g. "OldEthnicityDescriptor"
    child_fqn: str = ""              # e.g. "edfi.OldEthnicityDescriptor"
    descriptor_id: int | None = None
    short_description: str = ""


class ValueIndex:
    """All low-cardinality column values from the catalog, indexed for fast
    lookup. Built once; queried by every tier of the resolver."""

    def __init__(self, records: list[ValueRecord]) -> None:
        self._records = records
        # Exact-match: lowercase value -> [records]
        self._by_value_lower: dict[str, list[ValueRecord]] = defaultdict(list)
        # Per-column index: (fqn, column) -> [records] for column-scoped fuzzy
        self._by_column: dict[tuple[str, str], list[ValueRecord]] = defaultdict(list)
        # All values flat for tier-2 fuzzy
        self._all_values: list[str] = []
        for r in records:
            self._by_value_lower[r.value.lower()].append(r)
            self._by_column[(r.fqn, r.column)].append(r)
            self._all_values.append(r.value)

    @property
    def records(self) -> list[ValueRecord]:
        return self._records

    def exact(self, query: str) -> list[ValueRecord]:
        return self._by_value_lower.get(query.lower(), [])

    def all_values(self) -> list[str]:
        return self._all_values

    def by_column(self, fqn: str, column: str) -> list[ValueRecord]:
        return self._by_column.get((fqn, column), [])

    def in_domains(self, domains: list[str]) -> list[ValueRecord]:
        if not domains:
            return self._records
        ds = set(domains)
        return [r for r in self._records if set(r.domains) & ds]

    def __len__(self) -> int:
        return len(self._records)


def build_value_index(catalog: TableCatalog) -> ValueIndex:
    records: list[ValueRecord] = []
    # Regular per-column samples — but skip descriptor child tables (their
    # only column is an opaque numeric ID; useless for entity resolution).
    for entry in catalog.entries:
        if entry.is_descriptor or entry.fqn == "edfi.Descriptor":
            continue
        for col in entry.columns:
            for v in col.sample_values:
                records.append(ValueRecord(
                    fqn=entry.fqn,
                    column=col.name,
                    value=v,
                    domains=tuple(entry.domains),
                    is_descriptor=False,
                ))
    # Descriptor codes from edfi.Descriptor, fanned out with full type metadata.
    for d in catalog.descriptor_codes:
        if not d.code_value:
            continue
        records.append(ValueRecord(
            fqn="edfi.Descriptor",
            column="CodeValue",
            value=d.code_value,
            domains=(),                         # descriptors are cross-domain by nature
            is_descriptor=True,
            descriptor_type=d.type_name,
            child_fqn=d.child_fqn,
            descriptor_id=d.descriptor_id,
            short_description=d.short_description,
        ))
        # Also index ShortDescription when it adds new info (often it's a
        # human-friendly variant of the codevalue).
        if d.short_description and d.short_description.lower() != d.code_value.lower():
            records.append(ValueRecord(
                fqn="edfi.Descriptor",
                column="ShortDescription",
                value=d.short_description,
                domains=(),
                is_descriptor=True,
                descriptor_type=d.type_name,
                child_fqn=d.child_fqn,
                descriptor_id=d.descriptor_id,
                short_description=d.short_description,
            ))
    return ValueIndex(records)
