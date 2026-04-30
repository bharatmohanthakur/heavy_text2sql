"""Parse the operator-supplied Relationships CSV → list[FKEdge].

Format (one row per FK column-pair — composite FKs span multiple rows
sharing the same FK_Name):

    FK_Name | Parent_Table | Parent_Column | Referenced_Table |
    Referenced_Column | Parent_Schema | Referenced_Schema

Emits FKEdge instances in the same shape graph/fk_parser.py already
defines, so downstream graph builders are unaffected. Composite FKs
are reassembled by grouping rows by FK_Name and preserving the row
order as the column-pair order (the constraint's column ordering
matters for multi-column JOIN ON clauses).

Pivot: Q2. Replaces the regex parser for 0030-ForeignKeys.sql.
"""

from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from typing import Iterable

from text2sql.graph.fk_parser import FKEdge


class RelationshipsCsvError(ValueError):
    """Raised when the Relationships CSV is malformed at the format
    level. Surfaces in the Settings UI with a row number."""


_REQUIRED_COLUMNS = (
    "fk_name",
    "parent_table",
    "parent_column",
    "referenced_table",
    "referenced_column",
    "parent_schema",
    "referenced_schema",
)


def _normalize_header(header: list[str]) -> dict[str, int]:
    idx: dict[str, int] = {}
    for i, name in enumerate(header):
        key = (name or "").strip().lower()
        if key:
            idx[key] = i
    missing = [c for c in _REQUIRED_COLUMNS if c not in idx]
    if missing:
        raise RelationshipsCsvError(
            f"Relationships CSV is missing required column(s): {missing}. "
            f"Expected (case-insensitive): {list(_REQUIRED_COLUMNS)}. "
            f"Got: {header}"
        )
    return idx


def _strip_schema_prefix(raw: str, schema: str) -> str:
    """Drop a redundant 'schema.' prefix from a table-name cell when the
    schema column already carries it. Same rule as schema_csv.py."""
    s = (raw or "").strip()
    if "." in s:
        prefix, _, rest = s.partition(".")
        if prefix == schema:
            return rest
    return s


def parse_relationships_csv(source: str | Path | Iterable[str]) -> list[FKEdge]:
    """Parse the Relationships CSV → list[FKEdge].

    Rows sharing an FK_Name are reassembled into one composite FKEdge
    with the column-pair order preserved from the file. Rows where
    Parent_Table + Referenced_Table change across the same FK_Name
    are an authoring error (a single FK constraint can only span one
    parent → one referenced relationship); we raise rather than guess.

    Raises RelationshipsCsvError on format problems so the Settings UI
    can surface a precise validation message.
    """
    text = _read(source)
    reader = csv.reader(StringIO(text))
    try:
        header = next(reader)
    except StopIteration as e:
        raise RelationshipsCsvError("Relationships CSV is empty") from e
    idx = _normalize_header(header)

    # Group rows by FK_Name, preserving file order both for groups and
    # within groups (so column-pair order in composite FKs matches the
    # CSV).
    groups: dict[str, list[tuple[int, list[str]]]] = {}
    group_order: list[str] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not (c or "").strip() for c in raw):
            continue
        try:
            fk_name = raw[idx["fk_name"]].strip()
        except IndexError as e:
            raise RelationshipsCsvError(
                f"line {line_no}: row has fewer columns than the header"
            ) from e
        if not fk_name:
            raise RelationshipsCsvError(
                f"line {line_no}: FK_Name is required"
            )
        if fk_name not in groups:
            groups[fk_name] = []
            group_order.append(fk_name)
        groups[fk_name].append((line_no, raw))

    edges: list[FKEdge] = []
    for fk_name in group_order:
        edges.append(_assemble_edge(fk_name, groups[fk_name], idx))
    return edges


def _assemble_edge(
    fk_name: str,
    rows: list[tuple[int, list[str]]],
    idx: dict[str, int],
) -> FKEdge:
    """Turn the rows for one FK_Name into a single FKEdge. Rejects
    inconsistent rows (different parent/referenced tables across the
    same constraint name)."""
    src_schema = src_table = dst_schema = dst_table = None
    column_pairs: list[tuple[str, str]] = []
    for line_no, raw in rows:
        try:
            row_src_schema = raw[idx["parent_schema"]].strip()
            row_src_table = _strip_schema_prefix(raw[idx["parent_table"]], row_src_schema)
            row_dst_schema = raw[idx["referenced_schema"]].strip()
            row_dst_table = _strip_schema_prefix(raw[idx["referenced_table"]], row_dst_schema)
            row_src_col = raw[idx["parent_column"]].strip()
            row_dst_col = raw[idx["referenced_column"]].strip()
        except IndexError as e:
            raise RelationshipsCsvError(
                f"line {line_no}: row has fewer columns than the header"
            ) from e
        if not (row_src_schema and row_src_table and row_dst_schema and row_dst_table
                and row_src_col and row_dst_col):
            raise RelationshipsCsvError(
                f"line {line_no}: every column in the Relationships CSV must be non-empty "
                f"(FK_Name={fk_name!r})"
            )
        if src_schema is None:
            src_schema, src_table = row_src_schema, row_src_table
            dst_schema, dst_table = row_dst_schema, row_dst_table
        else:
            if (row_src_schema, row_src_table) != (src_schema, src_table) \
               or (row_dst_schema, row_dst_table) != (dst_schema, dst_table):
                raise RelationshipsCsvError(
                    f"line {line_no}: rows for FK_Name={fk_name!r} disagree on "
                    f"parent or referenced table — expected "
                    f"{src_schema}.{src_table} → {dst_schema}.{dst_table}, "
                    f"got {row_src_schema}.{row_src_table} → {row_dst_schema}.{row_dst_table}"
                )
        column_pairs.append((row_src_col, row_dst_col))
    assert src_schema is not None              # group is never empty here
    return FKEdge(
        src_schema=src_schema,
        src_table=src_table,
        dst_schema=dst_schema or "",
        dst_table=dst_table or "",
        constraint_name=fk_name,
        column_pairs=tuple(column_pairs),
    )


# ── Helpers ─────────────────────────────────────────────────────────────────


def _read(source: str | Path | Iterable[str]) -> str:
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, str) and "\n" not in source and len(source) < 4096:
        try:
            p = Path(source)
            if p.exists():
                return p.read_text(encoding="utf-8")
        except OSError:
            pass
    if isinstance(source, str):
        return source
    return "\n".join(source)
