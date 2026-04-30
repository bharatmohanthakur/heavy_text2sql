"""Parse the operator-supplied Schema CSV.

Format (one row per (schema, table, column) — operator-authored):

    Ranking | Domain | TABLE_SCHEMA | TABLE_NAME | COLUMN_NAME | Populated

Semantics:
  * Ranking      — integer; smaller = more important domain (used as
                   the routing-priority signal). 0 is allowed and means
                   "primary domain for this column's table".
  * Domain       — free-form string. The set of distinct values across
                   the file IS the domain taxonomy; we never hard-code
                   a 35-domain list anymore.
  * TABLE_SCHEMA — DB-level schema (e.g. "edfi", "tpdm"). Authoritative.
  * TABLE_NAME   — bare table name. If the cell looks like
                   "edfi.MyTable" we strip the schema prefix when it
                   matches TABLE_SCHEMA — operators sometimes export it
                   that way.
  * COLUMN_NAME  — bare column name.
  * Populated    — yes/no/true/false/1/0; whether the column carries
                   useful data in the target DB. Drives whether we
                   sample distinct values for entity resolution.

This file replaces the ApiModel.json parser (`classification/metadata.py`
upstream side) and is the only allowed source for the catalog's
table → domain → column mapping. Pivot: Q1.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable


class SchemaCsvError(ValueError):
    """Raised when the Schema CSV is malformed at the format level —
    missing required columns, non-integer Ranking, etc. Operators see
    this in the Settings UI as a validation failure with a row number.
    """


@dataclass(frozen=True, slots=True)
class ColumnRow:
    """One row of the Schema CSV. Frozen so it's safe to use as a dict
    key when grouping by (schema, table) downstream."""

    ranking: int
    domain: str
    schema: str
    table: str
    column: str
    populated: bool

    @property
    def fqn(self) -> str:
        """`schema.table` — matches FKEdge.src_fqn/dst_fqn convention so
        the catalog builder can hash-join across the two CSVs."""
        return f"{self.schema}.{self.table}"


# ── Header handling ─────────────────────────────────────────────────────────

# Required columns, normalized to lowercase. Operators tend to ship
# the Excel-flavored header casing (TABLE_SCHEMA, TABLE_NAME) so we
# normalize before lookup rather than fighting over case.
_REQUIRED_COLUMNS = (
    "ranking",
    "domain",
    "table_schema",
    "table_name",
    "column_name",
    "populated",
)


def _normalize_header(header: list[str]) -> dict[str, int]:
    """Map normalized column-name → 0-based column index. Raises
    SchemaCsvError if any required column is absent."""
    idx: dict[str, int] = {}
    for i, name in enumerate(header):
        key = (name or "").strip().lower()
        if key:
            idx[key] = i
    missing = [c for c in _REQUIRED_COLUMNS if c not in idx]
    if missing:
        raise SchemaCsvError(
            f"Schema CSV is missing required column(s): {missing}. "
            f"Expected (case-insensitive): {list(_REQUIRED_COLUMNS)}. "
            f"Got: {header}"
        )
    return idx


def _parse_populated(raw: str) -> bool:
    """Yes/No/True/False/1/0/Y/N — be liberal in what we accept since
    the operator authors this by hand."""
    s = (raw or "").strip().lower()
    if s in ("yes", "y", "true", "1", "t"):
        return True
    if s in ("no", "n", "false", "0", "f", ""):
        return False
    raise SchemaCsvError(f"unrecognized Populated value: {raw!r} "
                         "(expected yes/no/true/false/1/0)")


def _strip_schema_prefix(raw: str, schema: str) -> str:
    """If the operator wrote 'edfi.MyTable' in TABLE_NAME and 'edfi' in
    TABLE_SCHEMA, drop the redundant prefix. Leave foreign prefixes
    alone — those signal a real bug we shouldn't paper over."""
    s = (raw or "").strip()
    if "." in s:
        prefix, _, rest = s.partition(".")
        if prefix == schema:
            return rest
    return s


# ── Public entry points ─────────────────────────────────────────────────────


def parse_schema_csv(source: str | Path | Iterable[str]) -> list[ColumnRow]:
    """Parse the Schema CSV from a path, raw text, or an iterable of
    lines. Returns rows in file order — callers that need them grouped
    by table should use `group_by_table()`.

    Raises SchemaCsvError on format problems (missing columns, bad
    Populated value, non-integer Ranking) so the Settings UI can echo
    a precise message back to the operator.
    """
    text = _read(source)
    reader = csv.reader(StringIO(text))
    try:
        header = next(reader)
    except StopIteration as e:
        raise SchemaCsvError("Schema CSV is empty") from e
    idx = _normalize_header(header)

    rows: list[ColumnRow] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not (c or "").strip() for c in raw):
            continue                              # tolerate blank rows
        try:
            ranking_raw = raw[idx["ranking"]].strip()
            ranking = int(ranking_raw) if ranking_raw else 0
        except (ValueError, IndexError) as e:
            raise SchemaCsvError(
                f"line {line_no}: Ranking must be an integer, got {raw[idx['ranking']]!r}"
            ) from e
        try:
            domain = raw[idx["domain"]].strip()
            schema = raw[idx["table_schema"]].strip()
            table = _strip_schema_prefix(raw[idx["table_name"]], schema)
            column = raw[idx["column_name"]].strip()
            populated = _parse_populated(raw[idx["populated"]])
        except IndexError as e:
            raise SchemaCsvError(
                f"line {line_no}: row has fewer columns than the header"
            ) from e
        if not (schema and table and column):
            raise SchemaCsvError(
                f"line {line_no}: TABLE_SCHEMA, TABLE_NAME, and COLUMN_NAME "
                f"are all required (got {schema!r}, {table!r}, {column!r})"
            )
        rows.append(ColumnRow(
            ranking=ranking, domain=domain, schema=schema,
            table=table, column=column, populated=populated,
        ))
    return rows


def group_by_table(rows: list[ColumnRow]) -> dict[str, list[ColumnRow]]:
    """Bucket rows by `schema.table` fqn. Order is preserved within
    each bucket — the catalog builder surfaces columns in the order
    the operator listed them."""
    out: dict[str, list[ColumnRow]] = {}
    for r in rows:
        out.setdefault(r.fqn, []).append(r)
    return out


def distinct_domains(rows: list[ColumnRow]) -> list[str]:
    """Domain taxonomy = set of distinct non-empty Domain values, sorted
    by their first-seen ranking (so the operator's intended priority
    survives). This replaces the hard-coded 35-domain list."""
    seen: dict[str, int] = {}
    for r in rows:
        if not r.domain:
            continue
        if r.domain not in seen or r.ranking < seen[r.domain]:
            seen[r.domain] = r.ranking
    return sorted(seen.keys(), key=lambda d: (seen[d], d))


# ── Helpers ─────────────────────────────────────────────────────────────────


def _read(source: str | Path | Iterable[str]) -> str:
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, str) and "\n" not in source and len(source) < 4096:
        # Looks like a path — but only treat it as one if it actually exists,
        # else fall through to the "raw text" branch.
        try:
            p = Path(source)
            if p.exists():
                return p.read_text(encoding="utf-8")
        except OSError:
            pass
    if isinstance(source, str):
        return source
    return "\n".join(source)
