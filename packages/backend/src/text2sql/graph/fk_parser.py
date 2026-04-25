"""Parse 0030-ForeignKeys.sql into a list of FKEdge records.

Format (T-SQL, MSSQL flavor):

    ALTER TABLE [schema].[table] WITH CHECK ADD CONSTRAINT [FK_name]
        FOREIGN KEY ([col1], [col2], ...)
    REFERENCES [schema].[table] ([col1], [col2], ...)
    [ON DELETE CASCADE]
    GO

Statements are GO-terminated. Composite FKs span multiple columns; the parsed
edge captures the ordered (source_col, target_col) pairs so JOIN expansion can
reproduce the multi-column ON clause.

We accept the same statement shape from a Postgres dump (sqlglot-tolerant)
without changes — Ed-Fi's PgSql dialect uses bare identifiers but otherwise
the same structure.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

# Bracketed [name] or quoted "name" or bare name
_IDENT = r"(?:\[(?P<{0}_b>[^\]]+)\]|\"(?P<{0}_q>[^\"]+)\"|(?P<{0}_n>[A-Za-z_][\w]*))"


def _ident(group_prefix: str) -> str:
    return _IDENT.format(group_prefix)


_FK_PATTERN = re.compile(
    r"ALTER\s+TABLE\s+"
    + _ident("src_schema") + r"\s*\.\s*" + _ident("src_table")
    + r"\s+WITH\s+(?:CHECK|NOCHECK)?\s*ADD\s+CONSTRAINT\s+"
    + _ident("constraint")
    + r"\s+FOREIGN\s+KEY\s*\(\s*(?P<src_cols>[^\)]+)\s*\)\s+"
    + r"REFERENCES\s+"
    + _ident("dst_schema") + r"\s*\.\s*" + _ident("dst_table")
    + r"\s*\(\s*(?P<dst_cols>[^\)]+)\s*\)",
    re.IGNORECASE | re.DOTALL,
)


def _ident_value(m: re.Match[str], prefix: str) -> str:
    return m.group(f"{prefix}_b") or m.group(f"{prefix}_q") or m.group(f"{prefix}_n") or ""


def _split_cols(raw: str) -> tuple[str, ...]:
    parts = re.split(r",\s*", raw.strip())
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if p.startswith("[") and p.endswith("]"):
            p = p[1:-1]
        elif p.startswith('"') and p.endswith('"'):
            p = p[1:-1]
        if p:
            out.append(p)
    return tuple(out)


@dataclass(frozen=True)
class FKEdge:
    """One foreign-key constraint, possibly composite."""
    src_schema: str
    src_table: str
    dst_schema: str
    dst_table: str
    constraint_name: str
    column_pairs: tuple[tuple[str, str], ...]   # ordered (src_col, dst_col)

    @property
    def src_fqn(self) -> str:
        return f"{self.src_schema}.{self.src_table}"

    @property
    def dst_fqn(self) -> str:
        return f"{self.dst_schema}.{self.dst_table}"

    @property
    def is_composite(self) -> bool:
        return len(self.column_pairs) > 1


def parse_fks(sql_text_or_path: str | Path) -> list[FKEdge]:
    """Parse a 0030-ForeignKeys.sql file or text into FKEdge records.

    Accepts a Path (read from disk) or raw SQL text. Statements are split on
    GO; each statement runs through the regex once. Constraints with no
    matching columns (e.g. ON DELETE-only DDL) are silently dropped.
    """
    if isinstance(sql_text_or_path, Path):
        text = sql_text_or_path.read_text()
    else:
        text = sql_text_or_path

    # Strip line comments
    text = re.sub(r"--[^\n]*\n", "\n", text)
    # Drop CREATE INDEX et al — we only care about FK constraints
    statements = [s.strip() for s in re.split(r"\n\s*GO\s*\n", text) if s.strip()]

    edges: list[FKEdge] = []
    for stmt in statements:
        m = _FK_PATTERN.search(stmt)
        if not m:
            continue
        src_cols = _split_cols(m.group("src_cols"))
        dst_cols = _split_cols(m.group("dst_cols"))
        if len(src_cols) != len(dst_cols) or not src_cols:
            continue
        edges.append(FKEdge(
            src_schema=_ident_value(m, "src_schema"),
            src_table=_ident_value(m, "src_table"),
            dst_schema=_ident_value(m, "dst_schema"),
            dst_table=_ident_value(m, "dst_table"),
            constraint_name=_ident_value(m, "constraint"),
            column_pairs=tuple(zip(src_cols, dst_cols)),
        ))
    return edges
