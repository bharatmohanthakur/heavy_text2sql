"""SQLite SQL engine adapter — file-backed, zero-infra option.

Pick this when you want to demo / CI / prototype against a small Ed-Fi
schema without standing up MSSQL or Postgres. SQLite has no host, port,
user, password, or schema — only a filesystem path. The engine opens the
DB read-only by default so the agent loop can never mutate a hand-curated
sample.

Repo-relative paths resolve against `REPO_ROOT` so the same overlay file
works for `make demo` (which lives at the repo root) and CI (which runs
from a checkout). Absolute paths pass through unchanged.

We deliberately reject `:memory:` paths through the connector form — an
in-process database isn't shareable across requests so it'd silently
appear empty after the first connection. In-process tests instantiate
`SqliteEngine` directly with `:memory:` and that path stays open.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from text2sql.config import REPO_ROOT, ProviderEntry
from text2sql.providers.base import SqlEngine
from text2sql.providers.factory import register_sql_engine


class SqliteEngine(SqlEngine):
    """SQLite-via-SQLAlchemy. Single schema (`main`) — `list_tables()`
    returns `("main", name)` so callers that expect a (schema, table)
    tuple work unchanged."""

    SCHEMA_NAME = "main"

    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        raw_path: str = str(cfg.get("path") or "").strip()
        if not raw_path:
            raise ValueError("sqlite provider requires a `path` (filesystem location)")
        self._read_only: bool = bool(cfg.get("read_only", True))

        if raw_path == ":memory:":
            # In-process DB — useful for tests but not for the connector
            # form (each request would see an empty DB). Allowed because
            # we may instantiate the engine directly in tests.
            self._path = ":memory:"
            url = sa.URL.create(drivername="sqlite", database=":memory:")
        else:
            # Resolve repo-relative paths so the same overlay works in CI
            # and dev. Absolute paths pass through unchanged.
            p = Path(raw_path)
            if not p.is_absolute():
                p = REPO_ROOT / p
            self._path = str(p)
            mode = "ro" if self._read_only else "rwc"
            uri = f"file:{self._path}?mode={mode}"
            url = sa.URL.create(drivername="sqlite", database=uri,
                                query={"uri": "true"})

        self._engine: Engine = sa.create_engine(url, future=True)

    @property
    def dialect(self) -> str:
        # sqlglot dialect name — matches what validator/generator pass through.
        return "sqlite"

    @property
    def path(self) -> str:
        return self._path

    @property
    def read_only(self) -> bool:
        return self._read_only

    def execute(
        self, sql: str, *, params: dict[str, Any] | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            cur = conn.execute(sa.text(sql), params or {})
            rows = cur.mappings().all() if cur.returns_rows else []
        if limit is not None:
            rows = rows[:limit]
        return [dict(r) for r in rows]

    def explain(self, sql: str) -> str:
        # SQLite has both EXPLAIN (VDBE bytecode) and EXPLAIN QUERY PLAN
        # (human-readable). The latter is what we want for the validator's
        # "did this parse + plan cleanly" check.
        with self._engine.connect() as conn:
            cur = conn.execute(sa.text(f"EXPLAIN QUERY PLAN {sql}"))
            return "\n".join(" | ".join(str(c) for c in row) for row in cur)

    def list_tables(self) -> list[tuple[str, str]]:
        # `sqlite_master` has every persistent object; filter to user tables
        # (exclude `sqlite_%` system tables and views).
        sql = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        with self._engine.connect() as conn:
            return [(self.SCHEMA_NAME, r[0]) for r in conn.execute(sa.text(sql))]

    def list_columns(self, schema: str, table: str) -> list[tuple[str, str, bool]]:
        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk).
        # Type is the declared type string verbatim — SQLite is dynamically
        # typed, so an empty type is valid (we surface it as ""). nullable
        # is the inverse of notnull.
        # PRAGMA can't be parameterized, so quote the identifier defensively.
        safe = table.replace('"', '""')
        sql = f'PRAGMA table_info("{safe}")'
        with self._engine.connect() as conn:
            return [
                (r[1], (r[2] or ""), not bool(r[3]))
                for r in conn.execute(sa.text(sql))
            ]

    def list_foreign_keys(self, table: str) -> list[dict[str, Any]]:
        """Optional helper not on the Protocol — used by the catalog when
        importing a SQLite DB without a precomputed FK file. Returns
        `[{id, parent, child, parent_col, child_col, on_delete, on_update}, ...]`,
        one row per FK column (composite FKs share the same `id`)."""
        safe = table.replace('"', '""')
        sql = f'PRAGMA foreign_key_list("{safe}")'
        with self._engine.connect() as conn:
            return [
                {
                    "id": r[0], "seq": r[1],
                    "parent": r[2],   # referenced table
                    "child": table,
                    "child_col": r[3],
                    "parent_col": r[4],
                    "on_update": r[5], "on_delete": r[6],
                }
                for r in conn.execute(sa.text(sql))
            ]

    def quote_identifier(self, name: str) -> str:
        # SQLite accepts both "double" and [bracket] quoting; double-quote
        # is the SQL standard and matches Postgres, which keeps the
        # generator's prompt identical for both.
        return '"' + name.replace('"', '""') + '"'


@register_sql_engine("sqlite")
def _build(spec: ProviderEntry) -> SqliteEngine:
    return SqliteEngine(spec)
