"""Postgres SQL engine adapter (default dev/test target — Ed-Fi populated image)."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from text2sql.config import ProviderEntry
from text2sql.providers.base import SqlEngine
from text2sql.providers.factory import _resolve_secret, register_sql_engine


class PostgresEngine(SqlEngine):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        password = _resolve_secret(cfg["password_env"]) if cfg.get("password_env") else cfg.get("password", "")
        url = sa.URL.create(
            drivername="postgresql+psycopg",
            username=cfg["user"],
            password=password,
            host=cfg["host"],
            port=int(cfg["port"]),
            database=cfg["database"],
        )
        self._engine: Engine = sa.create_engine(url, pool_pre_ping=True, future=True)
        self._search_path: list[str] = list(cfg.get("schema_search_path", ["edfi"]))

    @property
    def dialect(self) -> str:
        return "postgresql"

    def execute(
        self, sql: str, *, params: dict[str, Any] | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        bound = sa.text(sql)
        with self._engine.connect() as conn:
            if self._search_path:
                conn.exec_driver_sql(f'SET search_path TO {", ".join(self._search_path)}')
            cur = conn.execute(bound, params or {})
            rows = cur.mappings().all() if cur.returns_rows else []
        if limit is not None:
            rows = rows[:limit]
        return [dict(r) for r in rows]

    def explain(self, sql: str) -> str:
        with self._engine.connect() as conn:
            cur = conn.execute(sa.text(f"EXPLAIN {sql}"))
            return "\n".join(row[0] for row in cur)

    def list_tables(self) -> list[tuple[str, str]]:
        sql = (
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_type='BASE TABLE' "
            "AND table_schema NOT IN ('pg_catalog','information_schema') "
            "ORDER BY table_schema, table_name"
        )
        with self._engine.connect() as conn:
            return [(r[0], r[1]) for r in conn.execute(sa.text(sql))]

    def list_columns(self, schema: str, table: str) -> list[tuple[str, str, bool]]:
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "ORDER BY ordinal_position"
        )
        with self._engine.connect() as conn:
            cur = conn.execute(
                sa.text(sql),
                {"schema": schema.lower(), "table": table.lower()},
            )
            return [
                (r[0], r[1], (r[2] or "").upper() == "YES")
                for r in cur
            ]

    def quote_identifier(self, name: str) -> str:
        # Ed-Fi's Postgres installer creates lowercase identifiers; quoting
        # CamelCase as-is would break case-sensitive Postgres lookups.
        return '"' + name.replace('"', '""').lower() + '"'


@register_sql_engine("postgresql")
def _build(spec: ProviderEntry) -> PostgresEngine:
    return PostgresEngine(spec)
