"""MSSQL / Azure SQL adapter.

Default driver is `pymssql` (pure-FreeTDS, easy to install on macOS); falls
back to `pyodbc` + the official MS ODBC Driver 18 when configured.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from text2sql.config import ProviderEntry
from text2sql.providers.base import SqlEngine
from text2sql.providers.factory import _resolve_secret, register_sql_engine


class MSSqlEngine(SqlEngine):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        password = _resolve_secret(cfg["password_env"]) if cfg.get("password_env") else cfg.get("password", "")
        host = cfg["host"]
        port = int(cfg.get("port") or 1433)
        driver_choice = (cfg.get("python_driver") or "pymssql").lower()

        if driver_choice == "pymssql":
            url = sa.URL.create(
                drivername="mssql+pymssql",
                username=cfg["user"],
                password=password,
                host=host,
                port=port,
                database=cfg["database"],
                query={"tds_version": cfg.get("tds_version", "7.4")},
            )
        else:
            import urllib.parse
            host_port = host if "," in str(host) else f"{host},{port}"
            odbc = (
                f"DRIVER={{{cfg.get('driver', 'ODBC Driver 18 for SQL Server')}}};"
                f"SERVER={host_port};"
                f"DATABASE={cfg['database']};"
                f"UID={cfg['user']};"
                f"PWD={password};"
            )
            if cfg.get("trust_server_certificate", True):
                odbc += "TrustServerCertificate=yes;"
            if cfg.get("encrypt") is False:
                odbc += "Encrypt=no;"
            url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc)}"

        self._engine: Engine = sa.create_engine(url, pool_pre_ping=True, future=True)

    @property
    def dialect(self) -> str:
        return "mssql"

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
        # MSSQL doesn't have EXPLAIN. SHOWPLAN_TEXT runs the query optimizer
        # and rejects un-runnable SQL, which is exactly the validate-only
        # contract we need.
        with self._engine.connect() as conn:
            conn.exec_driver_sql("SET SHOWPLAN_TEXT ON")
            try:
                cur = conn.execute(sa.text(sql))
                return "\n".join(str(row[0]) for row in cur)
            finally:
                conn.exec_driver_sql("SET SHOWPLAN_TEXT OFF")

    def list_tables(self) -> list[tuple[str, str]]:
        sql = (
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE='BASE TABLE' "
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
        )
        with self._engine.connect() as conn:
            return [(r[0], r[1]) for r in conn.execute(sa.text(sql))]

    def list_columns(self, schema: str, table: str) -> list[tuple[str, str, bool]]:
        sql = (
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
            "ORDER BY ORDINAL_POSITION"
        )
        with self._engine.connect() as conn:
            cur = conn.execute(sa.text(sql), {"schema": schema, "table": table})
            return [(r[0], r[1], (r[2] or "").upper() == "YES") for r in cur]

    def quote_identifier(self, name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"


@register_sql_engine("mssql")
def _build(spec: ProviderEntry) -> MSSqlEngine:
    return MSSqlEngine(spec)
