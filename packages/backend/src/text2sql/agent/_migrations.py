"""Lightweight forward-only migration helper.

The platform never had Alembic, and adding it now would slow boot for
the no-infra demo path. Instead, after `metadata.create_all(engine)`
runs, we walk the SQLAlchemy model and ADD COLUMN any column that the
live table is missing. This handles the only mutation kind we've ever
introduced — adding nullable / defaulted columns to existing tables —
which is what every recent step (O1's `dialect`, N4's
`target_provider` / `dialect` / `source_gold_id`) needs on legacy
metadata DBs.

Scope is intentionally narrow:
  * forward-only — no down-migrations, no version table
  * additive only — never drops, never alters type
  * column-level only — table renames, FKs, or constraint changes
    must be done out of band

If we ever need a destructive change, switch to Alembic; do not
extend this module.
"""

from __future__ import annotations

import logging

import sqlalchemy as sa
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def add_missing_columns(engine: Engine, table: sa.Table) -> list[str]:
    """For each column in `table`, ALTER TABLE ADD COLUMN if it's
    missing from the live DB. Returns the list of column names actually
    added (empty list when the table is already up to date).

    Skips columns that are part of the primary key — `create_all` would
    have created the table fresh if it didn't exist, so a missing PK on
    an existing table would mean the operator is pointing at a
    completely different DB (operator error, not a migration).

    Driver constraints we respect:
      * SQLite: `ALTER TABLE ADD COLUMN` only accepts a NOT NULL clause
        when a non-NULL DEFAULT is also provided. To stay portable we
        always emit the column as nullable here, and rely on the SA
        model's Python-side default to populate new rows. Existing rows
        get NULL (or the empty string for String columns; readers
        already tolerate this — see ConversationRow.dialect comment).
      * MSSQL: `ALTER TABLE … ADD …` (no `COLUMN` keyword). SA's
        DDL compiler emits the right form per dialect, so we go through
        the compiler rather than hand-formatting SQL.
    """
    inspector = sa.inspect(engine)
    if not inspector.has_table(table.name):
        # `create_all` should have just made it; nothing to add.
        return []

    existing = {c["name"] for c in inspector.get_columns(table.name)}
    added: list[str] = []

    for column in table.columns:
        if column.name in existing:
            continue
        if column.primary_key:
            log.warning(
                "table %r is missing primary-key column %r; refusing to ALTER. "
                "Operator should drop and recreate the table or recreate the DB.",
                table.name, column.name,
            )
            continue

        # Always render the new column as nullable: SQLite ALTER TABLE
        # ADD COLUMN can't enforce NOT NULL without a non-null DEFAULT
        # (which SA's type compiler doesn't help with portably). Existing
        # rows would have to take some default anyway, and the SA model's
        # Python-side default populates new rows from the app side.
        type_compiler = engine.dialect.type_compiler_instance
        col_type_sql = type_compiler.process(column.type)
        if engine.dialect.name == "mssql":
            # MSSQL: `ALTER TABLE … ADD …` (no COLUMN keyword), bracketed identifiers
            ddl = sa.text(
                f"ALTER TABLE [{table.name}] ADD [{column.name}] {col_type_sql} NULL"
            )
        else:
            ddl = sa.text(
                f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" {col_type_sql}'
            )

        with engine.begin() as conn:
            conn.execute(ddl)
        added.append(column.name)
        log.info("migrated %s: added column %r (%s)", table.name, column.name, col_type_sql)

    return added
