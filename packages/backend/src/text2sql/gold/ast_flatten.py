"""Flatten a SQL string into a structural fingerprint for embedding.

The fingerprint preserves shape — table names, columns, join structure,
aggregations — but discards literals. Two queries that compute the same
thing with different filter values map to the same neighborhood, which is
what we want for few-shot retrieval.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp


def flatten_sql_ast(sql: str, dialect: str = "postgres") -> str:
    """Return a deterministic, literal-free representation of the SQL's AST.

    Tables, columns, join keys, aggregations, GROUP/ORDER BY clauses survive.
    String/number/date literals are replaced with their type tag.
    """
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return sql.strip().lower()

    bits: list[str] = []

    for node in tree.walk():
        e = node[0] if isinstance(node, tuple) else node
        if isinstance(e, exp.Table):
            db = e.args.get("db")
            schema = (db.name if db else "")
            name = e.name
            bits.append(f"T:{schema}.{name}" if schema else f"T:{name}")
        elif isinstance(e, exp.Column):
            tbl = e.args.get("table")
            col = e.name
            bits.append(f"C:{tbl.name + '.' if tbl else ''}{col}")
        elif isinstance(e, exp.Func):
            bits.append(f"F:{e.sql_name().lower()}")
        elif isinstance(e, exp.Join):
            kind = e.args.get("kind") or "JOIN"
            bits.append(f"J:{kind.lower()}")
        elif isinstance(e, (exp.Literal, exp.Boolean, exp.Null)):
            bits.append(f"L:{type(e).__name__.lower()}")
        elif isinstance(e, exp.Group):
            bits.append("GROUP")
        elif isinstance(e, exp.Order):
            bits.append("ORDER")
        elif isinstance(e, exp.Limit):
            bits.append("LIMIT")
        elif isinstance(e, exp.Where):
            bits.append("WHERE")
        elif isinstance(e, exp.Having):
            bits.append("HAVING")

    return " ".join(bits)
