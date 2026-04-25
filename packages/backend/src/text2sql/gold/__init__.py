"""Component 7: Gold SQL store.

Persists validated NL → SQL pairs in the metadata Postgres DB. Drives the
few-shot examples that Component 8 (NL→SQL pipeline) feeds the LLM.

Public API:
  GoldStore(metadata_engine, embedding_provider)
    .ensure_schema()
    .create(nl, sql, tables_used, ...)
    .approve(id, reviewer)
    .reject(id, reviewer, reason)
    .retrieve_top_k(nl, *, domains=None, k=3)  → top approved gold examples
    .list(...)
    .get(id)

Each row carries:
  * nl_question  + nl_embedding
  * sql_text     + sql_ast_embedding
  * tables_used  + domains_used (derived from tables_used + table_catalog)
  * approval_status (pending | approved | rejected)
  * exec_check_passed (set by validate())
"""

from text2sql.gold.ast_flatten import flatten_sql_ast
from text2sql.gold.schema import GoldRecord
from text2sql.gold.store import GoldStore

__all__ = ["GoldRecord", "GoldStore", "flatten_sql_ast"]
