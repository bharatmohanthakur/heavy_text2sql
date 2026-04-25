"""Validation + repair loop for generated SQL.

When the LLM's first SQL fails (parse error, unknown column, syntax error,
or EXPLAIN rejects it), feed the failing SQL + the engine's error back to
the LLM and ask for a corrected SELECT. Retry until either:

  * SQL passes parse + EXPLAIN, or
  * `max_attempts` is exhausted.

The repair prompt re-uses the original PromptContext (M-Schema + JOIN paths
+ resolved bindings + few-shots) so the LLM keeps the same grounding;
only the user message gets the failure breadcrumb.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

import sqlglot

from text2sql.pipeline.context import PromptContext
from text2sql.providers.base import LLMMessage, LLMProvider, SqlEngine

log = logging.getLogger(__name__)


@dataclass
class RepairAttempt:
    sql: str
    rationale: str
    error: str | None
    accepted: bool


@dataclass
class RepairResult:
    final_sql: str
    final_rationale: str
    accepted: bool
    attempts: list[RepairAttempt] = field(default_factory=list)


_REPAIR_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "sql": {"type": "string"},
        "rationale": {"type": "string"},
    },
    "required": ["sql", "rationale"],
}


_REPAIR_SYSTEM = """\
The previous SQL you generated FAILED. The engine returned the error verbatim
below. Repair the SQL to fix that exact error while staying inside the
M-Schema + JOIN paths from the original context.

Rules — same as before:
  - Use ONLY columns shown in the M-Schema. If a column is missing on the
    table you tried, find the table where it actually lives (use the JOIN
    paths to bring it into scope) and select from THAT table.
  - Use the pre-resolved JOIN paths verbatim.
  - Return a single SELECT (no DDL/DML, no trailing semicolon).
  - Reply JSON only: {"sql": "...", "rationale": "<one short sentence>"}
"""


def validate_sql(
    sql: str, sql_engine: SqlEngine, *, dialect: str | None = None
) -> str | None:
    """Return None if SQL is valid; otherwise an error string."""
    if not sql or not sql.strip():
        return "empty SQL"
    sqlglot_dialect = {"postgresql": "postgres", "mssql": "tsql"}.get(
        dialect or sql_engine.dialect, dialect or sql_engine.dialect,
    )
    try:
        tree = sqlglot.parse_one(sql, read=sqlglot_dialect)
    except Exception as e:
        return f"parse error: {e}"
    if (
        tree.find(sqlglot.exp.Update) is not None
        or tree.find(sqlglot.exp.Insert) is not None
        or tree.find(sqlglot.exp.Delete) is not None
    ):
        return "non-SELECT statement rejected"
    try:
        sql_engine.explain(sql)
    except Exception as e:
        return f"explain error: {e}"
    return None


class RepairLoop:
    def __init__(
        self,
        llm: LLMProvider,
        sql_engine: SqlEngine,
        *,
        max_attempts: int = 3,
        dialect: str | None = None,
    ) -> None:
        self._llm = llm
        self._sql_engine = sql_engine
        self._max_attempts = max(1, max_attempts)
        self._dialect = dialect or sql_engine.dialect

    def run(
        self,
        prompt: PromptContext,
        initial_sql: str,
        initial_rationale: str = "",
    ) -> RepairResult:
        attempts: list[RepairAttempt] = []
        sql = initial_sql
        rationale = initial_rationale
        last_err: str | None = None

        for i in range(self._max_attempts):
            err = validate_sql(sql, self._sql_engine, dialect=self._dialect)
            attempt = RepairAttempt(sql=sql, rationale=rationale, error=err, accepted=err is None)
            attempts.append(attempt)
            if err is None:
                return RepairResult(
                    final_sql=sql,
                    final_rationale=rationale,
                    accepted=True,
                    attempts=attempts,
                )
            last_err = err
            if i + 1 >= self._max_attempts:
                break
            # Ask the LLM for a repair
            sql, rationale = self._ask_repair(prompt, sql, err)

        return RepairResult(
            final_sql=sql,
            final_rationale=rationale or last_err or "",
            accepted=False,
            attempts=attempts,
        )

    def _ask_repair(
        self, prompt: PromptContext, broken_sql: str, error: str
    ) -> tuple[str, str]:
        repair_user = (
            f"{prompt.render_user_prompt()}\n\n"
            f"--- PREVIOUS ATTEMPT ---\n"
            f"{broken_sql}\n\n"
            f"--- ENGINE ERROR ---\n"
            f"{error}\n"
        )
        # Network / API errors are caught — they mean we couldn't reach the LLM
        # at all. JSON parse errors are NOT caught; the provider's strict
        # json_schema mode guarantees parseable output, so a failure there is a
        # real bug we want to see.
        try:
            raw = self._llm.complete(
                [
                    LLMMessage(role="system", content=_REPAIR_SYSTEM),
                    LLMMessage(role="user", content=repair_user),
                ],
                schema=_REPAIR_SCHEMA,
                temperature=0.0,
                max_tokens=1200,
            )
        except Exception as e:
            log.warning("repair LLM call failed: %s", e)
            return broken_sql, f"repair LLM call failed: {e}"
        payload = json.loads(raw)
        return payload["sql"].strip(), payload.get("rationale", "")
