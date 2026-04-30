"""Build the LLM prompt context.

The prompt's job is to give GPT-4o exactly enough to write correct SQL and
nothing more:

  1. Database dialect (postgresql or mssql).
  2. M-Schema for ONLY the tables Steiner picked — table description +
     column descriptions + a few sample values per column.
  3. Resolved JOIN clauses from the FK graph (no LLM-side join inference).
  4. Resolved entity bindings — "the user said 'Hispanic'; that's the
     CodeValue 'Hispanic' in OldEthnicityDescriptor; bridge through
     edfi.OldEthnicityDescriptor".
  5. 0–3 few-shot NL→SQL pairs from the gold store.
  6. Hard rules (no DDL, return SELECT only, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from text2sql.classification.query_classifier import QueryClassification
from text2sql.embedding.retriever import TableHit
from text2sql.entity_resolution.resolver import EntityCandidate, ResolutionResult
from text2sql.gold.store import GoldHit
from text2sql.graph.steiner import SteinerTree
from text2sql.providers.base import LLMMessage
from text2sql.table_catalog import TableCatalog, TableEntry


# Cap how much per-table detail we pack into a prompt — keeps token usage
# bounded even if Steiner brings in 10+ tables for cross-cutting questions.
DEFAULT_MAX_COLUMNS_PER_TABLE = 30
DEFAULT_MAX_VALUES_PER_COLUMN = 6


@dataclass
class PromptContext:
    nl_question: str
    dialect: str
    domain_routing: list[str]
    selected_tables: list[str]
    m_schema_block: str
    join_clauses: list[str]
    resolved_bindings: list[EntityCandidate]
    few_shots: list[GoldHit]
    rules: list[str] = field(default_factory=list)

    def to_messages(self) -> list[LLMMessage]:
        return [
            LLMMessage(role="system", content=_SYSTEM),
            LLMMessage(role="user", content=self.render_user_prompt()),
        ]

    def render_user_prompt(self) -> str:
        sections: list[str] = []
        sections.append(f"DATABASE DIALECT: {self.dialect}")
        if self.domain_routing:
            sections.append(f"ROUTED DOMAINS: {', '.join(self.domain_routing)}")
        sections.append("")
        sections.append("RELEVANT TABLES (M-Schema):")
        sections.append(self.m_schema_block.rstrip() or "(none)")
        if self.join_clauses:
            sections.append("")
            sections.append("PRE-RESOLVED JOIN PATHS (use these as written):")
            for c in self.join_clauses:
                sections.append(f"  {c}")
        if self.resolved_bindings:
            sections.append("")
            sections.append("RESOLVED ENTITY VALUES (from the user's question):")
            for b in self.resolved_bindings:
                if b.descriptor_type:
                    sections.append(
                        f"  - {b.value!r} is a {b.descriptor_type} CodeValue "
                        f"(descriptor_id={b.descriptor_id}, "
                        f"bridge through {b.child_fqn})"
                    )
                else:
                    sections.append(
                        f"  - {b.value!r} matches column {b.fqn}.{b.column}"
                    )
        if self.few_shots:
            sections.append("")
            sections.append("FEW-SHOT EXAMPLES:")
            for i, h in enumerate(self.few_shots, 1):
                sections.append(f"  Example {i} (similarity={h.score:.2f}):")
                sections.append(f"    Q: {h.record.nl_question}")
                sections.append(f"    A:\n{_indent(h.record.sql_text, '       ')}")
        sections.append("")
        if self.rules:
            sections.append("RULES:")
            for r in self.rules:
                sections.append(f"  - {r}")
            sections.append("")
        sections.append(f"USER QUESTION:\n{self.nl_question}\n")
        sections.append(
            "Return JSON only: "
            '{"sql": "<SELECT statement>", "rationale": "<one short sentence>"}'
        )
        return "\n".join(sections)


_SYSTEM = """\
You translate natural-language questions to SQL against an Ed-Fi-style ODS.

Strict rules — violate them and the SQL will fail:

1. Use ONLY columns that appear under their parent table in the M-Schema
   block. If the column isn't listed under that table, it doesn't exist
   there. Do not rely on prior knowledge of Ed-Fi schemas.

2. The PRE-RESOLVED JOIN PATHS section is the truth about how tables connect.
   Use those JOINs verbatim — schema, table, and column names exactly. Never
   invent FK columns. If you need a column that isn't on a table currently
   in your FROM/JOIN list, find a JOIN path that brings in the table where
   the column actually lives, then select from THAT table.

3. When the user mentions a value the prompt resolved to a descriptor code,
   filter on edfi.descriptor.codevalue, joined through the child descriptor
   table named in the binding.

4. Return a single SELECT (no DDL/DML, no trailing semicolon).

5. Always alias computed columns with AS.
"""


@dataclass
class ContextBuilder:
    catalog: TableCatalog
    dialect: str = "postgresql"
    max_columns_per_table: int = DEFAULT_MAX_COLUMNS_PER_TABLE
    max_values_per_column: int = DEFAULT_MAX_VALUES_PER_COLUMN

    # ── M-Schema rendering ─────────────────────────────────────────────────────

    def m_schema_for(self, fqns: list[str]) -> str:
        by_fqn = self.catalog.by_fqn()
        chunks: list[str] = []
        for fqn in fqns:
            entry = by_fqn.get(fqn)
            if not entry:
                continue
            chunks.append(self._render_one_table(entry))
        return "\n".join(chunks)

    def _render_one_table(self, entry: TableEntry) -> str:
        ident = self._quote_table(entry.schema, entry.table)
        head = f"### {ident}"
        if entry.description:
            head += f"\n# {entry.description}"
        cols = entry.columns[: self.max_columns_per_table]
        col_lines: list[str] = []
        for c in cols:
            qcol = self._quote_id(c.name)
            type_part = f" ({c.data_type})" if c.data_type else ""
            null = "NULL" if c.nullable else "NOT NULL"
            line = f"  - {qcol}{type_part}, {null}"
            if c.is_identifying:
                line += " [PK]"
            if c.description:
                line += f"  -- {c.description}"
            if c.sample_values:
                vals = ", ".join(c.sample_values[: self.max_values_per_column])
                line += f"  [examples: {vals}]"
            col_lines.append(line)
        if len(entry.columns) > self.max_columns_per_table:
            col_lines.append(f"  - … ({len(entry.columns) - self.max_columns_per_table} more)")
        return head + "\n" + "\n".join(col_lines)

    # ── Identifier quoting per dialect ─────────────────────────────────────────

    def _quote_table(self, schema: str, table: str) -> str:
        if self.dialect == "sqlite":
            # SQLite has a single (`main`) schema; the catalog's `edfi.X`
            # FQN doesn't correspond to a real attached schema in the
            # live DB, so drop the prefix to match steiner.to_join_clauses.
            return self._quote_id(table)
        return f"{self._quote_id(schema)}.{self._quote_id(table)}"

    def _quote_id(self, name: str) -> str:
        if self.dialect == "mssql":
            return f"[{name}]"
        if self.dialect == "sqlite":
            # SQLite is case-preserving and (once quoted) case-sensitive.
            # The Ed-Fi table is "Student", not "student".
            return '"' + name + '"'
        # Postgres: identifiers in the populated DB are lowercase; quote-lower
        # to match. For other Postgres deployments the user can override this.
        return '"' + name.lower() + '"'

    # ── Top-level builder ──────────────────────────────────────────────────────

    def build(
        self,
        *,
        nl_question: str,
        domain_routing: QueryClassification,
        retrieved_tables: list[TableHit],
        steiner: SteinerTree,
        resolution: ResolutionResult,
        few_shots: list[GoldHit],
        rules: list[str] | None = None,
    ) -> PromptContext:
        # Selected tables = union of retrieved hits and Steiner pivots
        # (so Steiner-internal bridge tables also get their schema rendered).
        selected: list[str] = []
        seen: set[str] = set()
        for h in retrieved_tables:
            if h.fqn not in seen:
                selected.append(h.fqn)
                seen.add(h.fqn)
        for n in steiner.nodes:
            if n not in seen:
                selected.append(n)
                seen.add(n)

        m_schema_block = self.m_schema_for(selected)
        joins = steiner.to_join_clauses(dialect=self.dialect)

        chosen = [p.chosen for p in resolution.phrases if p.chosen]
        return PromptContext(
            nl_question=nl_question,
            dialect=self.dialect,
            domain_routing=domain_routing.domains,
            selected_tables=selected,
            m_schema_block=m_schema_block,
            join_clauses=joins,
            resolved_bindings=chosen,
            few_shots=few_shots,
            rules=list(rules or _DEFAULT_RULES),
        )


_DEFAULT_RULES = [
    "Return only a SELECT — no INSERT/UPDATE/DELETE/DDL.",
    "Use the pre-resolved JOIN paths exactly. Do not invent FKs.",
    "When filtering on a descriptor, the predicate goes on edfi.descriptor.codevalue.",
    # Result-size cap — pick exactly one based on whether the question is a list
    # or an aggregate. Be deterministic so users see consistent counts across runs.
    "If the question asks for a LIST of rows (not an aggregate / count / sum / "
    "average): cap the result to exactly 50 rows. Use `SELECT TOP 50 ...` for "
    "mssql or append `LIMIT 50` for postgresql / sqlite. Do NOT use `TOP 1000`, "
    "`TOP 100`, or any other cap — always exactly 50.",
    "If the question asks for an AGGREGATE (count / sum / average / min / max / "
    "group-by totals): do NOT add TOP/LIMIT — return all aggregate rows.",
    "Identifiers must be quoted per the declared dialect. For postgresql, "
    "ALL identifiers (schemas, tables, columns) must be lowercase — the "
    "live database stores them lowercased.",
]


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.strip().splitlines())
