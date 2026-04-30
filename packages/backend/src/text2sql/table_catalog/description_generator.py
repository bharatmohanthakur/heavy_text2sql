"""LLM-driven gap-filler for column descriptions.

ApiModel.json already has table descriptions for 100% of tables and column
descriptions for ~60% of columns. We only call the LLM to fill the remaining
40% of column descriptions where Ed-Fi left them empty (mostly descriptor
foreign-key columns and id columns) AND we have sample data to work with.

The same generator can fill a table description when it's missing (rare —
about 0% in DS 6.1.0 but useful for non-Ed-Fi schemas).

Cached by hash(schema+table+missing-cols+samples) so re-runs are free.
"""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from text2sql.providers.base import LLMMessage, LLMProvider

log = logging.getLogger(__name__)


@dataclass
class GeneratedDescriptions:
    table_description: str            # may be empty if not requested
    column_descriptions: dict[str, str]
    source: str                       # "llm" | "cache" | "fallback"


@dataclass
class TableSampleData:
    schema: str
    table: str
    apimodel_table_description: str = ""
    columns: list[dict] = field(default_factory=list)
    # column dict shape: {name, data_type, nullable, samples, has_apimodel_desc, distinct_count}
    sample_rows: list[dict] = field(default_factory=list)
    row_count: int | None = None
    request_table_desc: bool = False        # only fill if missing
    columns_to_describe: list[str] = field(default_factory=list)

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"


_SYSTEM = """\
You write concise database documentation. You are filling specific gaps in
existing documentation — not rewriting it.

For each column the user asks about, produce a one-line description: what
the column holds, in domain terms. Be factual; if a column is an opaque
numeric ID, say so plainly. Do not invent business meaning the data does
not support.

If asked for a table description, write 1–2 sentences about what the table
represents.

Return JSON only.
"""


def _build_response_schema(column_names: list[str], include_table: bool) -> dict[str, Any]:
    props: dict[str, Any] = {}
    required: list[str] = []
    if include_table:
        props["table_description"] = {"type": "string"}
        required.append("table_description")
    if column_names:
        props["column_descriptions"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {c: {"type": "string"} for c in column_names},
            "required": column_names,
        }
        required.append("column_descriptions")
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": props,
        "required": required,
    }


def _hash_key(s: TableSampleData) -> str:
    payload = {
        "fqn": s.fqn,
        "wanted_table_desc": s.request_table_desc,
        "wanted_cols": sorted(s.columns_to_describe),
        "cols": [(c["name"], c.get("data_type"), c.get("samples")) for c in s.columns],
        "rows": s.sample_rows[:5],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def _format_prompt(s: TableSampleData) -> str:
    lines = [f"TABLE: {s.fqn}"]
    if s.apimodel_table_description and not s.request_table_desc:
        # When we don't need a new table description, share the existing one
        # as context — helps the LLM stay on-domain.
        lines.append(f"TABLE PURPOSE (already known): {s.apimodel_table_description}")
    lines.append(f"ROW COUNT: {s.row_count if s.row_count is not None else 'unknown'}")
    lines.append("")
    lines.append("COLUMNS (full list, for context):")
    for c in s.columns:
        samples = c.get("samples") or []
        nullable = "nullable" if c.get("nullable") else "not null"
        sample_str = (
            "  e.g. " + ", ".join(repr(v) for v in samples[:6])
            if samples else "  (no sample values observed)"
        )
        lines.append(f"  - {c['name']}  ({c.get('data_type','?')}, {nullable})")
        lines.append(sample_str)
    lines.append("")
    if s.sample_rows:
        lines.append(f"SAMPLE ROWS (up to {len(s.sample_rows)}):")
        for r in s.sample_rows[:5]:
            compact = ", ".join(f"{k}={v!r}" for k, v in r.items())
            lines.append(f"  {{{compact}}}")
    else:
        lines.append("SAMPLE ROWS: (none — table empty or unreadable)")
    lines.append("")
    if s.request_table_desc:
        lines.append("Please provide:")
        lines.append("  - table_description: 1-2 sentences on what the table represents.")
    if s.columns_to_describe:
        lines.append(
            f"Provide column_descriptions for ONLY these columns "
            f"(others already documented): {', '.join(s.columns_to_describe)}"
        )
    return "\n".join(lines)


class _Cache:
    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._data: dict[str, dict[str, Any]] = {}
        if path and path.exists():
            try:
                self._data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}

    def get(self, key: str) -> dict[str, Any] | None:
        return self._data.get(key)

    def put(self, key: str, value: dict[str, Any]) -> None:
        self._data[key] = value
        self._flush()

    def _flush(self) -> None:
        if not self._path:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, sort_keys=True), encoding="utf-8")


class DescriptionGenerator:
    def __init__(self, llm: LLMProvider, *, cache_path: Path | None = None) -> None:
        self._llm = llm
        self._cache = _Cache(cache_path)

    def generate(self, sample: TableSampleData) -> GeneratedDescriptions:
        # Nothing to fill?
        if not sample.request_table_desc and not sample.columns_to_describe:
            return GeneratedDescriptions(
                table_description="", column_descriptions={}, source="fallback",
            )
        key = _hash_key(sample)
        cached = self._cache.get(key)
        if cached:
            return GeneratedDescriptions(
                table_description=cached.get("table_description", ""),
                column_descriptions=cached.get("column_descriptions", {}),
                source="cache",
            )
        try:
            schema = _build_response_schema(
                sample.columns_to_describe, sample.request_table_desc
            )
            raw = self._llm.complete(
                [
                    LLMMessage(role="system", content=_SYSTEM),
                    LLMMessage(role="user", content=_format_prompt(sample)),
                ],
                schema=schema,
                temperature=0.0,
                max_tokens=600,
            )
        except Exception as e:
            log.warning("description gap-fill failed for %s: %s", sample.fqn, e)
            return GeneratedDescriptions(
                table_description="",
                column_descriptions={c: "" for c in sample.columns_to_describe},
                source="fallback",
            )
        # strict json_schema → trust the parse
        payload = json.loads(raw)
        result = GeneratedDescriptions(
            table_description=(payload.get("table_description") or "").strip(),
            column_descriptions={
                k: (v or "").strip()
                for k, v in payload.get("column_descriptions", {}).items()
            },
            source="llm",
        )
        self._cache.put(key, {
            "table_description": result.table_description,
            "column_descriptions": result.column_descriptions,
        })
        return result

    def generate_many(
        self,
        samples: list[TableSampleData],
        *,
        max_workers: int = 8,
    ) -> dict[str, GeneratedDescriptions]:
        # Skip samples with nothing to fill — they don't need an LLM call.
        active = [s for s in samples if s.request_table_desc or s.columns_to_describe]
        if not active:
            return {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(self.generate, active))
        return {s.fqn: r for s, r in zip(active, results)}
