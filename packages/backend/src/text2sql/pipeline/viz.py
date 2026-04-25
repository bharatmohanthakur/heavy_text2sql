"""Component 10: visualization + description.

Given the executed rows, produce two artifacts in parallel:
  * `viz_spec` — a Vega-Lite spec the frontend can render directly.
  * `description` — a one-paragraph plain-English summary.

Both are LLM-generated with strict json_schema responses.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from text2sql.providers.base import LLMMessage, LLMProvider

log = logging.getLogger(__name__)


# ── Result-set shape inference (cheap, deterministic) ────────────────────────


@dataclass
class ResultShape:
    column_names: list[str]
    column_types: dict[str, str]                 # name → "string" | "number" | "datetime" | "boolean" | "null"
    row_count: int
    is_single_row: bool
    has_aggregate_column: bool                   # any name like count_*, total_*, avg_*, etc.
    has_temporal_column: bool


def _infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    s = str(value)
    if len(s) >= 8 and (s[4] == "-" or s[4] == "/"):
        return "datetime"
    return "string"


def _infer_shape(rows: list[dict]) -> ResultShape:
    if not rows:
        return ResultShape(
            column_names=[], column_types={}, row_count=0,
            is_single_row=False, has_aggregate_column=False, has_temporal_column=False,
        )
    cols = list(rows[0].keys())
    types: dict[str, str] = {}
    for c in cols:
        seen: set[str] = set()
        for r in rows[: min(20, len(rows))]:
            t = _infer_type(r.get(c))
            if t != "null":
                seen.add(t)
        # Pick the dominant non-null type; fall back to string.
        if "datetime" in seen:
            types[c] = "datetime"
        elif "number" in seen and "string" not in seen:
            types[c] = "number"
        elif "boolean" in seen and len(seen) == 1:
            types[c] = "boolean"
        else:
            types[c] = "string" if seen else "null"
    aggregate_kw = ("count", "total", "avg", "average", "sum", "min", "max", "n")
    return ResultShape(
        column_names=cols,
        column_types=types,
        row_count=len(rows),
        is_single_row=len(rows) == 1,
        has_aggregate_column=any(any(k in c.lower() for k in aggregate_kw) for c in cols),
        has_temporal_column=any(t == "datetime" for t in types.values()),
    )


# ── LLM-driven viz + description ─────────────────────────────────────────────


VIZ_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "kind": {"type": "string", "enum": ["bar", "line", "point", "stat", "table"]},
        "x": {"type": ["string", "null"]},
        "y": {"type": ["string", "null"]},
        "color": {"type": ["string", "null"]},
        "title": {"type": "string"},
        "rationale": {"type": "string"},
    },
    "required": ["kind", "x", "y", "color", "title", "rationale"],
}


DESCRIPTION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
    },
    "required": ["summary"],
}


_VIZ_SYSTEM = """\
Pick a chart type for a SQL result set:
  - "bar"   for a categorical x-axis vs numeric y (rankings, counts by group).
  - "line"  for a temporal x-axis vs numeric y (trends over time).
  - "point" for two numeric columns (correlation / scatter).
  - "stat"  for a single-row result with one or more aggregate values.
  - "table" when nothing else fits (text-heavy, multi-column listings).

Return JSON only with: kind, x, y, color (nullable), title, rationale.
"""


_DESC_SYSTEM = """\
Write a single-paragraph plain-English summary of a SQL result set for an
end-user dashboard. Be specific (cite numbers, names from the rows). 80
words maximum. Do not mention SQL, columns, or table names. JSON only:
{"summary": "..."}.
"""


@dataclass
class VizSpec:
    """Lean chart spec — render to Vega-Lite client-side or via the helper below."""
    kind: str                   # bar | line | point | stat | table
    x: str | None
    y: str | None
    color: str | None
    title: str
    rationale: str

    def to_vega_lite(self, rows: list[dict]) -> dict | None:
        if self.kind in ("table", "stat"):
            return None
        mark = {
            "bar": "bar", "line": "line", "point": "point",
        }.get(self.kind, "bar")
        encoding: dict[str, Any] = {}
        if self.x:
            x_enc: dict[str, Any] = {"field": self.x, "type": _vega_type(self.x, rows)}
            # Long category labels (school names etc.) collapse into illegible
            # ticks at small widths — angle them and cap label length.
            if x_enc["type"] in ("nominal", "ordinal"):
                x_enc["axis"] = {"labelAngle": -35, "labelLimit": 200}
            encoding["x"] = x_enc
        if self.y:
            encoding["y"] = {"field": self.y, "type": _vega_type(self.y, rows)}
        if self.color:
            encoding["color"] = {"field": self.color, "type": "nominal"}
        return {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "title": self.title,
            "data": {"values": rows},
            # Fill the parent container width; pick a comfortable fixed
            # height. Without these, vega-embed renders at its ~200px default
            # which looks crammed in the chat bubble.
            "width": "container",
            "height": 320,
            "autosize": {"type": "fit", "contains": "padding", "resize": True},
            "padding": {"left": 8, "right": 8, "top": 8, "bottom": 8},
            "mark": mark,
            "encoding": encoding,
        }


def _vega_type(field: str, rows: list[dict]) -> str:
    if not rows:
        return "nominal"
    sample = next((r[field] for r in rows if r.get(field) is not None), None)
    if isinstance(sample, bool):
        return "nominal"
    if isinstance(sample, (int, float)):
        return "quantitative"
    s = str(sample) if sample is not None else ""
    if len(s) >= 8 and (s[4:5] == "-" or s[4:5] == "/"):
        return "temporal"
    return "nominal"


@dataclass
class VizResult:
    spec: VizSpec | None
    description: str
    shape: ResultShape
    errors: list[str] = field(default_factory=list)


def _truncate_for_prompt(rows: list[dict], max_rows: int = 25) -> list[dict]:
    return rows[:max_rows]


class VizDescriber:
    """Run the viz + description LLM calls in parallel."""

    def __init__(self, llm: LLMProvider) -> None:
        self._llm = llm

    def annotate(
        self,
        nl_question: str,
        rows: list[dict],
        sql: str,
    ) -> VizResult:
        shape = _infer_shape(rows)
        if not rows:
            return VizResult(spec=None, description="No matching rows.", shape=shape)

        sample = _truncate_for_prompt(rows)
        result = VizResult(spec=None, description="", shape=shape)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_viz = pool.submit(self._gen_viz, nl_question, sample, shape)
            f_desc = pool.submit(self._gen_description, nl_question, sample, shape)
            try:
                result.spec = f_viz.result()
            except Exception as e:
                log.warning("viz LLM call failed: %s", e)
                result.errors.append(f"viz: {e}")
            try:
                result.description = f_desc.result()
            except Exception as e:
                log.warning("description LLM call failed: %s", e)
                result.errors.append(f"description: {e}")
        return result

    def _gen_viz(
        self, nl: str, sample: list[dict], shape: ResultShape
    ) -> VizSpec:
        user = (
            f"User question: {nl}\n\n"
            f"Result shape:\n"
            f"  rows: {shape.row_count}\n"
            f"  columns: {[(c, shape.column_types.get(c, '?')) for c in shape.column_names]}\n"
            f"  has_aggregate_column: {shape.has_aggregate_column}\n"
            f"  has_temporal_column: {shape.has_temporal_column}\n\n"
            f"First {len(sample)} rows:\n{json.dumps(sample, default=str)[:2500]}\n"
        )
        raw = self._llm.complete(
            [LLMMessage(role="system", content=_VIZ_SYSTEM),
             LLMMessage(role="user", content=user)],
            schema=VIZ_SCHEMA,
            temperature=0.0,
            max_tokens=300,
        )
        payload = json.loads(raw)
        return VizSpec(
            kind=payload["kind"],
            x=payload["x"],
            y=payload["y"],
            color=payload["color"],
            title=payload["title"],
            rationale=payload["rationale"],
        )

    def _gen_description(
        self, nl: str, sample: list[dict], shape: ResultShape
    ) -> str:
        user = (
            f"User question: {nl}\n\n"
            f"Result has {shape.row_count} row(s). First {len(sample)}:\n"
            f"{json.dumps(sample, default=str)[:2500]}\n"
        )
        raw = self._llm.complete(
            [LLMMessage(role="system", content=_DESC_SYSTEM),
             LLMMessage(role="user", content=user)],
            schema=DESCRIPTION_SCHEMA,
            temperature=0.2,
            max_tokens=200,
        )
        return json.loads(raw)["summary"]
