"""Component 10 — visualization + description.

Three layers:
  * Unit (offline): result-shape inference, Vega-Lite spec rendering.
  * Integration: real Azure GPT-4o picks chart type for known result shapes.
  * Integration: full pipeline answer() returns viz + description.
"""

from __future__ import annotations

import os

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.pipeline.viz import (
    VizDescriber,
    VizSpec,
    _infer_shape,
)


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


# ── Offline ──────────────────────────────────────────────────────────────────


def test_infer_shape_empty() -> None:
    s = _infer_shape([])
    assert s.row_count == 0
    assert not s.is_single_row
    assert s.column_names == []


def test_infer_shape_single_row_aggregate() -> None:
    s = _infer_shape([{"total_students": 1959}])
    assert s.row_count == 1
    assert s.is_single_row
    assert s.has_aggregate_column
    assert s.column_types["total_students"] == "number"


def test_infer_shape_categorical_vs_numeric() -> None:
    rows = [
        {"school": "Grand Bend High", "n": 800},
        {"school": "Grand Bend Middle", "n": 600},
    ]
    s = _infer_shape(rows)
    assert s.row_count == 2
    assert s.column_types == {"school": "string", "n": "number"}
    assert s.has_aggregate_column


def test_infer_shape_temporal() -> None:
    rows = [
        {"event_date": "2024-09-09", "n": 12},
        {"event_date": "2024-09-10", "n": 9},
    ]
    s = _infer_shape(rows)
    assert s.has_temporal_column
    assert s.column_types["event_date"] == "datetime"


def test_vega_lite_render_bar_chart() -> None:
    spec = VizSpec(kind="bar", x="school", y="n", color=None,
                   title="Students per school", rationale="categorical x, numeric y")
    rows = [{"school": "A", "n": 100}, {"school": "B", "n": 50}]
    out = spec.to_vega_lite(rows)
    assert out["mark"] == "bar"
    assert out["encoding"]["x"]["field"] == "school"
    assert out["encoding"]["y"]["field"] == "n"
    assert out["data"]["values"] == rows


def test_vega_lite_render_table_returns_none() -> None:
    spec = VizSpec(kind="table", x=None, y=None, color=None, title="", rationale="")
    assert spec.to_vega_lite([{"a": 1}]) is None


def test_vega_lite_render_stat_returns_none() -> None:
    spec = VizSpec(kind="stat", x=None, y=None, color=None, title="", rationale="")
    assert spec.to_vega_lite([{"n": 100}]) is None


# ── Integration: real LLM ────────────────────────────────────────────────────


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_viz_picks_bar_for_per_school_counts() -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    vd = VizDescriber(build_llm(cfg.llm_for_task("visualization")))
    rows = [
        {"school": "Grand Bend High School", "student_count": 800},
        {"school": "Grand Bend Middle School", "student_count": 600},
        {"school": "Grand Bend Elementary School", "student_count": 559},
    ]
    out = vd.annotate(
        "How many students are enrolled in each school?", rows, sql="SELECT ...",
    )
    assert out.spec is not None
    assert out.spec.kind == "bar"
    assert out.spec.y in ("student_count", None)   # LLM may pick the numeric col
    assert out.description, "expected a non-empty description"
    print(f"\nbar picked: x={out.spec.x} y={out.spec.y} title={out.spec.title!r}")
    print(f"description: {out.description}")


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_viz_picks_stat_for_single_aggregate_row() -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    vd = VizDescriber(build_llm(cfg.llm_for_task("visualization")))
    rows = [{"total_students": 1959}]
    out = vd.annotate("How many students are in the database?", rows, sql="SELECT ...")
    assert out.spec is not None
    assert out.spec.kind == "stat"
    assert out.description


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_full_pipeline_returns_viz_and_description() -> None:
    """End-to-end: ask a real question, expect rows + viz + description."""
    cfg = load_config()
    if not _can_connect_target_db():
        pytest.skip("target DB unreachable")
    from text2sql.cli import _build_pipeline
    pipeline = _build_pipeline()
    result = pipeline.answer("How many students are enrolled in each school?")
    assert result.executed, result.error
    assert result.rows
    assert result.viz is not None
    assert result.description, "expected a non-empty description"
    if result.viz.kind in ("bar", "line", "point"):
        assert result.viz_vega_lite is not None
        assert result.viz_vega_lite["mark"] in ("bar", "line", "point")


def _can_connect_target_db() -> bool:
    cfg = load_config()
    try:
        from text2sql.providers import build_sql_engine
        eng = build_sql_engine(cfg.target_db_provider())
        eng.execute("SELECT 1 AS ok")
        return True
    except Exception:
        return False
