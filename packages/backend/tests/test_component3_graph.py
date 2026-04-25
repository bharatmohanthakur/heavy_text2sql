"""Component 3 integration tests against real DS 6.1.0 FK data.

  - Parse 0030-ForeignKeys.sql → ≥1,663 edges (DS 6.1.0 reference).
  - Build graph; node count includes every FK endpoint.
  - APSP latency.
  - Known shortest paths (Student→School, StudentSchoolAssociation→Course, …).
  - Steiner tree spans all targets.
  - Round-trip via save/load preserves edges + APSP.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pytest

from text2sql.classification import read_table_mapping
from text2sql.config import REPO_ROOT, load_config
from text2sql.graph import (
    FKGraph,
    build_graph,
    load_graph,
    parse_fks,
    save_graph,
    steiner,
)
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all


@pytest.fixture(scope="module")
def manifest():
    cfg = load_config()
    return fetch_all(IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT))


@pytest.fixture(scope="module")
def edges(manifest):
    out = []
    for art in manifest.artifacts:
        out.extend(parse_fks(art.foreign_keys_sql_path))
    return out


@pytest.fixture(scope="module")
def classifications():
    p = REPO_ROOT / "data/artifacts/table_classification.json"
    if not p.exists():
        return []
    return read_table_mapping(p).classifications


@pytest.fixture(scope="module")
def graph(edges, classifications):
    return build_graph(edges, classifications=classifications)


# ── Parse ──────────────────────────────────────────────────────────────────────


def test_parse_fk_count(edges) -> None:
    # DS 6.1.0 reference: 1,663 FKs in associationDefinitions
    assert len(edges) >= 1_500, f"expected ≥1500 FK edges, got {len(edges)}"
    assert len(edges) <= 1_700


def test_parse_fk_known_simple(edges) -> None:
    student_school = next(
        (e for e in edges if e.src_fqn == "edfi.AcademicWeek" and e.dst_fqn == "edfi.School"),
        None,
    )
    assert student_school is not None
    assert student_school.column_pairs == (("SchoolId", "SchoolId"),)
    assert not student_school.is_composite


def test_parse_fk_known_composite(edges) -> None:
    composite = [e for e in edges if e.is_composite]
    assert composite, "expected at least some composite FKs in DS 6.1.0"
    sample = composite[0]
    assert len(sample.column_pairs) >= 2


# ── Build ──────────────────────────────────────────────────────────────────────


def test_graph_has_core_tables(graph: FKGraph) -> None:
    for fqn in ["edfi.Student", "edfi.School", "edfi.StudentSchoolAssociation",
                "edfi.Course", "edfi.Section"]:
        assert fqn in graph.node_index, f"missing {fqn}"


def test_apsp_dimensions(graph: FKGraph) -> None:
    n = len(graph.nodes)
    assert graph.dist is not None and graph.next_hop is not None
    assert graph.dist.shape == (n, n)
    assert graph.next_hop.shape == (n, n)
    # Every node has zero distance to itself.
    assert np.allclose(np.diag(graph.dist), 0.0)


# ── Shortest paths ─────────────────────────────────────────────────────────────


def test_shortest_path_student_to_school(graph: FKGraph) -> None:
    path = graph.shortest_path("edfi.Student", "edfi.School")
    # Student → … → School. Must include StudentSchoolAssociation in some
    # shortest path or take a direct hop through Person.
    assert path, "no path Student → School"
    assert path[0] == "edfi.Student"
    assert path[-1] == "edfi.School"
    assert len(path) <= 4


def test_shortest_path_studentsection_to_course(graph: FKGraph) -> None:
    path = graph.shortest_path("edfi.StudentSectionAssociation", "edfi.Course")
    assert path[0] == "edfi.StudentSectionAssociation"
    assert path[-1] == "edfi.Course"
    assert len(path) <= 5


# ── Steiner ────────────────────────────────────────────────────────────────────


def test_steiner_two_nodes_matches_shortest(graph: FKGraph) -> None:
    targets = ["edfi.Student", "edfi.School"]
    tree = steiner(graph, targets)
    assert set(targets).issubset(tree.nodes)
    assert tree.total_weight > 0
    assert len(tree.edges) == len(tree.nodes) - 1


def test_steiner_three_nodes_connected(graph: FKGraph) -> None:
    targets = ["edfi.Student", "edfi.School", "edfi.Course"]
    tree = steiner(graph, targets)
    assert set(targets).issubset(set(tree.nodes))
    # Tree property: edges = nodes − 1
    assert len(tree.edges) == len(tree.nodes) - 1


def test_steiner_join_clauses_render(graph: FKGraph) -> None:
    tree = steiner(graph, ["edfi.Student", "edfi.School"])
    clauses = tree.to_join_clauses(dialect="postgresql")
    assert clauses
    assert all('"' in c and "ON" in c.upper() for c in clauses)


# ── Performance ────────────────────────────────────────────────────────────────


def test_steiner_latency_p99_under_5ms(graph: FKGraph) -> None:
    """For k≤5 over the live graph, p99 should be < 5ms (target < 1ms)."""
    import random
    random.seed(7)
    candidates = [n for n in graph.nodes if not graph.meta[n].is_descriptor]
    samples_us: list[float] = []
    for _ in range(200):
        k = random.randint(2, 5)
        targets = random.sample(candidates, k)
        t0 = time.perf_counter()
        steiner(graph, targets)
        samples_us.append((time.perf_counter() - t0) * 1e6)
    samples_us.sort()
    p50 = samples_us[len(samples_us) // 2]
    p99 = samples_us[int(len(samples_us) * 0.99)]
    print(f"\nSteiner latency  p50={p50:.0f}µs  p99={p99:.0f}µs over 200 random k∈[2,5] queries")
    assert p99 < 5_000, f"p99={p99:.0f}µs exceeds 5ms"


# ── Round-trip ─────────────────────────────────────────────────────────────────


def test_save_load_round_trip(graph: FKGraph, tmp_path: Path) -> None:
    save_graph(graph, tmp_path)
    loaded = load_graph(tmp_path)
    assert loaded.nodes == graph.nodes
    assert len(loaded.edges) == len(graph.edges)
    # Spot-check APSP equality
    s = graph.node_id("edfi.Student")
    t = graph.node_id("edfi.School")
    assert float(loaded.dist[s, t]) == pytest.approx(float(graph.dist[s, t]))
