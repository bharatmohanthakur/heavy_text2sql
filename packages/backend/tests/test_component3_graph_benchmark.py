"""Component 3 deep benchmark — real-world join scenarios.

Goes beyond the smoke tests. Hits the live DS 6.1.0 graph (829 nodes, 1,663
FKs) and asserts:

  * Known multi-table join paths land within expected hop counts.
  * Edge weighting works — descriptor pivots cost more, association pivots cost
    less. (Spec §5.1 weights.)
  * Composite FKs survive Steiner-tree rendering with all column pairs intact.
  * JOIN clauses are syntactically valid for both MSSQL and Postgres dialects.
  * Steiner trees connect every requested target with no orphans.
  * Steiner runs across the *entire* node space without latency spikes for
    large k (up to 10 targets).
"""

from __future__ import annotations

import random
import re
import time
from collections import Counter

import numpy as np
import pytest

from text2sql.classification import read_table_mapping
from text2sql.config import REPO_ROOT, load_config
from text2sql.graph import FKGraph, build_graph, parse_fks, steiner
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def graph() -> FKGraph:
    cfg = load_config()
    manifest = fetch_all(IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT))
    edges = []
    for art in manifest.artifacts:
        edges.extend(parse_fks(art.foreign_keys_sql_path))
    cl_path = REPO_ROOT / "data/artifacts/table_classification.json"
    classifications = (
        read_table_mapping(cl_path).classifications if cl_path.exists() else []
    )
    return build_graph(edges, classifications=classifications)


# ── Real Ed-Fi join scenarios (15) ────────────────────────────────────────────

# Each scenario describes a realistic NL→SQL query and the tables it should
# touch. The Steiner tree must include every target.
JOIN_SCENARIOS = [
    {
        "name": "students per school",
        "tables": ["edfi.Student", "edfi.School"],
        "max_hops": 3,
    },
    {
        "name": "student attendance by school",
        "tables": ["edfi.Student", "edfi.StudentSchoolAttendanceEvent", "edfi.School"],
        "max_hops": 4,
    },
    {
        "name": "section enrollment + course",
        "tables": [
            "edfi.Student",
            "edfi.StudentSectionAssociation",
            "edfi.Section",
            "edfi.Course",
        ],
        "max_hops": 6,
    },
    {
        "name": "graduation by school year",
        "tables": [
            "edfi.Student",
            "edfi.StudentSchoolAssociation",
            "edfi.SchoolYearType",
        ],
        "max_hops": 4,
    },
    {
        "name": "discipline incidents per student",
        "tables": [
            "edfi.Student",
            "edfi.DisciplineIncident",
            "edfi.StudentDisciplineIncidentBehaviorAssociation",
        ],
        "max_hops": 5,
    },
    {
        "name": "staff teaching a section",
        "tables": [
            "edfi.Staff",
            "edfi.StaffSectionAssociation",
            "edfi.Section",
        ],
        "max_hops": 4,
    },
    {
        "name": "report card grades",
        "tables": [
            "edfi.Student",
            "edfi.ReportCard",
            "edfi.Grade",
        ],
        "max_hops": 6,
    },
    {
        "name": "assessment results per student",
        "tables": [
            "edfi.Student",
            "edfi.StudentAssessment",
            "edfi.Assessment",
        ],
        "max_hops": 5,
    },
    {
        "name": "course transcripts",
        "tables": [
            "edfi.Student",
            "edfi.CourseTranscript",
            "edfi.Course",
        ],
        "max_hops": 6,
    },
    {
        "name": "calendar dates per school",
        "tables": [
            "edfi.School",
            "edfi.CalendarDate",
            "edfi.Calendar",
        ],
        "max_hops": 4,
    },
    {
        "name": "intervention services for students",
        "tables": [
            "edfi.Student",
            "edfi.StudentInterventionAssociation",
            "edfi.Intervention",
        ],
        "max_hops": 5,
    },
    {
        "name": "special-ed program associations",
        "tables": [
            "edfi.Student",
            "edfi.StudentSpecialEducationProgramAssociation",
            "edfi.Program",
        ],
        "max_hops": 5,
    },
    {
        "name": "staff credentials",
        "tables": [
            "edfi.Staff",
            "edfi.Credential",
        ],
        "max_hops": 5,
    },
    {
        "name": "bell schedule sections",
        "tables": [
            "edfi.BellSchedule",
            "edfi.Section",
            "edfi.School",
        ],
        "max_hops": 5,
    },
    {
        "name": "five-table cross-domain",
        "tables": [
            "edfi.Student",
            "edfi.School",
            "edfi.Section",
            "edfi.Course",
            "edfi.Assessment",
        ],
        "max_hops": 10,
    },
]


@pytest.mark.parametrize("scenario", JOIN_SCENARIOS, ids=lambda s: s["name"])
def test_steiner_real_scenarios(graph: FKGraph, scenario: dict) -> None:
    """Every requested target ends up in the Steiner tree, hop-count is sane."""
    targets = scenario["tables"]
    missing = [t for t in targets if t not in graph.node_index]
    if missing:
        pytest.skip(f"tables not in graph: {missing}")

    tree = steiner(graph, targets)
    assert set(targets).issubset(set(tree.nodes)), (
        f"missing tables in tree for {scenario['name']!r}: "
        f"want {targets}, got {tree.nodes}"
    )
    # Tree property
    assert len(tree.edges) == len(tree.nodes) - 1
    # Hop-count budget
    assert len(tree.nodes) <= scenario["max_hops"], (
        f"{scenario['name']!r}: tree spans {len(tree.nodes)} nodes "
        f"(max {scenario['max_hops']})"
    )


# ── Edge-weight policy ────────────────────────────────────────────────────────


def test_descriptor_penalty_steers_paths(graph: FKGraph) -> None:
    """A path that pivots through a Descriptor must be heavier than a path that
    doesn't (when both are possible). Validates the 3× descriptor weight."""
    desc_edges = [
        e for e in graph.edges.values() if any(
            graph.meta[fqn].is_descriptor for fqn in (e.a, e.b)
        )
    ]
    non_desc_edges = [
        e for e in graph.edges.values() if not any(
            graph.meta[fqn].is_descriptor for fqn in (e.a, e.b)
        )
    ]
    avg_desc = sum(e.weight for e in desc_edges) / len(desc_edges)
    avg_non = sum(e.weight for e in non_desc_edges) / len(non_desc_edges)
    print(f"\navg descriptor edge weight:     {avg_desc:.3f}")
    print(f"avg non-descriptor edge weight: {avg_non:.3f}")
    assert avg_desc > 2.0 * avg_non


def test_association_bonus_present(graph: FKGraph) -> None:
    """Edges touching an association should average lighter than other
    non-descriptor edges."""
    assoc_edges = [
        e for e in graph.edges.values()
        if any(graph.meta[fqn].is_association for fqn in (e.a, e.b))
        and not any(graph.meta[fqn].is_descriptor for fqn in (e.a, e.b))
    ]
    plain_edges = [
        e for e in graph.edges.values()
        if not any(graph.meta[fqn].is_association for fqn in (e.a, e.b))
        and not any(graph.meta[fqn].is_descriptor for fqn in (e.a, e.b))
    ]
    if not assoc_edges or not plain_edges:
        pytest.skip("no associations or no plain edges to compare")
    avg_assoc = sum(e.weight for e in assoc_edges) / len(assoc_edges)
    avg_plain = sum(e.weight for e in plain_edges) / len(plain_edges)
    print(f"\navg association edge weight: {avg_assoc:.3f}")
    print(f"avg plain edge weight:       {avg_plain:.3f}")
    assert avg_assoc < avg_plain


# ── Composite FK handling ─────────────────────────────────────────────────────


def test_composite_fk_round_trips_through_steiner(graph: FKGraph) -> None:
    """Find a known composite-FK pair, request a Steiner tree across it,
    confirm both column-pairs land in the rendered SQL."""
    composite_edges = [
        e for e in graph.edges.values() if any(fk.is_composite for fk in e.fks)
    ]
    assert composite_edges, "no composite FKs found in DS 6.1.0?"

    # Pick the first composite edge — exercise both endpoints as Steiner targets.
    edge = composite_edges[0]
    fk = next(fk for fk in edge.fks if fk.is_composite)
    targets = [fk.src_fqn, fk.dst_fqn]
    tree = steiner(graph, targets)
    sql = " ; ".join(tree.to_join_clauses(dialect="mssql"))
    # Each column pair must appear as a separate AND condition.
    for src_col, dst_col in fk.column_pairs:
        assert src_col in sql and dst_col in sql, (
            f"missing column pair {src_col}={dst_col} in rendered SQL"
        )
    # And there should be at least N-1 ANDs for an N-column composite FK.
    n_ands = sql.upper().count(" AND ")
    assert n_ands >= len(fk.column_pairs) - 1


# ── JOIN clause syntactic sanity ──────────────────────────────────────────────


def test_join_clauses_postgres_dialect(graph: FKGraph) -> None:
    tree = steiner(graph, ["edfi.Student", "edfi.School"])
    clauses = tree.to_join_clauses(dialect="postgresql")
    assert clauses
    for c in clauses:
        # double-quoted identifiers, JOIN ... ON shape
        assert re.match(r'^JOIN "[^"]+"\."[^"]+" ON ', c), c
        assert "=" in c


def test_join_clauses_mssql_dialect(graph: FKGraph) -> None:
    tree = steiner(graph, ["edfi.Student", "edfi.School"])
    clauses = tree.to_join_clauses(dialect="mssql")
    assert clauses
    for c in clauses:
        assert re.match(r"^JOIN \[[^\]]+\]\.\[[^\]]+\] ON ", c), c


# ── Connectivity / completeness ───────────────────────────────────────────────


def test_graph_is_largely_connected(graph: FKGraph) -> None:
    """Most non-extension tables should sit in one giant connected component.
    Ed-Fi designs the schema this way; if a table is isolated, we want to know."""
    import rustworkx as rx
    components = rx.connected_components(graph.rx_graph)
    sizes = sorted((len(c) for c in components), reverse=True)
    print(f"\nconnected components (size desc): {sizes[:5]}, total {len(sizes)}")
    assert sizes[0] >= 700, (
        f"largest connected component is only {sizes[0]} nodes — schema is fractured?"
    )


def test_steiner_handles_unreachable(graph: FKGraph) -> None:
    """Asking for a tree across an isolated node must not crash; it should
    skip it and warn."""
    import rustworkx as rx
    components = rx.connected_components(graph.rx_graph)
    components.sort(key=len)
    isolated_component = next(
        (c for c in components if len(c) <= 3), None
    )
    if not isolated_component:
        pytest.skip("no small isolated component in this graph")
    isolated_fqn = graph.nodes[next(iter(isolated_component))]
    tree = steiner(graph, ["edfi.Student", isolated_fqn])
    # Either we still got Student in a singleton tree or total weight is inf
    assert "edfi.Student" in tree.nodes


# ── Latency at scale ──────────────────────────────────────────────────────────


def test_steiner_latency_high_k(graph: FKGraph) -> None:
    """Even at k=10 across the live graph, p99 stays well under 10ms."""
    random.seed(11)
    candidates = [n for n in graph.nodes if not graph.meta[n].is_descriptor]
    samples_us: list[float] = []
    for _ in range(100):
        targets = random.sample(candidates, 10)
        t0 = time.perf_counter()
        steiner(graph, targets)
        samples_us.append((time.perf_counter() - t0) * 1e6)
    samples_us.sort()
    p50 = samples_us[len(samples_us) // 2]
    p99 = samples_us[int(len(samples_us) * 0.99)]
    print(f"\nSteiner latency (k=10) p50={p50:.0f}µs p99={p99:.0f}µs over 100 random samples")
    assert p99 < 10_000


# ── Demonstration: real rendered SQL ──────────────────────────────────────────


def test_demo_render_real_join(graph: FKGraph, capsys) -> None:
    """Print real JOIN clauses for the canonical query
    'students with their school and grade level'."""
    targets = [
        "edfi.Student",
        "edfi.StudentSchoolAssociation",
        "edfi.School",
    ]
    tree = steiner(graph, targets)
    print(f"\n=== JOIN tree for: students × school × association ===")
    print(f"nodes ({len(tree.nodes)}):")
    for n in tree.nodes:
        print(f"  - {n}")
    print(f"\nedges ({len(tree.edges)}):")
    for e in tree.edges:
        for fk in e.fks:
            cols = " AND ".join(f"{s}={d}" for s, d in fk.column_pairs)
            print(f"  - {e.src_fqn} -- {e.dst_fqn}  via {fk.constraint_name}  ({cols})")
    print(f"\nMSSQL JOIN clauses:")
    for c in tree.to_join_clauses(dialect="mssql"):
        print(f"  {c}")
    print(f"\nPostgres JOIN clauses:")
    for c in tree.to_join_clauses(dialect="postgresql"):
        print(f"  {c}")
    print(f"\ntotal weight: {tree.total_weight:.2f}")
    assert tree.edges
