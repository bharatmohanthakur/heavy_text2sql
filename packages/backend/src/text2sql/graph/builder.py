"""Build a weighted undirected graph from parsed FKEdges + APSP artifacts.

Edge weights (spec §5.1):
  base               = 1.0
  × 3.0              if either endpoint is a descriptor   (penalize pivots through descriptors)
  × 0.5              if either endpoint is an association (favor association joins)
  × 0.8              if composite FK and both endpoints non-descriptor
  hop decay 1.2^h    applied at query time during Steiner expansion (not edge weight)

APSP outputs are mmap-ready:
  dist.npy       (n × n × float32)
  next_hop.npy   (n × n × int32, -1 = unreachable)
  graph.msgpack  (node + edge metadata, including composite column pairs)
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

import msgpack
import numpy as np
import rustworkx as rx

from text2sql.classification.table_mapping import TableClassification
from text2sql.graph.fk_parser import FKEdge

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableMeta:
    """Per-node attributes used for weighting + filtering."""
    fqn: str
    is_descriptor: bool = False
    is_association: bool = False
    is_extension: bool = False
    primary_domain: str | None = None
    aggregate_root: str | None = None


@dataclass
class _LogicalEdge:
    """An edge in the undirected graph: one or more FKEdges merged.

    Multiple FKEdges between the same (a, b) — typically rare for Ed-Fi, but
    possible — become one logical edge with weights minimized.
    """
    a: str                                   # fqn (sorted)
    b: str
    weight: float
    fks: list[FKEdge] = field(default_factory=list)


def _edge_weight(fk: FKEdge, meta: dict[str, TableMeta]) -> float:
    a = meta.get(fk.src_fqn)
    b = meta.get(fk.dst_fqn)
    is_desc = bool(a and a.is_descriptor) or bool(b and b.is_descriptor)
    is_assoc = bool(a and a.is_association) or bool(b and b.is_association)
    w = 1.0
    if is_desc:
        w *= 3.0
    if is_assoc:
        w *= 0.5
    if fk.is_composite and not is_desc:
        w *= 0.8
    return w


@dataclass
class FKGraph:
    """In-memory FK graph + APSP + lookup tables."""
    nodes: list[str]                    # fqn ordered → node-id
    node_index: dict[str, int]
    meta: dict[str, TableMeta]
    edges: dict[tuple[int, int], _LogicalEdge]   # (a_id, b_id), a_id < b_id
    rx_graph: rx.PyGraph                # undirected
    dist: np.ndarray | None = None      # n × n float32
    next_hop: np.ndarray | None = None  # n × n int32 (next node id along path)
    # Per-provider provenance (N3) — populated by save() / load(). Empty
    # for graphs built via the legacy single-target CLI path.
    provider_name: str = ""
    target_dialect: str = ""

    # ── construction helpers ───────────────────────────────────────────────────

    def edge_between(self, a: int, b: int) -> _LogicalEdge | None:
        key = (a, b) if a < b else (b, a)
        return self.edges.get(key)

    def neighbors(self, node_id: int) -> list[int]:
        return list(self.rx_graph.neighbors(node_id))

    def fqn(self, node_id: int) -> str:
        return self.nodes[node_id]

    def node_id(self, fqn: str) -> int:
        return self.node_index[fqn]

    # ── APSP ──────────────────────────────────────────────────────────────────

    def compute_apsp(self) -> None:
        """Compute all-pairs shortest paths via per-node Dijkstra.

        Stores `dist` (n × n float32) and `next_hop` (n × n int32) so that the
        path from i to j is rebuilt by repeatedly following next_hop[i, k].
        """
        n = len(self.nodes)
        dist = np.full((n, n), np.inf, dtype=np.float32)
        next_hop = np.full((n, n), -1, dtype=np.int32)
        np.fill_diagonal(dist, 0.0)

        for src in range(n):
            d_map = rx.dijkstra_shortest_path_lengths(
                self.rx_graph, src, edge_cost_fn=lambda e: e
            )
            paths = rx.dijkstra_shortest_paths(
                self.rx_graph, src, weight_fn=lambda e: e
            )
            for tgt, d in d_map.items():
                dist[src, tgt] = float(d)
            for tgt, path in paths.items():
                if len(path) >= 2:
                    next_hop[src, tgt] = int(path[1])

        self.dist = dist
        self.next_hop = next_hop

    def shortest_path(self, src_fqn: str, dst_fqn: str) -> list[str]:
        """Reconstruct the node-fqn path from APSP next-hop matrix."""
        if self.next_hop is None:
            raise RuntimeError("APSP not yet computed; call compute_apsp().")
        s = self.node_id(src_fqn)
        t = self.node_id(dst_fqn)
        if s == t:
            return [src_fqn]
        if self.dist[s, t] == np.inf:
            return []
        path = [s]
        cur = s
        while cur != t:
            nxt = int(self.next_hop[cur, t])
            if nxt < 0:
                return []
            path.append(nxt)
            cur = nxt
        return [self.nodes[i] for i in path]

    # ── Persistence (mmap-friendly) ───────────────────────────────────────────

    def save(
        self,
        dir_: Path,
        *,
        provider_name: str = "",
        target_dialect: str = "",
    ) -> None:
        """Persist graph + APSP to `dir_`. The optional provider tags get
        embedded in graph.msgpack's payload so a later load can fail loudly
        on cross-provider mismatch (catalog vs graph from different DBs is
        a silent data quality bug)."""
        dir_.mkdir(parents=True, exist_ok=True)
        if self.dist is None or self.next_hop is None:
            raise RuntimeError("call compute_apsp() before save()")
        np.save(dir_ / "dist.npy", self.dist)
        np.save(dir_ / "next_hop.npy", self.next_hop)

        # Edge metadata: composite FK pairs need to survive a reload so JOIN
        # expansion can produce the right ON clauses without re-parsing SQL.
        edges_payload = []
        for (a, b), e in self.edges.items():
            edges_payload.append({
                "a": a,
                "b": b,
                "weight": e.weight,
                "fks": [
                    {
                        "src_schema": fk.src_schema,
                        "src_table": fk.src_table,
                        "dst_schema": fk.dst_schema,
                        "dst_table": fk.dst_table,
                        "constraint_name": fk.constraint_name,
                        "column_pairs": list(fk.column_pairs),
                    }
                    for fk in e.fks
                ],
            })
        meta_payload = {
            fqn: {
                "fqn": m.fqn,
                "is_descriptor": m.is_descriptor,
                "is_association": m.is_association,
                "is_extension": m.is_extension,
                "primary_domain": m.primary_domain,
                "aggregate_root": m.aggregate_root,
            }
            for fqn, m in self.meta.items()
        }
        payload = {
            "nodes": self.nodes,
            "edges": edges_payload,
            "meta": meta_payload,
            # Per-provider provenance (N3). Empty when the build was kicked
            # off without an active provider context (legacy CLI path).
            "provider_name": provider_name,
            "target_dialect": target_dialect,
        }
        (dir_ / "graph.msgpack").write_bytes(msgpack.packb(payload, use_bin_type=True))

    @classmethod
    def load(
        cls,
        dir_: Path,
        *,
        expected_provider: str | None = None,
    ) -> "FKGraph":
        """Load graph + APSP from `dir_`. When `expected_provider` is
        passed and the on-disk payload's provider_name is non-empty and
        different, raise — this catches the case where a deployment
        flipped target_db.primary without rebuilding artifacts."""
        payload = msgpack.unpackb((dir_ / "graph.msgpack").read_bytes(), raw=False)
        provider_name = payload.get("provider_name", "") or ""
        if expected_provider and provider_name and provider_name != expected_provider:
            raise RuntimeError(
                f"FK graph at {dir_} was built for provider {provider_name!r}, "
                f"but active provider is {expected_provider!r}. Run "
                f"`text2sql rebuild --provider {expected_provider}` to refresh."
            )
        nodes: list[str] = payload["nodes"]
        node_index = {n: i for i, n in enumerate(nodes)}
        meta = {
            fqn: TableMeta(**m) for fqn, m in payload["meta"].items()
        }
        rx_graph = rx.PyGraph(multigraph=False)
        rx_graph.add_nodes_from(nodes)
        edges: dict[tuple[int, int], _LogicalEdge] = {}
        for e in payload["edges"]:
            a, b = int(e["a"]), int(e["b"])
            fks = [
                FKEdge(
                    src_schema=fk["src_schema"], src_table=fk["src_table"],
                    dst_schema=fk["dst_schema"], dst_table=fk["dst_table"],
                    constraint_name=fk["constraint_name"],
                    column_pairs=tuple(tuple(pair) for pair in fk["column_pairs"]),
                )
                for fk in e["fks"]
            ]
            le = _LogicalEdge(a=nodes[a], b=nodes[b], weight=float(e["weight"]), fks=fks)
            edges[(a, b)] = le
            rx_graph.add_edge(a, b, le.weight)
        g = cls(
            nodes=nodes, node_index=node_index, meta=meta,
            edges=edges, rx_graph=rx_graph,
        )
        # Stash the provenance so callers can introspect it post-load.
        g.provider_name = provider_name
        g.target_dialect = payload.get("target_dialect", "") or ""
        dist_path = dir_ / "dist.npy"
        if dist_path.exists():
            g.dist = np.load(dist_path, mmap_mode="r")
        next_path = dir_ / "next_hop.npy"
        if next_path.exists():
            g.next_hop = np.load(next_path, mmap_mode="r")
        return g


def _table_meta_from_classifications(
    classifications: Iterable[TableClassification],
) -> dict[str, TableMeta]:
    out: dict[str, TableMeta] = {}
    for c in classifications:
        out[c.fqn] = TableMeta(
            fqn=c.fqn,
            is_descriptor=c.is_descriptor,
            is_association=c.is_association,
            is_extension=c.is_extension,
            primary_domain=c.primary_domain,
            aggregate_root=c.aggregate_root,
        )
    return out


def build_graph(
    edges: list[FKEdge],
    classifications: Iterable[TableClassification] | None = None,
) -> FKGraph:
    """Build an FKGraph + compute APSP. Pass classifications so descriptor /
    association edge weights apply correctly; without them, every edge is
    treated as a normal table-to-table FK at weight 1.0."""

    meta: dict[str, TableMeta] = (
        _table_meta_from_classifications(classifications) if classifications else {}
    )

    # Discover every node
    fqns: set[str] = set()
    for e in edges:
        fqns.add(e.src_fqn)
        fqns.add(e.dst_fqn)
    # Ensure classified tables are present even if they have no FKs
    for fqn in meta.keys():
        fqns.add(fqn)

    nodes = sorted(fqns)
    node_index = {n: i for i, n in enumerate(nodes)}

    # Default-fill missing meta
    for fqn in nodes:
        if fqn not in meta:
            meta[fqn] = TableMeta(
                fqn=fqn,
                is_descriptor=fqn.split(".")[-1].endswith("Descriptor"),
                is_association=fqn.split(".")[-1].endswith("Association"),
            )

    # Merge multiple FKs between same node-pair
    grouped: dict[tuple[int, int], list[FKEdge]] = defaultdict(list)
    for e in edges:
        a = node_index[e.src_fqn]
        b = node_index[e.dst_fqn]
        if a == b:        # self-FK — skip (no useful join structure)
            continue
        key = (a, b) if a < b else (b, a)
        grouped[key].append(e)

    rx_graph = rx.PyGraph(multigraph=False)
    rx_graph.add_nodes_from(nodes)
    logical_edges: dict[tuple[int, int], _LogicalEdge] = {}
    for (a, b), fks in grouped.items():
        weight = min(_edge_weight(fk, meta) for fk in fks)
        le = _LogicalEdge(a=nodes[a], b=nodes[b], weight=weight, fks=fks)
        logical_edges[(a, b)] = le
        rx_graph.add_edge(a, b, weight)

    g = FKGraph(
        nodes=nodes,
        node_index=node_index,
        meta=meta,
        edges=logical_edges,
        rx_graph=rx_graph,
    )
    g.compute_apsp()
    return g


def save_graph(
    g: FKGraph,
    dir_: Path,
    *,
    provider_name: str = "",
    target_dialect: str = "",
) -> None:
    g.save(dir_, provider_name=provider_name, target_dialect=target_dialect)


def load_graph(
    dir_: Path,
    *,
    expected_provider: str | None = None,
) -> FKGraph:
    return FKGraph.load(dir_, expected_provider=expected_provider)
