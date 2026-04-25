"""Component 3: FK graph.

Parse 0030-ForeignKeys.sql → in-memory FKEdge list → undirected weighted graph
in rustworkx → APSP (dist, next-hop) → Steiner solver → JOIN-clause expansion.

Public API:
  parse_fks(sql_path) -> list[FKEdge]
  build_graph(edges, table_meta) -> FKGraph
  FKGraph.shortest_path(t1, t2) -> list[FKEdge]
  FKGraph.steiner(tables) -> SteinerTree
  SteinerTree.to_join_clauses(dialect) -> list[str]
"""

from text2sql.graph.fk_parser import FKEdge, parse_fks
from text2sql.graph.builder import FKGraph, build_graph, load_graph, save_graph
from text2sql.graph.steiner import SteinerEdge, SteinerTree, steiner

__all__ = [
    "FKEdge",
    "FKGraph",
    "SteinerEdge",
    "SteinerTree",
    "build_graph",
    "load_graph",
    "parse_fks",
    "save_graph",
    "steiner",
]
