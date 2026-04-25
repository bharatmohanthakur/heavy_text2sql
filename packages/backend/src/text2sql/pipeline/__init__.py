"""Component 8: NL→SQL pipeline orchestrator.

Wires every prior component into a single answer(nl) entry point:

  [1] domain classifier  (C2b)         → ranked domains
  [2] entity resolver    (C6)          → phrase → (descriptor / value) bindings
  [3] table retrieval    (C5)          → hybrid search filtered by routed domains
  [4] FK graph Steiner   (C3)          → join tree over retrieved tables
  [5] context assembly                 → M-Schema + joins + few-shots + bindings
  [6] LLM SQL generation               → final SQL
  [7] basic validation                 → sqlglot parse + LIMIT-0 EXPLAIN
  [8] execute                          → first N rows

Returns a `PipelineResult` carrying every intermediate artifact so debugging
is mechanical rather than guesswork.
"""

from text2sql.pipeline.context import ContextBuilder, PromptContext
from text2sql.pipeline.orchestrator import (
    PipelineResult,
    Text2SqlPipeline,
)
from text2sql.pipeline.repair import RepairLoop, RepairResult, validate_sql
from text2sql.pipeline.viz import VizDescriber, VizResult, VizSpec

__all__ = [
    "ContextBuilder",
    "PipelineResult",
    "PromptContext",
    "RepairLoop",
    "RepairResult",
    "Text2SqlPipeline",
    "VizDescriber",
    "VizResult",
    "VizSpec",
    "validate_sql",
]
