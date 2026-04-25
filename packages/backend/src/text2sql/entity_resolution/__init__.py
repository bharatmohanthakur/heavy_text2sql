"""Component 6: Entity resolver — NL phrase → DB value.

Given a phrase the user mentioned ("Hispanc", "Pre-K", "Algebra I") and
optional column scope (from cluster routing), return ranked candidate
(table, column, value) matches plus a confidence-style ordering.

4-tier funnel per spec §7:
  1. Bloom filter exact match           ~1ms     (sub-component: tier1)
  2. rapidfuzz trigram + Metaphone      ~5ms     (sub-component: tier2)
  3. Vector ANN on column_values        ~10ms    (sub-component: tier3)
  4. LLM disambiguation                 ~300ms   (sub-component: tier4)

Earlier tiers short-circuit when their match passes a confidence threshold;
only ambiguous misses fall through to the next.
"""

from text2sql.entity_resolution.resolver import (
    EntityCandidate,
    EntityResolver,
    ResolutionResult,
)
from text2sql.entity_resolution.value_index import ValueIndex, build_value_index

__all__ = [
    "EntityCandidate",
    "EntityResolver",
    "ResolutionResult",
    "ValueIndex",
    "build_value_index",
]
