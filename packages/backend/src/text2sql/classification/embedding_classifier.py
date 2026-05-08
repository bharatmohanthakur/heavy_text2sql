"""Search-and-score domain classifier (retrieval + entity-resolution).

Replaces the LLM-only `QueryDomainClassifier` for the query path. Three
signals get combined per candidate domain, then ranked:

  1. Retrieval score   — sum of top-K table-hit scores whose payload
                         lists this domain.
  2. Entity-resolution — for every phrase the resolver successfully
                         pinned to a value, count that match toward the
                         domain(s) of the matched table. Confirms
                         "Hispanic" → RaceDescriptor → Demographics.
  3. (Optional) LLM   — `LLMDomainReranker` can be layered on top for
                         the very flat-score case; not enabled by default.

The catalog (whatever the operator declared) is the source of truth for
the candidate set — no hard-coded domain list, no curated descriptions
required, no per-domain seed_entities to maintain.

Cached on disk by hash(query + sorted catalog fqns + sorted domain
names) so reruns of the same question are instant.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from text2sql.classification.query_classifier import QueryClassification
from text2sql.embedding.retriever import TableRetriever

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DomainScore:
    name: str
    retrieval: float
    n_resolved: int
    resolution_quality: float
    combined: float


class EmbeddingDomainClassifier:
    """Classify a question to a ranked list of domains by searching
    the table catalog (vector + BM25 hybrid) and confirming with the
    entity resolver.

    Constructor args:
      retriever          — `TableRetriever` over the `tables` collection.
      catalog            — `TableCatalog` (used for fqn → domains lookup
                           when attributing entity-resolution hits).
      entity_resolver    — Optional `EntityResolver`. When None, only the
                           retrieval signal is used.
      top_k_tables       — How many table hits to fetch (default 30).
      top_n              — Cap on returned domain rank list (default 3).
      alpha              — Weight for retrieval signal vs. entity
                           resolution. 1.0 = retrieval only;
                           0.5 = equal mix; default 0.7.
      cache_path         — On-disk cache file. None disables caching.
    """

    def __init__(
        self,
        retriever: TableRetriever,
        catalog,
        *,
        entity_resolver=None,
        top_k_tables: int = 30,
        top_n: int = 3,
        alpha: float = 0.7,
        cache_path: Path | None = None,
    ) -> None:
        self._retriever = retriever
        self._catalog = catalog
        self._resolver = entity_resolver
        self._top_k_tables = top_k_tables
        self._top_n = top_n
        self._alpha = alpha
        self._cache = _Cache(cache_path)

        # Build fqn → domains lookup once. Used to attribute entity-
        # resolution matches to their table's declared domain(s).
        self._domains_by_fqn: dict[str, tuple[str, ...]] = {
            e.fqn: tuple(e.domains) for e in catalog.entries
        }
        self._all_domain_names = sorted({
            d for ds in self._domains_by_fqn.values() for d in ds
        })

    # ── Public ─────────────────────────────────────────────────────────────

    def classify(self, query: str) -> QueryClassification:
        query = query.strip()
        if not query:
            return QueryClassification(
                query=query, domains=[], reasoning="", source="fallback",
            )
        if not self._all_domain_names:
            return QueryClassification(
                query=query, domains=[],
                reasoning="catalog has no domains tagged",
                source="fallback",
            )

        key = self._cache_key(query)
        cached = self._cache.get(key)
        if cached:
            return QueryClassification(
                query=query,
                domains=list(cached.get("domains", [])),
                reasoning=cached.get("reasoning", ""),
                source="cache",
                raw=cached,
            )

        ranked, debug = self._score(query)
        if not ranked:
            # No retrieval hits — fall back to the most-populated domain
            # so downstream layers still get something routable.
            ordered = sorted(self._domains_by_fqn.values(), key=lambda v: -len(v))
            fallback_names = []
            for ds in ordered:
                for d in ds:
                    if d not in fallback_names:
                        fallback_names.append(d)
                if len(fallback_names) >= self._top_n:
                    break
            return QueryClassification(
                query=query, domains=fallback_names[: self._top_n],
                reasoning="no table hits — fell back to catalog defaults",
                source="fallback",
            )

        domains = [d.name for d in ranked[: self._top_n]]
        reasoning = self._format_reasoning(ranked, debug)
        payload = {"domains": domains, "reasoning": reasoning, "debug": debug}
        self._cache.put(key, payload)
        return QueryClassification(
            query=query, domains=domains, reasoning=reasoning,
            source="embedding", raw=payload,
        )

    # ── Scoring ────────────────────────────────────────────────────────────

    def _score(self, query: str) -> tuple[list[_DomainScore], dict[str, Any]]:
        # Signal 1 — retrieval. Each hit attributes its score to every
        # domain its table is tagged with (multi-label OK).
        hits = self._retriever.search(
            query, k=self._top_k_tables, hybrid=True, domains=None,
        )
        retrieval: dict[str, float] = {}
        retrieval_supports: dict[str, list[tuple[str, float]]] = {}
        for h in hits:
            for d in h.domains:
                retrieval[d] = retrieval.get(d, 0.0) + h.score
                retrieval_supports.setdefault(d, []).append((h.fqn, h.score))

        if not retrieval:
            return [], {"hits": []}

        # Signal 2 — entity resolution. One resolve, no domain scope.
        # Every confidently-pinned phrase contributes to its table's
        # declared domains. Phrases the resolver couldn't pin contribute
        # nothing — that's the right signal: the question doesn't
        # actually mention any value living in those tables.
        n_resolved: dict[str, int] = {}
        resolution_quality: dict[str, float] = {}
        resolved_supports: list[dict[str, Any]] = []
        if self._resolver is not None:
            try:
                rr = self._resolver.resolve(query)
                for phrase in rr.phrases:
                    if phrase.chosen is None:
                        continue
                    cand_domains = self._domains_by_fqn.get(phrase.chosen.fqn, ())
                    for d in cand_domains:
                        n_resolved[d] = n_resolved.get(d, 0) + 1
                        resolution_quality[d] = (
                            resolution_quality.get(d, 0.0) + phrase.chosen.score
                        )
                    resolved_supports.append({
                        "phrase": phrase.phrase,
                        "fqn": phrase.chosen.fqn,
                        "column": phrase.chosen.column,
                        "value": phrase.chosen.value,
                        "score": phrase.chosen.score,
                        "domains": list(cand_domains),
                    })
            except Exception as e:
                log.debug("entity-resolution boost failed: %s", e)

        # Combine. Normalize each signal by its own max so the alpha
        # mix isn't dominated by the signal that happens to be on a
        # larger numerical scale (retrieval scores are summed; entity
        # quality is a single-hit cosine).
        max_r = max(retrieval.values())
        max_e = max(resolution_quality.values()) if resolution_quality else 0.0
        candidates = sorted(retrieval.keys(), key=lambda d: -retrieval[d])

        scored: list[_DomainScore] = []
        for d in candidates:
            r_norm = retrieval[d] / max_r if max_r else 0.0
            e_norm = (resolution_quality.get(d, 0.0) / max_e) if max_e else 0.0
            combined = self._alpha * r_norm + (1.0 - self._alpha) * e_norm
            scored.append(_DomainScore(
                name=d,
                retrieval=retrieval[d],
                n_resolved=n_resolved.get(d, 0),
                resolution_quality=resolution_quality.get(d, 0.0),
                combined=combined,
            ))
        scored.sort(key=lambda s: -s.combined)

        debug = {
            "hits": [{"fqn": h.fqn, "score": h.score, "domains": h.domains} for h in hits],
            "resolved": resolved_supports,
            "alpha": self._alpha,
        }
        return scored, debug

    # ── Helpers ────────────────────────────────────────────────────────────

    def _cache_key(self, query: str) -> str:
        payload = {
            "q": query.strip().lower(),
            "fqns": sorted(self._domains_by_fqn.keys()),
            "domains": self._all_domain_names,
            "alpha": self._alpha,
            "k": self._top_k_tables,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest()

    @staticmethod
    def _format_reasoning(ranked: list[_DomainScore], debug: dict[str, Any]) -> str:
        bits = []
        for s in ranked[:3]:
            bits.append(
                f"{s.name} (retrieval={s.retrieval:.2f}, "
                f"resolved={s.n_resolved}, combined={s.combined:.2f})"
            )
        return "; ".join(bits)


# ── Tiny disk-backed cache (mirrors QueryDomainClassifier._Cache) ──────────


class _Cache:
    _MAX = 5000

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
        if len(self._data) > self._MAX:
            for k in list(self._data.keys())[: self._MAX // 10]:
                self._data.pop(k, None)
        self._flush()

    def _flush(self) -> None:
        if not self._path:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, sort_keys=True), encoding="utf-8")
