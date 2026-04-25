"""Top-level resolver — orchestrates the 4-tier funnel.

Public surface:
  EntityResolver.resolve(query, *, domains=None, column_scope=None) -> ResolutionResult
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from text2sql.entity_resolution.extract import extract_phrases
from text2sql.entity_resolution.tiers import (
    tier1_exact,
    tier2_fuzzy,
    tier3_vector,
    tier4_llm,
)
from text2sql.entity_resolution.value_index import ValueIndex, ValueRecord
from text2sql.providers.base import EmbeddingProvider, LLMProvider, VectorStore


@dataclass
class EntityCandidate:
    fqn: str
    column: str
    value: str
    score: float
    tier: str             # "exact" | "fuzzy" | "vector" | "llm"
    # Descriptor join-chain hints, populated only when the value is a
    # descriptor code. Lets the schema linker assemble the right two-hop join.
    descriptor_type: str = ""        # e.g. "OldEthnicityDescriptor"
    child_fqn: str = ""              # e.g. "edfi.OldEthnicityDescriptor"
    descriptor_id: int | None = None


@dataclass
class PhraseResolution:
    phrase: str
    candidates: list[EntityCandidate]
    chosen: EntityCandidate | None

    def best(self) -> EntityCandidate | None:
        return self.chosen


@dataclass
class ResolutionResult:
    query: str
    phrases: list[PhraseResolution]

    def all_chosen(self) -> list[EntityCandidate]:
        return [p.chosen for p in self.phrases if p.chosen]

    def by_phrase(self) -> dict[str, EntityCandidate | None]:
        return {p.phrase: p.chosen for p in self.phrases}


class EntityResolver:
    def __init__(
        self,
        index: ValueIndex,
        embedder: EmbeddingProvider | None = None,
        store: VectorStore | None = None,
        llm: LLMProvider | None = None,
        *,
        fuzzy_floor: float = 0.75,
        vector_floor: float = 0.78,
        llm_delta: float = 0.05,
    ) -> None:
        self._index = index
        self._embedder = embedder
        self._store = store
        self._llm = llm
        self._fuzzy_floor = fuzzy_floor
        self._vector_floor = vector_floor
        self._llm_delta = llm_delta

    # ── Single phrase ─────────────────────────────────────────────────────────

    def resolve_phrase(
        self,
        phrase: str,
        *,
        domains: list[str] | None = None,
        column_scope: list[tuple[str, str]] | None = None,
    ) -> PhraseResolution:
        # Tier 1
        t1 = tier1_exact(phrase, self._index)
        if t1:
            chosen = self._top_to_candidate(t1[0], "exact")
            return PhraseResolution(
                phrase=phrase,
                candidates=[self._top_to_candidate(x, "exact") for x in t1[:5]],
                chosen=chosen,
            )

        # Tier 2 — domain-scoped pool when available
        pool: Iterable[ValueRecord] | None = None
        if column_scope:
            pool = []
            for fqn, col in column_scope:
                pool.extend(self._index.by_column(fqn, col))
        elif domains:
            pool = self._index.in_domains(domains)
        t2 = tier2_fuzzy(phrase, self._index, candidates=pool)
        if t2 and t2[0][1] >= self._fuzzy_floor:
            top = t2[0]
            second = t2[1] if len(t2) > 1 else None
            ambiguous = (
                second is not None
                and (top[1] - second[1]) < self._llm_delta
            )
            if ambiguous and self._llm is not None:
                chosen = self._llm_pick(phrase, t2[:5], "fuzzy")
            else:
                chosen = self._top_to_candidate(top, "fuzzy")
            return PhraseResolution(
                phrase=phrase,
                candidates=[self._top_to_candidate(x, "fuzzy") for x in t2[:5]],
                chosen=chosen,
            )

        # Tier 3 — vector ANN on column_values
        if self._embedder is not None and self._store is not None:
            t3 = tier3_vector(
                phrase, self._embedder, self._store,
                column_scope=column_scope,
                domain_scope=domains,
            )
            if t3 and t3[0][1] >= self._vector_floor:
                top = t3[0]
                second = t3[1] if len(t3) > 1 else None
                ambiguous = (
                    second is not None
                    and (top[1] - second[1]) < self._llm_delta
                )
                if ambiguous and self._llm is not None:
                    chosen = self._llm_pick(phrase, t3[:5], "vector")
                else:
                    chosen = self._top_to_candidate(top, "vector")
                return PhraseResolution(
                    phrase=phrase,
                    candidates=[self._top_to_candidate(x, "vector") for x in t3[:5]],
                    chosen=chosen,
                )

        # No tier found a confident match.
        all_cands: list[tuple[ValueRecord, float]] = []
        if t2:
            all_cands.extend(t2[:3])
        if self._embedder is not None and self._store is not None:
            t3_open = tier3_vector(phrase, self._embedder, self._store, k=5)
            all_cands.extend(t3_open)
        all_cands.sort(key=lambda x: x[1], reverse=True)
        return PhraseResolution(
            phrase=phrase,
            candidates=[self._top_to_candidate(x, "fuzzy") for x in all_cands[:5]],
            chosen=None,
        )

    # ── Whole query ───────────────────────────────────────────────────────────

    def resolve(
        self,
        query: str,
        *,
        domains: list[str] | None = None,
        column_scope: list[tuple[str, str]] | None = None,
    ) -> ResolutionResult:
        phrases = extract_phrases(query)
        out: list[PhraseResolution] = []
        for p in phrases:
            out.append(self.resolve_phrase(
                p, domains=domains, column_scope=column_scope,
            ))
        return ResolutionResult(query=query, phrases=out)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _top_to_candidate(
        self, hit: tuple[ValueRecord, float], tier: str
    ) -> EntityCandidate:
        rec, score = hit
        return EntityCandidate(
            fqn=rec.fqn, column=rec.column, value=rec.value,
            score=float(score), tier=tier,
            descriptor_type=rec.descriptor_type,
            child_fqn=rec.child_fqn,
            descriptor_id=rec.descriptor_id,
        )

    def _llm_pick(
        self,
        phrase: str,
        candidates: list[tuple[ValueRecord, float]],
        from_tier: str,
    ) -> EntityCandidate | None:
        if not self._llm or not candidates:
            return None
        chosen = tier4_llm(phrase, candidates, self._llm)
        if not chosen:
            return None
        return self._top_to_candidate(chosen, "llm")
