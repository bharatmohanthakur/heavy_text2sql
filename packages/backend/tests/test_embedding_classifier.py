"""Search-and-score domain classifier tests.

Pins three behaviors:

  1. Pure retrieval signal — the domain whose tables score highest in
     the table-vector search wins, even with no entity resolver.
  2. Entity-resolution boost — when phrase resolution lands on a
     specific table, that table's domain gets credit, breaking ties
     between near-equal retrieval scores.
  3. Cache hit — second classify() with same query returns
     `source="cache"` and never touches the retriever.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from text2sql.classification.embedding_classifier import EmbeddingDomainClassifier


# ── Stubs (no real embeddings / vector store) ──────────────────────────────


@dataclass
class _Hit:
    fqn: str
    score: float
    domains: list
    is_descriptor: bool = False
    text: str = ""


class _StubRetriever:
    """Returns canned hits keyed by query substring. Supports
    counting calls so the cache test can assert no second hit."""

    def __init__(self, hits_by_keyword: dict[str, list[_Hit]]) -> None:
        self._hits = hits_by_keyword
        self.calls: list[str] = []

    def search(self, query: str, *, k: int = 8, domains=None, hybrid=True):
        self.calls.append(query)
        for kw, hits in self._hits.items():
            if kw.lower() in query.lower():
                return hits
        return []


@dataclass
class _Cand:
    fqn: str
    column: str
    value: str
    score: float
    tier: str = "vector"


@dataclass
class _Phrase:
    phrase: str
    chosen: object | None
    candidates: list = None


@dataclass
class _Resolution:
    query: str
    phrases: list


class _StubResolver:
    """Returns predetermined phrase resolutions."""

    def __init__(self, phrases: list[_Phrase]) -> None:
        self._phrases = phrases
        self.calls: list[str] = []

    def resolve(self, query: str, **_):
        self.calls.append(query)
        return _Resolution(query=query, phrases=self._phrases)


@dataclass
class _CatalogEntry:
    fqn: str
    domains: list[str]


@dataclass
class _Catalog:
    entries: list[_CatalogEntry]


# ── Tests ──────────────────────────────────────────────────────────────────


def _catalog():
    return _Catalog([
        _CatalogEntry("edfi.Student", ["Student"]),
        _CatalogEntry("edfi.School", ["EducationOrganization"]),
        _CatalogEntry("edfi.StudentSchoolAssociation", ["Enrollment"]),
        _CatalogEntry("edfi.Staff", ["Staff"]),
    ])


def test_retrieval_only_picks_highest_scoring_domain():
    """No entity resolver — domain ranking is purely the sum of
    retrieval scores per domain. Highest-scored hit's domain wins."""
    retriever = _StubRetriever({
        "Northridge": [
            _Hit(fqn="edfi.School", score=0.92, domains=["EducationOrganization"]),
            _Hit(fqn="edfi.StudentSchoolAssociation", score=0.41, domains=["Enrollment"]),
        ],
    })
    cls = EmbeddingDomainClassifier(retriever, _catalog())  # no resolver
    out = cls.classify("students at Northridge High")

    assert out.source == "embedding"
    assert out.domains[0] == "EducationOrganization"
    assert "Enrollment" in out.domains


def test_entity_resolution_boosts_matching_domain():
    """Two near-equal retrieval scores; entity resolution pins one
    phrase to a specific table → its domain gets the tiebreaker."""
    retriever = _StubRetriever({
        "Hispanic": [
            _Hit(fqn="edfi.Student",       score=0.50, domains=["Student"]),
            _Hit(fqn="edfi.RaceDescriptor", score=0.50, domains=["EducationOrganization"]),
        ],
    })
    # Entity resolver pins "Hispanic" to RaceDescriptor — that boosts
    # EducationOrganization above the otherwise tied Student.
    resolver = _StubResolver([
        _Phrase(
            phrase="Hispanic",
            chosen=_Cand(fqn="edfi.RaceDescriptor", column="CodeValue", value="Hispanic", score=0.95),
            candidates=[],
        ),
    ])
    cls = EmbeddingDomainClassifier(
        # The catalog must know edfi.RaceDescriptor for the boost to attribute.
        retriever,
        _Catalog([
            _CatalogEntry("edfi.Student", ["Student"]),
            _CatalogEntry("edfi.RaceDescriptor", ["EducationOrganization"]),
        ]),
        entity_resolver=resolver,
        alpha=0.5,   # equal weight on retrieval and resolution so the boost
                     # actually breaks the tie.
    )
    out = cls.classify("how many Hispanic students")
    assert out.domains[0] == "EducationOrganization"
    assert resolver.calls, "entity resolver must have been consulted"


def test_cache_hit_skips_retriever_on_repeat(tmp_path: Path):
    retriever = _StubRetriever({
        "Northridge": [_Hit(fqn="edfi.School", score=0.9, domains=["EducationOrganization"])],
    })
    cls = EmbeddingDomainClassifier(
        retriever, _catalog(), cache_path=tmp_path / "qcc.json",
    )

    first = cls.classify("students at Northridge")
    assert first.source == "embedding"
    assert len(retriever.calls) == 1

    second = cls.classify("students at Northridge")
    assert second.source == "cache"
    assert second.domains == first.domains
    # Retriever was NOT consulted again — that's the cache contract.
    assert len(retriever.calls) == 1


def test_no_hits_falls_back_to_catalog_defaults():
    """Empty retrieval (live DB had nothing matching) shouldn't crash —
    classifier falls back to catalog domains in size order so
    downstream layers still get something routable."""
    retriever = _StubRetriever({"impossible": []})
    cls = EmbeddingDomainClassifier(retriever, _catalog())
    out = cls.classify("query that matches nothing")
    assert out.source == "fallback"
    assert out.domains   # at least one default domain
