"""Component 6 integration tests.

Real value index built from data/artifacts/table_catalog.json, real Azure
embeddings + FAISS for tier 3, real GPT-4o for tier 4 disambiguation.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.entity_resolution import (
    EntityResolver,
    build_value_index,
)
from text2sql.entity_resolution.extract import extract_phrases
from text2sql.entity_resolution.tiers import tier1_exact, tier2_fuzzy
from text2sql.entity_resolution.value_index import ValueIndex
from text2sql.table_catalog import load_table_catalog


CATALOG_PATH = REPO_ROOT / "data/artifacts/table_catalog.json"


@pytest.fixture(scope="module")
def value_index() -> ValueIndex:
    if not CATALOG_PATH.exists():
        pytest.skip("no table catalog; run `text2sql build-table-catalog-cmd` first")
    catalog = load_table_catalog(CATALOG_PATH)
    return build_value_index(catalog)


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


# ── Phrase extraction ────────────────────────────────────────────────────────


def test_extract_capitalized_phrases() -> None:
    phrases = extract_phrases(
        "How many Hispanic students were absent from Algebra I last semester?"
    )
    # Captures Hispanic + Algebra (the "I" gets folded into multi-word "Algebra I"
    # via the cap-word regex; "How" is an interrogative we filter)
    assert any("Hispanic" in p for p in phrases)
    assert any("Algebra" in p for p in phrases)


def test_extract_drops_interrogatives() -> None:
    phrases = extract_phrases("how many students enrolled in 2024")
    # 'how' shouldn't survive — it's in the stop-list.
    assert "how" not in [p.lower() for p in phrases]


def test_extract_quoted_phrase() -> None:
    phrases = extract_phrases('Show me students with grade level "Pre-K"')
    assert "Pre-K" in phrases


# ── Tier 1: exact match ──────────────────────────────────────────────────────


def test_tier1_exact_match_on_real_value(value_index: ValueIndex) -> None:
    # Find any value that appears in the index — pick a deterministic example
    # by scanning the records.
    sample = next(
        (r for r in value_index.records if r.value == "Tardy"),
        None,
    ) or value_index.records[0]
    matches = tier1_exact(sample.value, value_index)
    assert matches
    assert matches[0][0].value.lower() == sample.value.lower()
    assert matches[0][1] == 1.0


def test_tier1_misses_unknown(value_index: ValueIndex) -> None:
    matches = tier1_exact("NotAValueThatExists_xxxxx", value_index)
    assert matches == []


# ── Tier 2: fuzzy ─────────────────────────────────────────────────────────────


def test_tier2_fuzzy_misspelling(value_index: ValueIndex) -> None:
    # In the populated DB, ethnicity descriptors include "Hispanic or Latino"
    # — verify a misspelling resolves.
    has_hispanic = any("Hispanic" in r.value for r in value_index.records)
    if not has_hispanic:
        pytest.skip("no Hispanic-related values in this catalog")
    out = tier2_fuzzy("Hispanc", value_index)
    assert out
    top = out[0]
    assert "Hispanic" in top[0].value
    assert top[1] >= 0.6


# ── Resolver end-to-end (real LLM + vector) ──────────────────────────────────


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_resolver_end_to_end_misspelled_demographics(value_index: ValueIndex) -> None:
    cfg = load_config()
    from text2sql.providers import build_embedding, build_llm, build_vector_store
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))

    resolver = EntityResolver(value_index, embedder=embedder, store=store, llm=llm)
    result = resolver.resolve("Show students who are Hispanc")
    assert result.phrases
    chosen = [p for p in result.phrases if p.chosen]
    # We must have resolved at least one phrase (Hispanc → Hispanic)
    assert chosen, f"no phrase resolved; got {result.phrases}"
    hispanic_hit = next(
        (c.chosen for c in result.phrases
         if c.chosen and "Hispanic" in c.chosen.value),
        None,
    )
    assert hispanic_hit is not None, "expected Hispanic resolution"
    assert hispanic_hit.tier in ("exact", "fuzzy", "vector", "llm")


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_resolver_returns_unresolved_for_garbage(value_index: ValueIndex) -> None:
    cfg = load_config()
    from text2sql.providers import build_embedding, build_vector_store
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(cfg.vector_store_provider())

    resolver = EntityResolver(value_index, embedder=embedder, store=store)
    # An invented term should not produce a high-confidence resolution.
    result = resolver.resolve("students with FlibbertyJibbet status")
    if not result.phrases:
        return    # extractor could not identify candidates — acceptable
    for ph in result.phrases:
        if ph.phrase.lower() == "flibbertyjibbet":
            assert ph.chosen is None, f"junk phrase resolved: {ph.chosen}"
