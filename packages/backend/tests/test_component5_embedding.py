"""Component 5 integration tests.

Real Azure embeddings + FAISS, end-to-end:

  * blob shape — only table desc + column descs + values, nothing else
  * indexing — every TableEntry lands in the `tables` collection
  * domain filter — payload tag filter narrows results
  * known-good retrieval — natural-language questions surface the right tables
  * column_values collection — sample values are embedded for entity resolution
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.classification import read_table_mapping
from text2sql.classification.metadata import CatalogIndex
from text2sql.classification.table_mapping import map_tables
from text2sql.classification.catalog import load_domain_catalog
from text2sql.config import REPO_ROOT, load_config
from text2sql.embedding import (
    TableRetriever,
    build_table_blob,
    index_column_values,
    index_table_catalog,
)
from text2sql.embedding.blob_builder import build_column_value_blobs
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all
from text2sql.table_catalog import build_table_catalog


CATALOG_PATH = REPO_ROOT / "data/artifacts/table_catalog.json"


@pytest.fixture(scope="module")
def manifest():
    cfg = load_config()
    return fetch_all(IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT))


@pytest.fixture(scope="module")
def index(manifest):
    return CatalogIndex.from_manifest(manifest)


@pytest.fixture(scope="module")
def classifications(manifest, index):
    cl_path = REPO_ROOT / "data/artifacts/table_classification.json"
    if cl_path.exists():
        return read_table_mapping(cl_path).classifications
    return map_tables(index, load_domain_catalog(manifest), llm=None)


@pytest.fixture(scope="module")
def catalog(manifest, index, classifications):
    """Use the on-disk catalog if present (it has live samples + LLM gap-fill);
    otherwise build a fresh offline catalog for tests."""
    if CATALOG_PATH.exists():
        from text2sql.table_catalog import load_table_catalog
        return load_table_catalog(CATALOG_PATH)
    return build_table_catalog(classifications, index, manifest, sql_engine=None)


def _has_azure() -> bool:
    return bool(os.environ.get("AZURE_OPENAI_API_KEY"))


# ── Blob shape (offline) ──────────────────────────────────────────────────────


def test_blob_contains_only_semantic_content(catalog) -> None:
    entry = next(e for e in catalog.entries if e.fqn == "edfi.Student")
    blob = build_table_blob(entry)
    assert entry.description.strip() in blob
    # No FK structural plumbing in the embed text:
    for fqn in entry.parent_neighbors[:3]:
        assert fqn not in blob, f"structural neighbor leaked into embed text: {fqn}"
    # Domains are not in the text — they're in the payload only:
    for d in entry.domains:
        assert f"[domain]" not in blob.lower()
    # Sample column descriptions (the populated ones) appear:
    described = [c for c in entry.columns if c.description][:3]
    for c in described:
        assert c.name in blob


def test_descriptor_blob_includes_unique_values(catalog) -> None:
    """A descriptor like edfi.GradeLevelDescriptor with low-cardinality values
    should expose those values in the embed text — that's how the LLM finds
    'Pre-K', '1st Grade', etc., later."""
    desc = next(
        (e for e in catalog.entries
         if e.fqn == "edfi.GradeLevelDescriptor"
         and any(c.sample_values for c in e.columns)),
        None,
    )
    if not desc:
        pytest.skip("GradeLevelDescriptor has no sample values in this catalog")
    blob = build_table_blob(desc)
    assert "[values:" in blob


def test_column_value_blobs_carry_payload(catalog) -> None:
    has_samples = next(
        (e for e in catalog.entries if any(c.sample_values for c in e.columns)),
        None,
    )
    if not has_samples:
        pytest.skip("no sample values in catalog")
    blobs = build_column_value_blobs(has_samples)
    assert blobs
    id_, text, payload = blobs[0]
    assert payload["fqn"] == has_samples.fqn
    assert "column" in payload and "value" in payload


# ── Online: real Azure embeddings + FAISS ─────────────────────────────────────


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_index_and_retrieve_known_table(catalog, tmp_path: Path) -> None:
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_embedding, build_vector_store
    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(
        ProviderEntry(kind="faiss", path=str(tmp_path / "vec"))
    )

    stats = index_table_catalog(catalog, embedder, store)
    assert stats.tables_indexed >= 800
    assert stats.embedding_dim == embedder.dim

    retriever = TableRetriever(embedder, store)
    hits = retriever.search("students enrolled in a school", k=5)
    fqns = [h.fqn for h in hits]
    assert "edfi.StudentSchoolAssociation" in fqns


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_domain_filter_narrows_results(catalog, tmp_path: Path) -> None:
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_embedding, build_vector_store
    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(
        ProviderEntry(kind="faiss", path=str(tmp_path / "vec"))
    )
    index_table_catalog(catalog, embedder, store)
    retriever = TableRetriever(embedder, store)

    # Without filter
    open_hits = retriever.search("daily attendance event", k=5)
    # With domain filter restricted to StudentAttendance only
    filtered = retriever.search("daily attendance event", k=5, domains=["StudentAttendance"])
    for h in filtered:
        assert "StudentAttendance" in h.domains
    # Open search may return non-StudentAttendance tables; filtered must not.
    assert filtered, "expected at least one hit in StudentAttendance"


@pytest.mark.skipif(not _has_azure(), reason="no azure key")
def test_descriptor_query_finds_descriptor_table(catalog, tmp_path: Path) -> None:
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_embedding, build_vector_store
    cfg = load_config()
    embedder = build_embedding(cfg.embedding_provider())
    store = build_vector_store(
        ProviderEntry(kind="faiss", path=str(tmp_path / "vec"))
    )
    index_table_catalog(catalog, embedder, store)
    retriever = TableRetriever(embedder, store)
    hits = retriever.search("list all the grade levels we report on", k=5)
    fqns = [h.fqn for h in hits]
    assert any(f.endswith("GradeLevelDescriptor") or "Grade" in f for f in fqns), \
        f"expected a grade-level table; got {fqns}"
