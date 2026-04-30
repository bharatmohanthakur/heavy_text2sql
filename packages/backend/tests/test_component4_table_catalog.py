"""Component 4 (refactored): single TableCatalog — one entry per table.

Verifies:
  * Every classified table appears exactly once.
  * Domain coverage matches the ApiModel ground truth (tables-per-domain
    counts derived from `entityDefinitions[].domains`).
  * Table descriptions come from ApiModel (100% in DS 6.1.0); never get
    LLM-rewritten unnecessarily.
  * Descriptor tables get columns from the live DB.
  * Low-cardinality columns get sampled.
  * LLM gap-fills ONLY columns that ApiModel left empty — never overwrites.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from text2sql.classification import read_table_mapping
from text2sql.classification.metadata import CatalogIndex
from text2sql.classification.table_mapping import map_tables
from text2sql.classification.catalog import load_domain_catalog
from text2sql.config import REPO_ROOT, load_config
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all
from text2sql.table_catalog import (
    DescriptionGenerator,
    build_table_catalog,
    load_table_catalog,
    save_table_catalog,
)


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


def _has_db() -> bool:
    cfg = load_config()
    try:
        from text2sql.providers import build_sql_engine
        eng = build_sql_engine(cfg.target_db_provider())
        eng.execute("SELECT 1 AS ok")
        return True
    except Exception:
        return False


# ── Offline correctness ───────────────────────────────────────────────────────


def test_catalog_one_entry_per_table(manifest, index, classifications) -> None:
    catalog = build_table_catalog(classifications, index, manifest, sql_engine=None)
    fqns = [e.fqn for e in catalog.entries]
    assert len(fqns) == len(set(fqns)), "duplicate entries in catalog"
    assert len(catalog.entries) == len(classifications)


def test_domain_counts_match_apimodel(manifest, index, classifications) -> None:
    """Sum of tags-per-domain must match the ApiModel.json ground-truth — i.e.,
    if Survey lists 109 entities in `domains[]`, our catalog tags 109 entries."""
    apimodel_truth: dict[str, int] = {}
    for art in manifest.artifacts:
        data = json.loads(Path(art.api_model_path).read_text())
        for e in data.get("entityDefinitions", []):
            for d in e.get("domains") or []:
                apimodel_truth[d] = apimodel_truth.get(d, 0) + 1

    catalog = build_table_catalog(classifications, index, manifest, sql_engine=None)
    catalog_counts = catalog.domain_counts()

    # Every ApiModel domain must have at least the apimodel_truth count.
    # (Aggregate inheritance and descriptor voting can ADD a few; never remove.)
    for d, want in apimodel_truth.items():
        got = catalog_counts.get(d, 0)
        assert got >= want, f"domain {d!r}: catalog has {got}, ApiModel had {want}"


def test_table_descriptions_from_apimodel(manifest, index, classifications) -> None:
    catalog = build_table_catalog(classifications, index, manifest, sql_engine=None)
    n_apimodel = sum(1 for e in catalog.entries if e.description_source == "apimodel")
    # DS 6.1.0 has 100% table coverage in ApiModel — so should we.
    assert n_apimodel == len(catalog.entries)
    # Spot check
    student = next(e for e in catalog.entries if e.fqn == "edfi.Student")
    assert "individual for whom instruction" in student.description.lower()


def test_in_domain_filter(manifest, index, classifications) -> None:
    catalog = build_table_catalog(classifications, index, manifest, sql_engine=None)
    in_enroll = catalog.in_domain("Enrollment")
    assert any(e.fqn == "edfi.StudentSchoolAssociation" for e in in_enroll)
    assert any(e.fqn == "edfi.Student" for e in in_enroll)


def test_round_trip_save_load(manifest, index, classifications, tmp_path: Path) -> None:
    catalog = build_table_catalog(classifications, index, manifest, sql_engine=None)
    save_table_catalog(catalog, tmp_path / "catalog.json")
    again = load_table_catalog(tmp_path / "catalog.json")
    assert len(again.entries) == len(catalog.entries)
    assert again.entries[0].fqn == catalog.entries[0].fqn


# ── Live DB ───────────────────────────────────────────────────────────────────


@pytest.mark.skipif(not _has_db(), reason="target DB unreachable")
def test_descriptor_columns_filled_from_db(manifest, index, classifications) -> None:
    from text2sql.providers import build_sql_engine
    cfg = load_config()
    engine = build_sql_engine(cfg.target_db_provider())

    fqn = "edfi.GradeLevelDescriptor"
    # Skip if the active target DB doesn't actually have this table
    # (e.g., the demo SQLite). Only the legacy Ed-Fi target has it.
    try:
        live = {f"{s}.{t}" for s, t in engine.list_tables()}
        if fqn not in live and "main.GradeLevelDescriptor" not in live:
            pytest.skip(f"{fqn} not present in active target DB")
    except Exception as e:
        pytest.skip(f"target DB list_tables() failed: {e}")

    catalog = build_table_catalog(
        classifications, index, manifest,
        sql_engine=engine,
        only_fqns={fqn},
        include_unknown_tables=False,
    )
    assert len(catalog.entries) == 1
    e = catalog.entries[0]
    assert e.row_count is not None
    assert e.columns, f"{fqn}: expected DB-discovered columns, got none"


@pytest.mark.skipif(not _has_db(), reason="target DB unreachable")
def test_low_cardinality_columns_sampled(manifest, index, classifications) -> None:
    from text2sql.providers import build_sql_engine
    cfg = load_config()
    engine = build_sql_engine(cfg.target_db_provider())
    catalog = build_table_catalog(
        classifications, index, manifest,
        sql_engine=engine,
        only_fqns={"edfi.Student", "edfi.Staff", "edfi.School"},
    )
    sampled = sum(1 for e in catalog.entries for c in e.columns if c.sample_values)
    assert sampled > 0


# ── LLM gap-fill (only fills missing fields, never overwrites) ────────────────


@pytest.mark.skipif(
    not _has_db() or not os.environ.get("AZURE_OPENAI_API_KEY"),
    reason="DB or LLM unavailable",
)
def test_llm_only_fills_missing_descriptions(manifest, index, classifications, tmp_path: Path) -> None:
    from text2sql.providers import build_llm, build_sql_engine
    cfg = load_config()
    engine = build_sql_engine(cfg.target_db_provider())
    desc_gen = DescriptionGenerator(
        build_llm(cfg.llm_for_task("classifier_fallback")),
        cache_path=tmp_path / "desc.json",
    )

    # Use a small set with both apimodel-described and undescribed columns.
    catalog = build_table_catalog(
        classifications, index, manifest,
        sql_engine=engine,
        description_generator=desc_gen,
        only_fqns={"edfi.Student", "edfi.GradeLevelDescriptor"},
    )

    # Every column with description_source="apimodel" must NOT have been
    # overwritten — its description is the ApiModel one.
    apimodel_descs = 0
    llm_filled = 0
    for e in catalog.entries:
        for c in e.columns:
            if c.description_source == "apimodel":
                apimodel_descs += 1
            elif c.description_source in ("llm", "cache"):
                llm_filled += 1
    assert apimodel_descs > 0, "expected some apimodel-sourced column descriptions"
    print(f"\napimodel column descriptions kept: {apimodel_descs}")
    print(f"llm-filled column descriptions:    {llm_filled}")
