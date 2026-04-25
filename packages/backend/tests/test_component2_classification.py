"""Component 2 integration tests.

2a — Table-to-domain mapping (deterministic; ≤1 LLM call expected on DS 6.1.0).
2b — Query domain classifier (requires Azure OpenAI; skipped without key).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.classification import (
    QueryDomainClassifier,
    load_domain_catalog,
    map_tables,
    read_table_mapping,
    write_table_mapping,
)
from text2sql.classification.metadata import CatalogIndex
from text2sql.config import REPO_ROOT, load_config
from text2sql.ingestion.edfi_fetcher import IngestionConfig, IngestionManifest, fetch_all


@pytest.fixture(scope="module")
def manifest() -> IngestionManifest:
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    return fetch_all(ic)


@pytest.fixture(scope="module")
def catalog(manifest):
    return load_domain_catalog(manifest)


@pytest.fixture(scope="module")
def index(manifest):
    return CatalogIndex.from_manifest(manifest)


# ── 2a: table-to-domain mapping ────────────────────────────────────────────────


def test_table_mapping_full_coverage(manifest, catalog, index, tmp_path: Path) -> None:
    classifications = map_tables(
        index, catalog,
        llm=None,        # don't make any LLM calls in this test
        overrides_path=REPO_ROOT / "configs" / "domain_overrides.yaml",
    )
    assert len(classifications) == len(index.tables) == 829

    counts: dict[str, int] = {}
    for c in classifications:
        counts[c.source] = counts.get(c.source, 0) + 1
    # Expectations from the live DS 6.1.0 ApiModel.json:
    assert counts["apimodel"] == 750
    assert counts["aggregate_inheritance"] >= 70   # ~75
    # 3 descriptors should resolve via descriptor_voting OR llm fallback
    assert counts.get("descriptor_voting", 0) + counts.get("llm", 0) >= 3
    # No table left unclassified
    assert all(c.primary_domain for c in classifications)


def test_table_mapping_known_examples(manifest, catalog, index) -> None:
    classifications = map_tables(index, catalog, llm=None)
    by_fqn = {c.fqn: c for c in classifications}

    student = by_fqn["edfi.Student"]
    assert student.primary_domain == "AlternativeAndSupplementalServices"   # first in domains[]
    assert student.source == "apimodel"

    ssa = by_fqn["edfi.StudentSchoolAssociation"]
    assert ssa.primary_domain == "Enrollment"
    assert ssa.source == "apimodel"

    # GradeLevelDescriptor inherits via descriptor_voting (no domains[] in ApiModel)
    gl = by_fqn["edfi.GradeLevelDescriptor"]
    assert gl.is_descriptor
    assert gl.primary_domain != "Other"


def test_table_mapping_round_trip(manifest, catalog, index, tmp_path: Path) -> None:
    classifications = map_tables(index, catalog, llm=None)
    out = tmp_path / "table_classification.json"
    output = write_table_mapping(
        out, classifications,
        data_standard_version=manifest.data_standard_version,
        catalog=catalog,
    )
    again = read_table_mapping(out)
    assert again.summary["total"] == output.summary["total"]
    assert again.classifications[0].fqn == output.classifications[0].fqn


# ── 2b: query domain classifier ────────────────────────────────────────────────


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_query_classifier_simple_enrollment(manifest, catalog, tmp_path: Path) -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    qc = QueryDomainClassifier(llm, catalog, cache_path=tmp_path / "qc.json")

    out = qc.classify("How many students enrolled in Grade 9 last year?")
    assert out.source in ("llm", "cache")
    assert out.primary == "Enrollment", f"primary should be Enrollment; got {out.primary}"


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_query_classifier_cross_cutting(manifest, catalog, tmp_path: Path) -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    qc = QueryDomainClassifier(llm, catalog, cache_path=tmp_path / "qc.json")

    out = qc.classify(
        "How many Hispanic students were absent from Algebra I this semester?"
    )
    top = out.domains
    # Should hit attendance + demographics-or-enrollment
    assert "StudentAttendance" in top, f"expected StudentAttendance in {top}"
    assert any(
        d in top for d in ("StudentIdentificationAndDemographics", "Enrollment")
    ), f"expected demographics or enrollment in {top}"


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_query_classifier_cache_hit(manifest, catalog, tmp_path: Path) -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    cache = tmp_path / "qc.json"
    qc = QueryDomainClassifier(llm, catalog, cache_path=cache)

    q = "List all schools and their grade ranges"
    first = qc.classify(q)
    assert first.source == "llm"
    second = qc.classify(q)
    assert second.source == "cache"
    assert first.domains == second.domains


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_query_classifier_rejects_invented_domains(manifest, catalog, tmp_path: Path) -> None:
    from text2sql.providers import build_llm
    cfg = load_config()
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    qc = QueryDomainClassifier(llm, catalog, cache_path=tmp_path / "qc.json")

    out = qc.classify("Show me the sports teams and their seasons")
    valid = {d.name for d in catalog.domains}
    for n in out.domains:
        assert n in valid, f"hallucinated domain: {n}"
