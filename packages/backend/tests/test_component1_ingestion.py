"""Component 1 — integration tests against real systems.

These tests hit:
  * GitHub raw (ApiModel.json + 0030-ForeignKeys.sql for DS 6.1.0)
  * Local Postgres (Ed-Fi populated docker container at 127.0.0.1:5432)
  * Azure OpenAI (chat + embeddings)

If a service isn't reachable, the test is skipped with a precise reason.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from text2sql.config import REPO_ROOT, load_config
from text2sql.ingestion.edfi_fetcher import (
    DS_610_EXPECTED,
    IngestionConfig,
    fetch_all,
    verify_manifest,
)


# ── Component 1.A: Pure GitHub ingestion ──────────────────────────────────────


def test_ingest_ds610_core_from_github(tmp_path: Path) -> None:
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    ic.cache_dir = tmp_path
    manifest = fetch_all(ic, force=True)

    assert manifest.data_standard_version == "6.1.0"
    assert len(manifest.artifacts) == 1
    core = manifest.artifacts[0]
    assert core.source == "core"
    assert core.api_model_path.exists()
    assert core.foreign_keys_sql_path.exists()
    assert core.api_model_path.stat().st_size > 1_000_000   # ApiModel.json is multi-MB
    assert core.foreign_keys_sql_path.stat().st_size > 100_000

    counts = manifest.counts["core"]
    for key, want in DS_610_EXPECTED.items():
        assert counts[key] == want, f"{key}: expected {want}, got {counts[key]}"


def test_verify_manifest_passes_on_ds610() -> None:
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = fetch_all(ic)
    verify_manifest(manifest)            # raises on mismatch


def test_manifest_round_trips() -> None:
    cfg = load_config()
    ic = IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT)
    manifest = fetch_all(ic)
    again = type(manifest).from_json(manifest.to_json())
    assert again.counts == manifest.counts
    assert again.data_standard_version == manifest.data_standard_version


# ── Component 1.B: Provider-layer health checks ───────────────────────────────


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_azure_openai_llm_roundtrip() -> None:
    from text2sql.providers import build_llm
    from text2sql.providers.base import LLMMessage

    cfg = load_config()
    llm = build_llm(cfg.llm.providers[cfg.llm.primary])
    out = llm.complete(
        [
            LLMMessage(role="system", content="You answer with a single integer."),
            LLMMessage(role="user", content="What is 2+2?"),
        ],
        max_tokens=10,
    )
    assert "4" in out


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_azure_openai_embedding_dimensions() -> None:
    from text2sql.providers import build_embedding

    cfg = load_config()
    emb = build_embedding(cfg.embeddings.providers[cfg.embeddings.primary])
    vecs = emb.embed(["StudentSchoolAssociation joins Student to School"])
    assert vecs.shape == (1, emb.dim)
    assert vecs.shape[1] == 3072      # text-embedding-3-large


# ── Component 1.C: Target SQL engine reachable ────────────────────────────────


def test_target_db_postgres_reachable() -> None:
    from text2sql.providers import build_sql_engine

    cfg = load_config()
    spec = cfg.target_db_provider()
    if spec.kind != "postgresql":
        pytest.skip(f"target_db dialect is {spec.kind}, not postgresql")
    try:
        engine = build_sql_engine(spec)
        tables = engine.list_tables()
    except Exception as e:
        pytest.skip(f"postgres unreachable: {e}")
    schemas = {s for s, _ in tables}
    assert "edfi" in schemas, f"edfi schema missing; saw {schemas}"
    assert len(tables) > 500, f"expected >500 tables, got {len(tables)}"
