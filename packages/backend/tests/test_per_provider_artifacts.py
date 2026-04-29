"""Step N1 — per-provider artifact path resolution.

Bedrock for the multi-target story: each target_db provider gets an
isolated artifact directory under data/artifacts/per_provider/<name>/.
Catalog, FK graph, APSP, FAISS, Steiner cache, and build manifest all
live there. Reading code falls back to the legacy flat layout when the
per-provider file is missing, so upgrades don't break in-place.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from text2sql.config import REPO_ROOT, AppConfig, resolve_artifact_path


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _minimal_cfg(primary: str = "demo-pg") -> AppConfig:
    """The smallest AppConfig that load_config would produce — enough to
    exercise the per-provider path helpers without a real YAML file."""
    return AppConfig.model_validate({
        "ed_fi": {
            "data_standard_version": "6.1.0",
            "extensions": [],
            "cache_dir": "data/edfi",
            "artifact_dir": "data/artifacts",
            "github": {
                "ods_repo": "Ed-Fi-Alliance-OSS/Ed-Fi-ODS",
                "extensions_repo": "Ed-Fi-Alliance-OSS/Ed-Fi-ODS-Implementation",
            },
        },
        "llm": {"primary": "x", "providers": {"x": {"kind": "openai"}}},
        "embeddings": {"primary": "x", "providers": {"x": {"kind": "openai"}}},
        "vector_store": {"primary": "x", "providers": {"x": {"kind": "faiss"}}},
        "target_db": {
            "primary": primary,
            "providers": {primary: {"kind": "postgresql"}},
        },
        "metadata_db": {"kind": "postgresql"},
        "logging": {"level": "INFO", "format": "json"},
    })


# ── per_provider_artifact_dir ───────────────────────────────────────────────


def test_per_provider_dir_uses_active_primary_by_default():
    cfg = _minimal_cfg(primary="my-pg")
    p = cfg.per_provider_artifact_dir()
    assert p == REPO_ROOT / "data/artifacts/per_provider/my-pg"


def test_per_provider_dir_accepts_explicit_name():
    """An operator can rebuild artifacts for a non-active provider — useful
    when prepping a new connector before flipping primary."""
    cfg = _minimal_cfg(primary="prod")
    p = cfg.per_provider_artifact_dir("staging")
    assert p == REPO_ROOT / "data/artifacts/per_provider/staging"


def test_per_provider_dirs_are_disjoint_for_different_providers():
    cfg = _minimal_cfg()
    a = cfg.per_provider_artifact_dir("a")
    b = cfg.per_provider_artifact_dir("b")
    assert a != b
    # No path-traversal: a provider name with `/` would land where it
    # asks (we don't sanitize here — providers come from validated YAML
    # / runtime_overrides). Document the contract.
    assert a.parent == b.parent  # same per_provider/ root


# ── resolve_artifact_path ───────────────────────────────────────────────────


def test_resolve_write_always_returns_per_provider_path(tmp_path, monkeypatch):
    """Writes ALWAYS go to the per-provider layout, even if a flat file
    exists. Otherwise we'd silently keep growing two parallel datasets."""
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")
    # Plant a legacy flat file
    flat = tmp_path / "data/artifacts/table_catalog.json"
    flat.parent.mkdir(parents=True, exist_ok=True)
    flat.write_text("{}")

    out = resolve_artifact_path(cfg, "table_catalog.json", write=True)
    assert out == tmp_path / "data/artifacts/per_provider/prod/table_catalog.json"
    # Caller is responsible for mkdir
    assert not out.parent.exists()


def test_resolve_read_prefers_per_provider_when_present(tmp_path, monkeypatch):
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")
    pp = tmp_path / "data/artifacts/per_provider/prod/table_catalog.json"
    pp.parent.mkdir(parents=True, exist_ok=True)
    pp.write_text('{"source": "per_provider"}')
    flat = tmp_path / "data/artifacts/table_catalog.json"
    flat.parent.mkdir(parents=True, exist_ok=True)
    flat.write_text('{"source": "flat"}')

    out = resolve_artifact_path(cfg, "table_catalog.json")
    assert out == pp
    assert json.loads(out.read_text())["source"] == "per_provider"


def test_resolve_read_falls_back_to_flat_when_per_provider_missing(tmp_path, monkeypatch):
    """Backwards-compat: existing single-target deployments have flat
    artifacts. Reads must find them; writes will migrate to per-provider."""
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")
    flat = tmp_path / "data/artifacts/table_catalog.json"
    flat.parent.mkdir(parents=True, exist_ok=True)
    flat.write_text('{"source": "flat"}')

    out = resolve_artifact_path(cfg, "table_catalog.json")
    assert out == flat


def test_resolve_read_returns_per_provider_path_when_neither_exists(tmp_path, monkeypatch):
    """Useful for surfacing FileNotFoundError pointing at the *expected*
    path, not the legacy fallback. Reduces operator confusion."""
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")

    out = resolve_artifact_path(cfg, "table_catalog.json")
    assert out == tmp_path / "data/artifacts/per_provider/prod/table_catalog.json"
    assert not out.exists()


def test_resolve_supports_explicit_provider_name(tmp_path, monkeypatch):
    """When rebuilding artifacts for a non-active provider, the resolver
    must point at THAT provider's dir, not the active primary's."""
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")

    out = resolve_artifact_path(cfg, "table_catalog.json",
                                  provider_name="staging", write=True)
    assert out == tmp_path / "data/artifacts/per_provider/staging/table_catalog.json"


def test_two_providers_resolve_to_disjoint_paths(tmp_path, monkeypatch):
    """The whole point of N1 — two providers must never share artifact
    files. Cross-contamination in catalog/graph/APSP/FAISS would be a
    silent data quality bug."""
    monkeypatch.setattr("text2sql.config.REPO_ROOT", tmp_path)
    cfg = _minimal_cfg(primary="prod")

    a = resolve_artifact_path(cfg, "table_catalog.json",
                                provider_name="prod-a", write=True)
    b = resolve_artifact_path(cfg, "table_catalog.json",
                                provider_name="prod-b", write=True)
    assert a != b
    a.parent.mkdir(parents=True, exist_ok=True)
    b.parent.mkdir(parents=True, exist_ok=True)
    a.write_text('{"db": "a"}')
    b.write_text('{"db": "b"}')
    assert json.loads(a.read_text())["db"] == "a"
    assert json.loads(b.read_text())["db"] == "b"


def test_active_target_provider_name_returns_target_db_primary():
    cfg = _minimal_cfg(primary="my-northridge")
    assert cfg.active_target_provider_name() == "my-northridge"


# ── N2: catalog persistence with provider provenance ───────────────────────


def _minimal_catalog(*, provider: str = "", dialect: str = "") -> "TableCatalog":
    from text2sql.table_catalog import TableCatalog

    return TableCatalog(
        data_standard_version="6.1.0",
        generated_at="2026-04-29T00:00:00Z",
        entries=[],
        descriptor_codes=[],
        provider_name=provider,
        target_dialect=dialect,
    )


def test_catalog_save_load_round_trip_preserves_provider(tmp_path):
    """Round-trip: provider_name + target_dialect survive save/load."""
    from text2sql.table_catalog import load_table_catalog, save_table_catalog

    cat = _minimal_catalog(provider="prod-mssql", dialect="mssql")
    p = tmp_path / "table_catalog.json"
    save_table_catalog(cat, p)
    loaded = load_table_catalog(p)
    assert loaded.provider_name == "prod-mssql"
    assert loaded.target_dialect == "mssql"


def test_catalog_load_legacy_file_yields_empty_provider(tmp_path):
    """A pre-N2 catalog has no provider_name/target_dialect keys. Reader
    must tolerate (empty strings) for backwards compat."""
    from text2sql.table_catalog import load_table_catalog

    legacy = {
        "data_standard_version": "6.1.0",
        "generated_at": "2026-01-01T00:00:00Z",
        "entry_count": 0,
        "descriptor_code_count": 0,
        "domain_counts": {},
        "entries": [],
        "descriptor_codes": [],
    }
    p = tmp_path / "legacy.json"
    p.write_text(json.dumps(legacy))
    loaded = load_table_catalog(p)
    assert loaded.provider_name == ""
    assert loaded.target_dialect == ""


def test_catalog_load_with_expected_provider_raises_on_mismatch(tmp_path):
    """The mismatch guard prevents using one provider's catalog against
    another's live DB — exactly the silent data-quality bug N1/N2 exists
    to prevent."""
    from text2sql.table_catalog import load_table_catalog, save_table_catalog

    cat = _minimal_catalog(provider="prod-mssql", dialect="mssql")
    p = tmp_path / "table_catalog.json"
    save_table_catalog(cat, p)

    with pytest.raises(RuntimeError) as ei:
        load_table_catalog(p, expected_provider="my-sqlite-demo")
    msg = str(ei.value)
    assert "prod-mssql" in msg
    assert "my-sqlite-demo" in msg
    # The error tells the user exactly how to fix it
    assert "rebuild --provider" in msg


def test_catalog_load_with_expected_provider_passes_when_matching(tmp_path):
    from text2sql.table_catalog import load_table_catalog, save_table_catalog

    cat = _minimal_catalog(provider="prod-mssql", dialect="mssql")
    p = tmp_path / "table_catalog.json"
    save_table_catalog(cat, p)

    loaded = load_table_catalog(p, expected_provider="prod-mssql")
    assert loaded.provider_name == "prod-mssql"


def test_catalog_load_with_expected_provider_passes_for_legacy_unset(tmp_path):
    """If the on-disk catalog has no provider_name (pre-N2), don't break
    legacy flat-layout users by raising on every load."""
    from text2sql.table_catalog import load_table_catalog

    legacy = {
        "data_standard_version": "6.1.0",
        "generated_at": "2026-01-01T00:00:00Z",
        "entry_count": 0, "descriptor_code_count": 0,
        "domain_counts": {}, "entries": [], "descriptor_codes": [],
    }
    p = tmp_path / "legacy.json"
    p.write_text(json.dumps(legacy))

    loaded = load_table_catalog(p, expected_provider="anyone")
    assert loaded.provider_name == ""


# ── N3: FK graph persistence with provider provenance ─────────────────────


def _tiny_graph_with_apsp():
    """Build a 2-node FK graph + APSP for round-trip tests. Uses the
    real builder so any persistence regression surfaces."""
    import numpy as np
    from text2sql.graph.builder import (
        FKEdge, FKGraph, TableMeta, _LogicalEdge,
    )
    import rustworkx as rx

    nodes = ["edfi.A", "edfi.B"]
    node_index = {"edfi.A": 0, "edfi.B": 1}
    meta = {
        "edfi.A": TableMeta(fqn="edfi.A", is_descriptor=False,
                             is_association=False, is_extension=False,
                             primary_domain="X", aggregate_root="edfi.A"),
        "edfi.B": TableMeta(fqn="edfi.B", is_descriptor=False,
                             is_association=False, is_extension=False,
                             primary_domain="X", aggregate_root="edfi.B"),
    }
    rx_graph = rx.PyGraph(multigraph=False)
    rx_graph.add_nodes_from(nodes)
    fk = FKEdge(src_schema="edfi", src_table="A",
                  dst_schema="edfi", dst_table="B",
                  constraint_name="fk_a_b",
                  column_pairs=(("AId", "AId"),))
    edges = {(0, 1): _LogicalEdge(a="edfi.A", b="edfi.B", weight=1.0, fks=[fk])}
    rx_graph.add_edge(0, 1, 1.0)
    g = FKGraph(nodes=nodes, node_index=node_index, meta=meta,
                  edges=edges, rx_graph=rx_graph)
    g.compute_apsp()
    return g


def test_graph_save_load_round_trip_preserves_provider(tmp_path):
    from text2sql.graph.builder import load_graph, save_graph

    g = _tiny_graph_with_apsp()
    save_graph(g, tmp_path, provider_name="prod-pg", target_dialect="postgresql")
    loaded = load_graph(tmp_path)
    assert loaded.provider_name == "prod-pg"
    assert loaded.target_dialect == "postgresql"
    # Sanity: structural fields round-tripped too
    assert loaded.nodes == g.nodes
    assert (0, 1) in loaded.edges


def test_graph_load_legacy_payload_yields_empty_provider(tmp_path):
    """A legacy graph.msgpack written by pre-N3 code has no
    provider_name/target_dialect keys. Load must tolerate."""
    from text2sql.graph.builder import save_graph, load_graph
    import msgpack

    g = _tiny_graph_with_apsp()
    save_graph(g, tmp_path)  # provider_name="" — same as legacy
    # Verify the on-disk payload doesn't carry a non-empty provider tag
    raw = msgpack.unpackb((tmp_path / "graph.msgpack").read_bytes(), raw=False)
    assert raw["provider_name"] == ""

    loaded = load_graph(tmp_path)
    assert loaded.provider_name == ""
    assert loaded.target_dialect == ""


def test_graph_load_with_expected_provider_raises_on_mismatch(tmp_path):
    """The mismatch guard prevents using one provider's graph against
    another's catalog/live DB."""
    from text2sql.graph.builder import load_graph, save_graph

    g = _tiny_graph_with_apsp()
    save_graph(g, tmp_path, provider_name="prod-mssql", target_dialect="mssql")

    with pytest.raises(RuntimeError) as ei:
        load_graph(tmp_path, expected_provider="my-sqlite-demo")
    msg = str(ei.value)
    assert "prod-mssql" in msg
    assert "my-sqlite-demo" in msg
    assert "rebuild --provider" in msg


def test_graph_load_with_expected_provider_passes_when_matching(tmp_path):
    from text2sql.graph.builder import load_graph, save_graph

    g = _tiny_graph_with_apsp()
    save_graph(g, tmp_path, provider_name="prod", target_dialect="postgresql")
    loaded = load_graph(tmp_path, expected_provider="prod")
    assert loaded.provider_name == "prod"


def test_graph_load_legacy_passes_any_expected_provider(tmp_path):
    """Don't break in-place upgrades: legacy graph (provider_name=='')
    matches any expected_provider so existing single-target deployments
    keep working until the next rebuild."""
    from text2sql.graph.builder import load_graph, save_graph

    g = _tiny_graph_with_apsp()
    save_graph(g, tmp_path)  # provider_name=""
    loaded = load_graph(tmp_path, expected_provider="anyone")
    assert loaded.provider_name == ""
