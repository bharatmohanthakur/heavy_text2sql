"""Step P1 — switching active target_db provider must refresh /tables.

Regression: with N1–N5 splitting catalogs into per-provider directories,
the API used to load the FLAT data/artifacts/table_catalog.json once at
boot — so after an operator switched the active target_db, /tables,
/tables/{fqn}, and /domains kept serving the OLD provider's tables.

This test stages two per-provider catalogs on disk, points the active
provider at one, hits /tables, then switches the active provider via
the runtime overlay and hits /tables again. The two responses must
disagree.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import text2sql.config as cfg_mod
from text2sql.api import build_app
from text2sql.pipeline import PipelineResult
from text2sql.table_catalog import TableCatalog, save_table_catalog
from text2sql.table_catalog.catalog_builder import TableEntry, ColumnInfo


# ── Helpers ─────────────────────────────────────────────────────────────────


def _entry(schema: str, table: str, columns: list[str]) -> TableEntry:
    return TableEntry(
        schema=schema, table=table,
        description=f"{schema}.{table} description",
        description_source="apimodel", domains=["Student"],
        is_descriptor=False, is_association=False, is_extension=False,
        primary_key=[columns[0]] if columns else [],
        parent_neighbors=[], child_neighbors=[], aggregate_root=None,
        columns=[ColumnInfo(name=c, data_type="text", nullable=True,
                            description="")
                 for c in columns],
    )


def _make_catalog(*entries: TableEntry, provider_name: str, dialect: str) -> TableCatalog:
    return TableCatalog(
        data_standard_version="6.1.0",
        generated_at=datetime.now(timezone.utc).isoformat(),
        entries=list(entries),
        provider_name=provider_name,
        target_dialect=dialect,
    )


class _StubPipeline:
    def answer(self, q: str, *, execute: bool = True, max_rows: int = 100, **_):
        return PipelineResult(
            nl_question=q, sql="SELECT 1", rationale="x",
            rows=[], row_count=0, executed=False, validated=False,
            description="",
        )


# ── Fixture: two per-provider catalogs staged on disk + overlay ─────────────


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """Lay out two per-provider catalog dirs and patch REPO_ROOT/overlay."""
    repo = tmp_path / "repo"
    artifacts = repo / "data/artifacts"
    pp_mssql = artifacts / "per_provider/old-mssql"
    pp_sqlite = artifacts / "per_provider/new-sqlite"
    for p in (pp_mssql, pp_sqlite):
        p.mkdir(parents=True)

    # OLD provider has Student table only
    save_table_catalog(
        _make_catalog(
            _entry("edfi", "Student", ["StudentUSI", "FirstName"]),
            provider_name="old-mssql", dialect="mssql",
        ),
        pp_mssql / "table_catalog.json",
    )
    # NEW provider has Course table only — totally different shape
    save_table_catalog(
        _make_catalog(
            _entry("edfi", "Course", ["CourseCode", "Title"]),
            provider_name="new-sqlite", dialect="sqlite",
        ),
        pp_sqlite / "table_catalog.json",
    )

    # Patch REPO_ROOT in config + every place that captured it at import
    monkeypatch.setattr(cfg_mod, "REPO_ROOT", repo)

    # Minimal default.yaml the loader will accept
    configs_dir = repo / "configs"
    configs_dir.mkdir(parents=True)
    default_yaml = configs_dir / "default.yaml"
    default_yaml.write_text("""\
ed_fi: {data_standard_version: "6.1.0", extensions: [], cache_dir: "data/edfi", artifact_dir: "data/artifacts", github: {ods_repo: "x", extensions_repo: "y"}}
llm: {primary: "x", providers: {x: {kind: "openai"}}}
embeddings: {primary: "x", providers: {x: {kind: "openai"}}}
vector_store: {primary: "x", providers: {x: {kind: "faiss"}}}
target_db:
  primary: "old-mssql"
  providers:
    old-mssql: {kind: "mssql"}
    new-sqlite: {kind: "sqlite", path: "demo.sqlite"}
metadata_db: {kind: "sqlite", path: "meta.sqlite"}
logging: {level: "INFO", format: "json"}
""")

    overlay = artifacts / "runtime_overrides.json"
    monkeypatch.setattr(cfg_mod, "RUNTIME_OVERRIDES_PATH", overlay)

    return {
        "repo": repo,
        "default_yaml": default_yaml,
        "overlay": overlay,
    }


# ── Test ────────────────────────────────────────────────────────────────────


def test_tables_follows_active_provider(staged):
    """The /tables endpoint must reflect the catalog of whichever
    target_db provider is currently primary — re-resolved per request,
    not pinned to whatever was loaded at server boot."""
    from text2sql.config import load_config, resolve_artifact_path
    from text2sql.table_catalog import load_table_catalog

    # Provider-aware loader: re-reads config + per-provider path each call.
    def loader():
        cfg = load_config(config_path=staged["default_yaml"])
        path = resolve_artifact_path(cfg, "table_catalog.json")
        return load_table_catalog(path)

    # First request — primary is old-mssql.
    initial = loader()
    app = build_app(
        pipeline=_StubPipeline(), catalog=initial, gold_store=None,
        catalog_loader=loader,
    )
    client = TestClient(app)

    r1 = client.get("/tables").json()
    fqns_1 = sorted(t["fqn"] for t in r1["tables"])
    assert fqns_1 == ["edfi.Student"], fqns_1

    # Switch active provider via overlay (the same write Settings does).
    staged["overlay"].write_text(json.dumps({
        "target_db": {"primary": "new-sqlite"},
    }))

    # Same client, no restart — must now see the new provider's tables.
    r2 = client.get("/tables").json()
    fqns_2 = sorted(t["fqn"] for t in r2["tables"])
    assert fqns_2 == ["edfi.Course"], (
        f"after switching active provider, /tables still served the old "
        f"catalog: {fqns_2}"
    )


def test_table_detail_follows_active_provider(staged):
    """/tables/{fqn} for a name that only exists in the new provider must
    404 before the switch and 200 after."""
    from text2sql.config import load_config, resolve_artifact_path
    from text2sql.table_catalog import load_table_catalog

    def loader():
        cfg = load_config(config_path=staged["default_yaml"])
        path = resolve_artifact_path(cfg, "table_catalog.json")
        return load_table_catalog(path)

    app = build_app(
        pipeline=_StubPipeline(), catalog=loader(), gold_store=None,
        catalog_loader=loader,
    )
    client = TestClient(app)

    # Course only exists in new-sqlite catalog.
    assert client.get("/tables/edfi.Course").status_code == 404

    staged["overlay"].write_text(json.dumps({
        "target_db": {"primary": "new-sqlite"},
    }))
    r = client.get("/tables/edfi.Course")
    assert r.status_code == 200, r.text
    body = r.json()
    cols = sorted(c["name"] for c in body["columns"])
    assert cols == ["CourseCode", "Title"]
