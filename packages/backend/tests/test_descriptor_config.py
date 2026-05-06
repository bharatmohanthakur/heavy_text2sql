"""Operator-configurable descriptor layer.

Three behaviors to pin:

  1. Default config + Ed-Fi-shaped DB → master pull populates
     `descriptor_codes`, descriptor children get column-sampling skip.
  2. Default config + non-Ed-Fi DB (no `edfi.Descriptor` master) →
     descriptor_layer treated as inactive: children sampled like
     ordinary tables, `descriptor_codes` stays empty.
  3. Operator override (`master_fqn: null`) → layer disabled even when
     a master-shaped table exists in the catalog.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import sqlalchemy as sa

from text2sql.catalog_inputs import (
    CatalogInputs,
    synthesize_inputs_for_builder,
)
from text2sql.config import ProviderEntry
from text2sql.providers.db.sqlite import SqliteEngine
from text2sql.table_catalog import build_table_catalog
from text2sql.table_catalog.descriptor_config import DescriptorConfig, load_overrides


# ── Pure unit tests for the config helpers ─────────────────────────────────


def test_descriptor_config_defaults_match_ed_fi():
    cfg = DescriptorConfig()
    assert cfg.enabled
    assert cfg.master_fqn == "edfi.Descriptor"
    assert cfg.child_resolution == "namespace_tail"


def test_descriptor_config_disable_via_override(tmp_path: Path):
    p = tmp_path / "overrides.yaml"
    p.write_text("descriptors:\n  master_fqn: null\n", encoding="utf-8")
    cfg = load_overrides(p)
    assert not cfg.enabled


def test_descriptor_config_explicit_flagged_tables(tmp_path: Path):
    p = tmp_path / "overrides.yaml"
    p.write_text(textwrap.dedent("""
        descriptors:
          flagged_tables:
            - edfi.RaceDescriptor
            - edfi.GradeLevelDescriptor
    """).strip() + "\n", encoding="utf-8")
    cfg = load_overrides(p)
    assert cfg.flagged_tables == ("edfi.RaceDescriptor", "edfi.GradeLevelDescriptor")
    # Suffix heuristic must not also fire when the operator gave a list.
    from text2sql.table_catalog.descriptor_config import is_flagged
    assert is_flagged(cfg, "edfi.RaceDescriptor", "RaceDescriptor")
    assert not is_flagged(cfg, "edfi.UnlistedDescriptor", "UnlistedDescriptor")


# ── End-to-end: non-Ed-Fi DB no longer silently breaks ─────────────────────


@pytest.fixture
def non_edfi_db(tmp_path: Path):
    """Operator DB with a `*Descriptor`-named table but no master.
    Pre-fix behavior: GradeLevelDescriptor flagged is_descriptor=True,
    column sampling skipped, descriptor_codes empty → its values never
    reach the resolver. Post-fix: the layer auto-disables, the
    'Descriptor'-suffixed table is still flagged but column sampling
    runs, so its values are indexed."""
    db = tmp_path / "no_master.sqlite"
    raw = sa.create_engine(f"sqlite:///{db}", future=True)
    with raw.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE GradeLevelDescriptor ("
            "  GradeLevelDescriptorId INTEGER PRIMARY KEY,"
            "  CodeValue TEXT NOT NULL"
            ")"
        ))
        conn.execute(sa.text("INSERT INTO GradeLevelDescriptor VALUES (1, 'Ninth grade')"))
        conn.execute(sa.text("INSERT INTO GradeLevelDescriptor VALUES (2, 'Tenth grade')"))
    raw.dispose()
    return SqliteEngine(ProviderEntry.model_validate({
        "kind": "sqlite", "path": str(db), "read_only": False,
    }))


def _inputs_with_descriptor() -> CatalogInputs:
    schema = textwrap.dedent("""
        Ranking,Domain,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,Populated
        0,EducationOrg,edfi,GradeLevelDescriptor,GradeLevelDescriptorId,Yes
        0,EducationOrg,edfi,GradeLevelDescriptor,CodeValue,Yes
    """).strip() + "\n"
    rels = "FK_Name,Parent_Table,Parent_Column,Referenced_Table,Referenced_Column,Parent_Schema,Referenced_Schema\n"
    return CatalogInputs.from_csvs(schema, rels)


def test_non_edfi_descriptor_table_keeps_sample_values(non_edfi_db):
    """Pre-fix bug: a `*Descriptor`-named table on a non-Ed-Fi DB had
    is_descriptor=True AND no fanout source → empty sample_values.
    Post-fix: the layer auto-disables (no master in catalog), so the
    descriptor-flagged table is sampled like any other lookup."""
    idx, cls, manifest = synthesize_inputs_for_builder(
        _inputs_with_descriptor(), sql_engine=non_edfi_db,
    )
    catalog = build_table_catalog(
        cls, idx, manifest,
        sql_engine=non_edfi_db,
        description_generator=None,
        sample_row_count=2,
        include_unknown_tables=False,
    )
    by_fqn = {e.fqn: e for e in catalog.entries}
    glv = by_fqn["edfi.GradeLevelDescriptor"]

    # Children are still flagged for downstream resolver routing.
    assert glv.is_descriptor

    # But sample_values now flow through (the regression we're fixing).
    cv_col = next(c for c in glv.columns if c.name == "CodeValue")
    assert set(cv_col.sample_values) >= {"Ninth grade", "Tenth grade"}
    assert cv_col.distinct_count == 2

    # And no descriptor_codes pull happened because there's no master.
    assert catalog.descriptor_codes == []


def test_operator_override_disables_layer_completely(non_edfi_db, tmp_path: Path):
    """Even when a master-shaped table happens to exist, an operator
    `descriptors: {master_fqn: null}` override fully disables the layer."""
    cfg_disabled = DescriptorConfig(master_fqn=None)
    idx, cls, manifest = synthesize_inputs_for_builder(
        _inputs_with_descriptor(), sql_engine=non_edfi_db,
        descriptor_config=cfg_disabled,
    )
    catalog = build_table_catalog(
        cls, idx, manifest,
        sql_engine=non_edfi_db,
        description_generator=None,
        sample_row_count=2,
        include_unknown_tables=False,
        descriptor_config=cfg_disabled,
    )
    by_fqn = {e.fqn: e for e in catalog.entries}
    glv = by_fqn["edfi.GradeLevelDescriptor"]

    # is_descriptor stays False — operator opted out, name suffix
    # doesn't fire.
    assert not glv.is_descriptor
    assert catalog.descriptor_codes == []
