"""Operator-configurable descriptor layer.

Ed-Fi uses a two-table descriptor pattern: a master `edfi.Descriptor`
table holds every code value, and per-type child tables (`*Descriptor`)
just carry the FK so referencing tables can typecheck. We use this at
query time to map `"Hispanic" → edfi.RaceDescriptor` so the LLM gets
the full join chain.

Operators with non-Ed-Fi schemas almost certainly don't have this
shape. Hardcoding `edfi.Descriptor` + the namespace-tail convention
silently broke their builds: their `*Descriptor`-named tables were
flagged `is_descriptor=True` and had column-value sampling skipped,
and the master pull returned nothing, leaving descriptor codes
absent from the resolver index.

This module makes the whole layer operator-driven:

  * `DescriptorConfig` describes the shape of the operator's
    descriptor system (or declares there isn't one).
  * `auto_detect()` infers it at build time from the catalog +
    optional `domain_overrides.yaml` overrides.

When `master_fqn` is None, the builder treats descriptor tables like
ordinary lookup tables: column-value sampling proceeds, no master pull
runs, no descriptor-code blob is emitted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DescriptorConfig:
    """How the operator's descriptor layer maps to our resolver chain.

    Default values match the Ed-Fi DS 6.1.0 shape. Operators with a
    different layout override via `configs/domain_overrides.yaml`:

        descriptors:
          master_fqn: "ods.value_lookup"
          id_column: "lookup_id"
          code_column: "code"
          namespace_column: "category"
          short_description_column: "label"
          description_column: "description"
          child_resolution: "namespace_tail"   # or "explicit_map" / "name_suffix"
          name_suffix: "Descriptor"
          explicit_children: {RaceDescriptor: edfi.RaceDescriptor, ...}
          flagged_tables: [edfi.RaceDescriptor, edfi.GradeLevelDescriptor]

    To opt out completely:
        descriptors:
          master_fqn: null
    """
    master_fqn: str | None = "edfi.Descriptor"
    id_column: str = "DescriptorId"
    code_column: str = "CodeValue"
    namespace_column: str = "Namespace"
    short_description_column: str = "ShortDescription"
    description_column: str = "Description"
    # How to map a master row's namespace value to a child table fqn.
    #   "namespace_tail" — Ed-Fi: trailing path segment of namespace
    #                       URL must equal the child table's name.
    #   "explicit_map"   — operator provides a {namespace: child_fqn} map.
    #   "name_suffix"    — the master row's namespace IS the child table
    #                       name; pair with `name_suffix` to filter.
    child_resolution: str = "namespace_tail"
    name_suffix: str = "Descriptor"
    explicit_children: dict[str, str] = field(default_factory=dict)
    # Tables the operator explicitly flags as descriptor children. When
    # empty, the builder falls back to the suffix heuristic.
    flagged_tables: tuple[str, ...] = ()

    @property
    def enabled(self) -> bool:
        return bool(self.master_fqn)


def load_overrides(path: Path | None) -> DescriptorConfig:
    """Load descriptor settings from `domain_overrides.yaml`.

    Returns the default Ed-Fi-shaped config when the file is missing
    or has no `descriptors:` block. A `descriptors: {master_fqn: null}`
    block disables the layer entirely.
    """
    if path is None or not path.exists():
        return DescriptorConfig()
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        log.warning("descriptor overrides parse failed (%s): %s", path, e)
        return DescriptorConfig()
    block = raw.get("descriptors") or {}
    if not isinstance(block, dict):
        return DescriptorConfig()

    # Pydantic-free; we hand-pick the fields so unknown keys don't
    # silently swallow operator typos.
    return DescriptorConfig(
        master_fqn=block.get("master_fqn", DescriptorConfig.master_fqn),
        id_column=block.get("id_column", DescriptorConfig.id_column),
        code_column=block.get("code_column", DescriptorConfig.code_column),
        namespace_column=block.get("namespace_column", DescriptorConfig.namespace_column),
        short_description_column=block.get(
            "short_description_column", DescriptorConfig.short_description_column,
        ),
        description_column=block.get("description_column", DescriptorConfig.description_column),
        child_resolution=block.get("child_resolution", DescriptorConfig.child_resolution),
        name_suffix=block.get("name_suffix", DescriptorConfig.name_suffix),
        explicit_children={str(k): str(v) for k, v in (block.get("explicit_children") or {}).items()},
        flagged_tables=tuple(block.get("flagged_tables") or ()),
    )


def is_flagged(cfg: DescriptorConfig, fqn: str, table_name: str) -> bool:
    """Decide whether a given table should be treated as a descriptor.

    Three precedence levels:
      1. Operator's explicit `flagged_tables` list — wins if non-empty.
      2. Name-suffix heuristic — fires only when descriptor layer
         is enabled AND the operator hasn't given an explicit list.
      3. Otherwise: not a descriptor.
    """
    if cfg.flagged_tables:
        return fqn in cfg.flagged_tables
    if not cfg.enabled:
        return False
    return bool(cfg.name_suffix) and table_name.endswith(cfg.name_suffix)


def master_in_catalog(cfg: DescriptorConfig, fqns: Iterable[str]) -> bool:
    """Check whether the catalog actually contains the master table.

    Build-time guard: a config can declare `master_fqn` but the live
    DB might not have that table. In that case we skip the master pull
    + the column-sampling skip, treating descriptor tables as lookups.
    """
    if not cfg.enabled:
        return False
    return cfg.master_fqn in set(fqns)
