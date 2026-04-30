"""Operator-supplied catalog inputs (replaces Ed-Fi GitHub artifacts).

The platform pivoted (Apr 30, 2026) off Ed-Fi GitHub as its upstream
input. Two operator-supplied CSVs now drive every catalog build:

  * Schema CSV       — table & column inventory plus domain mapping
                       (Ranking · Domain · TABLE_SCHEMA · TABLE_NAME
                       · COLUMN_NAME · Populated)
  * Relationships CSV — foreign-key edges, composite-aware
                       (FK_Name · Parent_Table · Parent_Column ·
                       Referenced_Table · Referenced_Column ·
                       Parent_Schema · Referenced_Schema)

Every other catalog input (table descriptions, unique values, column
descriptions) is now sourced from the live target DB by sampling +
LLM, not from Ed-Fi prose.

This package is the single upstream contract for `catalog_builder` —
no other module should reach for ApiModel.json or 0030-ForeignKeys.sql.
"""

from text2sql.catalog_inputs.loader import CatalogInputs
from text2sql.catalog_inputs.relationships_csv import (
    RelationshipsCsvError,
    parse_relationships_csv,
)
from text2sql.catalog_inputs.schema_csv import (
    ColumnRow,
    SchemaCsvError,
    parse_schema_csv,
)

__all__ = [
    "CatalogInputs",
    "ColumnRow",
    "RelationshipsCsvError",
    "SchemaCsvError",
    "parse_relationships_csv",
    "parse_schema_csv",
]
