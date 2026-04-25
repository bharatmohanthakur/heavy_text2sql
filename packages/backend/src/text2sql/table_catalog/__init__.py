"""Component 4: Table catalog — one entry per table.

Each entry holds everything Components 5–10 need to embed, retrieve, route,
generate, and validate against:

  * Identity:    schema, name, fqn
  * Structure:   PK, FK neighbors, aggregate root, flags
  * Columns:     name, data type, nullable, description (from ApiModel),
                 sample values (from live DB), distinct count
  * Sample rows: a handful of real rows
  * Domains:     multi-label list straight from ApiModel.json
  * LLM gap-fill: only column descriptions that ApiModel left empty get
                  rewritten by the LLM. Table descriptions and 60% of column
                  descriptions are already authoritative in git — we don't
                  rebuild what's already there.

No `DomainPack`. Domains are tags on tables. Retrieval filters by tag.
"""

from text2sql.table_catalog.catalog_builder import (
    ColumnInfo,
    DescriptorCode,
    TableCatalog,
    TableEntry,
    build_table_catalog,
    load_table_catalog,
    save_table_catalog,
)
from text2sql.table_catalog.description_generator import (
    DescriptionGenerator,
    GeneratedDescriptions,
    TableSampleData,
)

__all__ = [
    "ColumnInfo",
    "DescriptionGenerator",
    "DescriptorCode",
    "GeneratedDescriptions",
    "TableCatalog",
    "TableEntry",
    "TableSampleData",
    "build_table_catalog",
    "load_table_catalog",
    "save_table_catalog",
]
