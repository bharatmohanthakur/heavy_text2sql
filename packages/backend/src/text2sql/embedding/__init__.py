"""Component 5: Embedding + vector index.

ONE collection per kind (no per-domain duplication):
  * `tables`         — one record per TableEntry; payload includes domains[]
                       so retrieval filters by domain at query time.
  * `column_values`  — distinct sample values from low-cardinality columns,
                       feeds Component 6 (entity resolver).

Embeddings come from whatever provider Component 0's factory wires (Azure
text-embedding-3-large by default). Indexing is one-shot per rebuild.
"""

from text2sql.embedding.blob_builder import (
    build_column_value_blobs,
    build_descriptor_code_blobs,
    build_table_blob,
)
from text2sql.embedding.indexer import (
    IndexStats,
    index_column_values,
    index_table_catalog,
)
from text2sql.embedding.retriever import TableHit, TableRetriever

__all__ = [
    "IndexStats",
    "TableHit",
    "TableRetriever",
    "build_column_value_blobs",
    "build_descriptor_code_blobs",
    "build_table_blob",
    "index_column_values",
    "index_table_catalog",
]
