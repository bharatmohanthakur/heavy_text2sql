"""Index TableCatalog → vector store.

Builds two collections:
  * `tables`         — one record per TableEntry. Embed text = semantic blob
                       (table desc + columns desc + values). Payload carries
                       domains[], fqn, and a copy of the embed text for
                       hybrid (BM25) reranking.
  * `column_values`  — one record per (table, column, value). Used by
                       Component 6 (entity resolver).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

from text2sql.embedding.blob_builder import (
    build_column_value_blobs,
    build_descriptor_code_blobs,
    build_table_blob,
)
from text2sql.providers.base import EmbeddingProvider, VectorStore
from text2sql.table_catalog import TableCatalog, TableEntry

log = logging.getLogger(__name__)


@dataclass
class IndexStats:
    tables_indexed: int = 0
    column_values_indexed: int = 0
    embedding_dim: int = 0
    total_input_chars: int = 0


def _payload_for_entry(entry: TableEntry, blob: str) -> dict:
    return {
        "fqn": entry.fqn,
        "schema": entry.schema,
        "table": entry.table,
        "domains": list(entry.domains),
        "is_descriptor": entry.is_descriptor,
        "is_association": entry.is_association,
        "is_extension": entry.is_extension,
        "primary_key": list(entry.primary_key),
        "row_count": entry.row_count,
        # Carry a copy of the embed text so hybrid_search BM25 can score over it
        # without the caller round-tripping to the catalog.
        "text": blob,
    }


def index_table_catalog(
    catalog: TableCatalog,
    embedder: EmbeddingProvider,
    store: VectorStore,
    *,
    collection: str = "tables",
    batch_size: int = 64,
) -> IndexStats:
    stats = IndexStats(embedding_dim=embedder.dim)

    ids: list[str] = []
    blobs: list[str] = []
    payloads: list[dict] = []
    for entry in catalog.entries:
        blob = build_table_blob(entry)
        if not blob:
            continue
        ids.append(entry.fqn)
        blobs.append(blob)
        payloads.append(_payload_for_entry(entry, blob))
        stats.total_input_chars += len(blob)

    # Embed in batches; hand to store as one upsert.
    vectors: list[np.ndarray] = []
    for i in range(0, len(blobs), batch_size):
        chunk = blobs[i : i + batch_size]
        v = embedder.embed(chunk, kind="doc")
        vectors.append(v)
    if not vectors:
        return stats
    matrix = np.vstack(vectors)
    store.upsert(collection, ids, matrix, payloads)
    stats.tables_indexed = len(ids)
    return stats


def index_column_values(
    catalog: TableCatalog,
    embedder: EmbeddingProvider,
    store: VectorStore,
    *,
    collection: str = "column_values",
    batch_size: int = 128,
) -> IndexStats:
    stats = IndexStats(embedding_dim=embedder.dim)

    ids: list[str] = []
    texts: list[str] = []
    payloads: list[dict] = []
    for entry in catalog.entries:
        for id_, text, payload in build_column_value_blobs(entry):
            ids.append(id_)
            texts.append(text)
            payloads.append(payload)
            stats.total_input_chars += len(text)

    # Descriptor codes — far more useful for entity resolution than the raw
    # column samples. One record per (codevalue, namespace) pair.
    for id_, text, payload in build_descriptor_code_blobs(catalog.descriptor_codes):
        ids.append(id_)
        texts.append(text)
        payloads.append(payload)
        stats.total_input_chars += len(text)

    if not ids:
        return stats

    vectors: list[np.ndarray] = []
    for i in range(0, len(texts), batch_size):
        chunk = texts[i : i + batch_size]
        v = embedder.embed(chunk, kind="doc")
        vectors.append(v)
    matrix = np.vstack(vectors)
    store.upsert(collection, ids, matrix, payloads)
    stats.column_values_indexed = len(ids)
    return stats
