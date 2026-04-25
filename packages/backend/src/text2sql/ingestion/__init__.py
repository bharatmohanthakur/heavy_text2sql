"""Component 1: Data ingestion from Ed-Fi GitHub.

Fetches authoritative artifacts (ApiModel.json, 0030-ForeignKeys.sql, descriptors,
domain inventory) for a target Data Standard version + selected extensions, caches
them locally, and provides a structured `IngestionManifest` that downstream
components consume.
"""

from text2sql.ingestion.edfi_fetcher import (
    ArtifactSet,
    ExtensionSpec,
    IngestionConfig,
    IngestionManifest,
    fetch_all,
    verify_manifest,
)

__all__ = [
    "ArtifactSet",
    "ExtensionSpec",
    "IngestionConfig",
    "IngestionManifest",
    "fetch_all",
    "verify_manifest",
]
