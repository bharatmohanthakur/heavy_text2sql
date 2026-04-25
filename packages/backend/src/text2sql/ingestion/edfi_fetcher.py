"""Component 1: Ed-Fi data ingestion.

Pulls authoritative Data-Standard artifacts from the public Ed-Fi GitHub repos
into a local cache. Idempotent — re-runs are no-ops unless `force=True`.

Two artifacts per source:
  * ApiModel.json (entity / aggregate / domain / association definitions)
  * 0030-ForeignKeys.sql (DDL of FK constraints, dialect-specific)

Verification step asserts entity / FK / domain counts so downstream components
fail loud at ingestion time, not three layers deep.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from text2sql.config import EdFiConfig

GITHUB_RAW = "https://raw.githubusercontent.com"


@dataclass(frozen=True)
class ExtensionSpec:
    """A single Ed-Fi extension to ingest alongside core ODS."""
    name: str                          # e.g. "TPDM"
    extension_version: str             # e.g. "1.1.0"
    data_standard_version: str         # extension's own DS-targeting subdir


@dataclass
class ArtifactSet:
    """Single source-of-truth files for a (source, DS-version) pair."""
    source: str                        # "core" or "ext:TPDM"
    data_standard_version: str
    api_model_path: Path
    foreign_keys_sql_path: Path
    api_model_sha256: str
    foreign_keys_sha256: str
    api_model_url: str
    foreign_keys_url: str


@dataclass
class IngestionConfig:
    """Resolved ingestion plan — derived from EdFiConfig, optionally overridden."""
    data_standard_version: str
    sql_dialect: str                   # "MsSql" or "PgSql"
    cache_dir: Path
    ods_repo: str
    ods_ref: str
    extensions_repo: str
    extensions_ref: str
    extensions: list[ExtensionSpec] = field(default_factory=list)

    @classmethod
    def from_app_config(cls, ed_fi: EdFiConfig, repo_root: Path) -> "IngestionConfig":
        return cls(
            data_standard_version=ed_fi.data_standard_version,
            sql_dialect=ed_fi.github.sql_dialect,
            cache_dir=(repo_root / ed_fi.cache_dir).resolve(),
            ods_repo=ed_fi.github.ods_repo,
            ods_ref=ed_fi.github.ods_ref,
            extensions_repo=ed_fi.github.extensions_repo,
            extensions_ref=ed_fi.github.extensions_ref,
            extensions=[],   # populated by callers who know which extensions are valid for this DS
        )


@dataclass
class IngestionManifest:
    """The output of fetch_all() — what was fetched, when, where, and counts."""
    data_standard_version: str
    sql_dialect: str
    fetched_at: str
    artifacts: list[ArtifactSet]
    counts: dict[str, dict[str, int]]    # per-source: {entities, fks, aggregates, domains, descriptors}

    def to_json(self) -> str:
        def conv(o: Any) -> Any:
            if isinstance(o, Path):
                return str(o)
            raise TypeError(f"unhandled: {o!r}")
        return json.dumps(asdict(self), default=conv, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> "IngestionManifest":
        raw = json.loads(text)
        raw["artifacts"] = [
            ArtifactSet(
                **{**a, "api_model_path": Path(a["api_model_path"]),
                   "foreign_keys_sql_path": Path(a["foreign_keys_sql_path"])}
            )
            for a in raw["artifacts"]
        ]
        return cls(**raw)


def _ods_urls(cfg: IngestionConfig) -> tuple[str, str]:
    base = f"{GITHUB_RAW}/{cfg.ods_repo}/{cfg.ods_ref}/Application/EdFi.Ods.Standard/Standard/{cfg.data_standard_version}/Artifacts"
    return (
        f"{base}/Metadata/ApiModel.json",
        f"{base}/{cfg.sql_dialect}/Structure/Ods/0030-ForeignKeys.sql",
    )


def _ext_urls(cfg: IngestionConfig, ext: ExtensionSpec) -> tuple[str, str]:
    base = (
        f"{GITHUB_RAW}/{cfg.extensions_repo}/{cfg.extensions_ref}/Extensions/EdFi.Ods.Extensions.{ext.name}"
        f"/Versions/{ext.extension_version}/Standard/{ext.data_standard_version}/Artifacts"
    )
    return (
        f"{base}/Metadata/ApiModel-EXTENSION.json",
        f"{base}/{cfg.sql_dialect}/Structure/Ods/0030-EXTENSION-{ext.name}-ForeignKeys.sql",
    )


def _fetch(client: httpx.Client, url: str, dest: Path, *, force: bool) -> str:
    if dest.exists() and not force:
        return hashlib.sha256(dest.read_bytes()).hexdigest()
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = client.get(url)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return hashlib.sha256(resp.content).hexdigest()


def _summarize(api_model_path: Path) -> dict[str, int]:
    data = json.loads(api_model_path.read_text())
    entities = data.get("entityDefinitions", [])
    domains: set[str] = set()
    descriptors = 0
    for e in entities:
        domains.update(e.get("domains", []) or [])
        if e.get("name", "").endswith("Descriptor"):
            descriptors += 1
    return {
        "entities": len(entities),
        "fks": len(data.get("associationDefinitions", [])),
        "aggregates": len(data.get("aggregateDefinitions", [])),
        "domains": len(domains),
        "descriptors": descriptors,
    }


def fetch_all(cfg: IngestionConfig, *, force: bool = False) -> IngestionManifest:
    """Fetch core ODS + every configured extension into the cache."""
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[ArtifactSet] = []
    counts: dict[str, dict[str, int]] = {}

    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        # Core
        api_url, fk_url = _ods_urls(cfg)
        api_path = cfg.cache_dir / "core" / cfg.data_standard_version / "ApiModel.json"
        fk_path = cfg.cache_dir / "core" / cfg.data_standard_version / "0030-ForeignKeys.sql"
        api_sha = _fetch(client, api_url, api_path, force=force)
        fk_sha = _fetch(client, fk_url, fk_path, force=force)
        artifacts.append(ArtifactSet(
            source="core",
            data_standard_version=cfg.data_standard_version,
            api_model_path=api_path,
            foreign_keys_sql_path=fk_path,
            api_model_sha256=api_sha,
            foreign_keys_sha256=fk_sha,
            api_model_url=api_url,
            foreign_keys_url=fk_url,
        ))
        counts["core"] = _summarize(api_path)

        # Extensions
        for ext in cfg.extensions:
            api_url, fk_url = _ext_urls(cfg, ext)
            sub = cfg.cache_dir / "ext" / ext.name / ext.extension_version / ext.data_standard_version
            api_path = sub / "ApiModel-EXTENSION.json"
            fk_path = sub / f"0030-EXTENSION-{ext.name}-ForeignKeys.sql"
            api_sha = _fetch(client, api_url, api_path, force=force)
            fk_sha = _fetch(client, fk_url, fk_path, force=force)
            artifacts.append(ArtifactSet(
                source=f"ext:{ext.name}",
                data_standard_version=ext.data_standard_version,
                api_model_path=api_path,
                foreign_keys_sql_path=fk_path,
                api_model_sha256=api_sha,
                foreign_keys_sha256=fk_sha,
                api_model_url=api_url,
                foreign_keys_url=fk_url,
            ))
            counts[f"ext:{ext.name}"] = _summarize(api_path)

    manifest = IngestionManifest(
        data_standard_version=cfg.data_standard_version,
        sql_dialect=cfg.sql_dialect,
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        artifacts=artifacts,
        counts=counts,
    )
    (cfg.cache_dir / "manifest.json").write_text(manifest.to_json())
    return manifest


# ── Verification gates ────────────────────────────────────────────────────────

# DS 6.1.0 reference numbers (verified live against the public repo on 2026-04-25)
DS_610_EXPECTED = {
    "entities": 829,
    "fks": 1663,
    "aggregates": 475,
    "domains": 35,
    "descriptors": 281,
}


class IngestionVerificationError(RuntimeError):
    pass


def verify_manifest(manifest: IngestionManifest, expected: dict[str, dict[str, int]] | None = None) -> None:
    """Assert counts match references. Raises IngestionVerificationError on mismatch.

    `expected` keys = manifest source IDs (e.g. "core", "ext:TPDM"). When None,
    we apply the DS 6.1.0 reference numbers iff the core manifest matches that DS.
    """
    if expected is None and manifest.data_standard_version == "6.1.0":
        expected = {"core": DS_610_EXPECTED}
    if not expected:
        return
    errors: list[str] = []
    for src, want in expected.items():
        got = manifest.counts.get(src)
        if got is None:
            errors.append(f"source {src!r} missing from manifest")
            continue
        for key, want_val in want.items():
            if got.get(key) != want_val:
                errors.append(f"{src}.{key}: expected {want_val}, got {got.get(key)}")
    if errors:
        raise IngestionVerificationError("\n".join(errors))
