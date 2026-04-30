"""Layered configuration: YAML defaults + .env + process env, validated.

Provider routing is config-driven — see configs/default.yaml. Secrets never
live in YAML; YAML references env vars via `${NAME}` and the loader interpolates
from the merged environment (process env wins over .env).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

REPO_ROOT = Path(__file__).resolve().parents[4]

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        out[key] = val
    return out


def _interpolate(value: Any, env: dict[str, str]) -> Any:
    """Recursively replace ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):
        def repl(m: re.Match[str]) -> str:
            var, default = m.group(1), m.group(2)
            return env.get(var, default if default is not None else "")
        return _ENV_VAR_PATTERN.sub(repl, value)
    if isinstance(value, list):
        return [_interpolate(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v, env) for k, v in value.items()}
    return value


class GitHubSource(BaseModel):
    ods_repo: str
    ods_ref: str = "main"
    extensions_repo: str
    extensions_ref: str = "main"
    sql_dialect: str = "MsSql"


class EdFiConfig(BaseModel):
    data_standard_version: str
    extensions: list[str] = Field(default_factory=list)
    cache_dir: str
    artifact_dir: str
    github: GitHubSource


class ProviderEntry(BaseModel):
    """Generic provider entry; concrete provider classes validate their own keys."""
    model_config = ConfigDict(extra="allow")
    kind: str


class LLMSection(BaseModel):
    primary: str
    fallback: str | None = None
    task_routing: dict[str, str] = Field(default_factory=dict)
    providers: dict[str, ProviderEntry]


class EmbeddingsSection(BaseModel):
    primary: str
    providers: dict[str, ProviderEntry]


class VectorStoreSection(BaseModel):
    primary: str
    providers: dict[str, ProviderEntry]


class TargetDbSection(BaseModel):
    primary: str
    providers: dict[str, ProviderEntry]


class MetadataDb(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: str


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"


class AppConfig(BaseModel):
    ed_fi: EdFiConfig
    llm: LLMSection
    embeddings: EmbeddingsSection
    vector_store: VectorStoreSection
    target_db: TargetDbSection
    metadata_db: MetadataDb
    logging: LoggingConfig

    def llm_for_task(self, task: str) -> ProviderEntry:
        """Resolve the LLM provider for a named task, falling back to primary."""
        name = self.llm.task_routing.get(task, self.llm.primary)
        return self.llm.providers[name]

    def embedding_provider(self) -> ProviderEntry:
        return self.embeddings.providers[self.embeddings.primary]

    def vector_store_provider(self) -> ProviderEntry:
        return self.vector_store.providers[self.vector_store.primary]

    def target_db_provider(self) -> ProviderEntry:
        return self.target_db.providers[self.target_db.primary]

    # ── Per-provider artifact paths ─────────────────────────────────────────
    #
    # Every target_db provider gets its own artifact directory under
    #   data/artifacts/per_provider/<provider_name>/
    # holding catalog / classification / FK graph / APSP / FAISS / Steiner
    # cache / build manifest. This lets one deployment serve multiple
    # databases (e.g. mssql-azure-prod + my-sqlite-demo) without cross-
    # contaminating their catalogs, embeddings, or gold SQL.
    #
    # Backwards compat: legacy single-target deployments wrote artifacts
    # flat under data/artifacts/. `resolve_artifact_path` falls back to
    # the flat layout if the per-provider file is missing AND a flat one
    # exists — so upgrades don't break in-place. New writes always go to
    # the per-provider layout.

    def active_target_provider_name(self) -> str:
        return self.target_db.primary

    def per_provider_artifact_dir(self, provider_name: str | None = None) -> Path:
        """Directory holding ALL per-provider artifacts. Created on first
        write; reading code that finds it absent should fall through to
        flat-layout via `resolve_artifact_path`."""
        name = provider_name or self.active_target_provider_name()
        return REPO_ROOT / "data/artifacts/per_provider" / name


def metadata_sa_url(cfg: "AppConfig") -> str:
    """Derive an SQLAlchemy URL for the metadata DB.

    Branches on metadata_db.kind:
      - postgresql → postgresql+psycopg://user:pw@host:port/db
      - mssql      → mssql+pymssql://user:pw@host:port/db?tds_version=7.4
      - sqlite     → sqlite:///<abs path> (repo-relative resolves via REPO_ROOT;
                     `:memory:` passes through unchanged)

    SQLite is the zero-infra story: pair a SQLite target_db with a
    SQLite metadata_db and the whole platform runs out of a single
    folder of files — no Docker, no Postgres, no MSSQL.
    """
    import os

    spec = cfg.metadata_db.model_dump()
    kind = spec.get("kind", "postgresql")

    if kind == "sqlite":
        path = (spec.get("path") or "").strip()
        if not path:
            raise RuntimeError(
                "metadata_db kind=sqlite requires a `path` field in the config"
            )
        if path != ":memory:" and not os.path.isabs(path):
            path = str(REPO_ROOT / path)
        return f"sqlite:///{path}"

    pw_env = spec.get("password_env")
    password = os.environ.get(pw_env or "", "")
    user = spec.get("user", "")
    host = spec.get("host", "127.0.0.1")
    port = int(spec.get("port") or (5432 if kind == "postgresql" else 1433))
    database = spec.get("database", "")

    if kind == "mssql":
        return (
            f"mssql+pymssql://{user}:{password}"
            f"@{host}:{port}/{database}?tds_version=7.4"
        )
    if kind == "postgresql":
        return (
            f"postgresql+psycopg://{user}:{password}"
            f"@{host}:{port}/{database}"
        )
    raise RuntimeError(f"unknown metadata_db kind: {kind!r}")


def resolve_artifact_path(
    cfg: "AppConfig",
    filename: str,
    *,
    provider_name: str | None = None,
    write: bool = False,
) -> Path:
    """Pick the right path for an artifact file.

    Resolution order:
      1. data/artifacts/per_provider/<active>/<filename> (preferred)
      2. data/artifacts/<filename> (backwards-compat for pre-N1 deployments)

    For READS: if the per-provider file exists, return it; else if the
    flat file exists, return that; else return the per-provider path
    (caller will see FileNotFoundError, which is the right error).

    For WRITES: always return the per-provider path. Caller is
    responsible for `mkdir(parents=True, exist_ok=True)`.
    """
    per_provider = cfg.per_provider_artifact_dir(provider_name) / filename
    if write:
        return per_provider
    if per_provider.exists():
        return per_provider
    flat = REPO_ROOT / "data/artifacts" / filename
    if flat.exists():
        return flat
    return per_provider


RUNTIME_OVERRIDES_PATH = REPO_ROOT / "data/artifacts/runtime_overrides.json"
RUNTIME_SECRETS_PATH = REPO_ROOT / "data/artifacts/runtime_secrets.json"


def _load_runtime_secrets() -> dict[str, str]:
    """Read {ENV_VAR_NAME: value} from the gitignored secrets file.

    These are merged into the env dict during interpolation BELOW process
    env (so a real env var still wins), but ABOVE nothing else. This lets
    the UI take a plaintext password / API key, persist it on disk, and
    have it transparently fill `${MY_DB_PASSWORD}` references in YAML.
    """
    p = Path(RUNTIME_SECRETS_PATH)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}


def _deep_merge(base: Any, overlay: Any) -> Any:
    """Merge `overlay` over `base` recursively. dict + dict → recursive merge;
    everything else → overlay wins."""
    if isinstance(base, dict) and isinstance(overlay, dict):
        out = dict(base)
        for k, v in overlay.items():
            out[k] = _deep_merge(base.get(k), v) if k in base else v
        return out
    return overlay if overlay is not None else base


def load_config(
    config_path: Path | str | None = None,
    env_file: Path | str | None = None,
    *,
    overlay_path: Path | str | None = None,
) -> AppConfig:
    """Load YAML config, interpolate env vars, layer runtime overrides, and
    return a typed AppConfig.

    Resolution order (lowest → highest precedence):
      1. configs/default.yaml         (committed defaults)
      2. data/artifacts/runtime_overrides.json  (UI-written, gitignored)
      3. process env / .env           (interpolation source for ${VAR}s)

    Secrets stay in .env — the overlay only changes selectors (which
    provider is `primary`, which task routes where, embedding kind, etc.).
    The runtime overrides file is gitignored via the existing
    data/artifacts/ rule so it never leaks into commits.
    """
    cfg_path = Path(config_path) if config_path else REPO_ROOT / "configs" / "default.yaml"
    env_path = Path(env_file) if env_file else REPO_ROOT / ".env"
    overlay_p = Path(overlay_path) if overlay_path else RUNTIME_OVERRIDES_PATH

    # Resolution order for ${VAR} interpolation, lowest precedence first:
    #   1. data/artifacts/runtime_secrets.json (UI-managed)
    #   2. .env file
    #   3. process env
    env: dict[str, str] = {
        **_load_runtime_secrets(),
        **_load_env_file(env_path),
        **os.environ,
    }

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if overlay_p.exists():
        try:
            overlay = json.loads(overlay_p.read_text(encoding="utf-8"))
            raw = _deep_merge(raw, overlay)
        except Exception:
            # Bad overlay shouldn't brick boot — log and continue.
            pass
    interpolated = _interpolate(raw, env)
    return AppConfig.model_validate(interpolated)
