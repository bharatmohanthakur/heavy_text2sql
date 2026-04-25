"""Layered configuration: YAML defaults + .env + process env, validated.

Provider routing is config-driven — see configs/default.yaml. Secrets never
live in YAML; YAML references env vars via `${NAME}` and the loader interpolates
from the merged environment (process env wins over .env).
"""

from __future__ import annotations

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
    for line in path.read_text().splitlines():
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


def load_config(
    config_path: Path | str | None = None,
    env_file: Path | str | None = None,
) -> AppConfig:
    """Load YAML config, interpolate env vars, and return a typed AppConfig."""
    cfg_path = Path(config_path) if config_path else REPO_ROOT / "configs" / "default.yaml"
    env_path = Path(env_file) if env_file else REPO_ROOT / ".env"

    env: dict[str, str] = {**_load_env_file(env_path), **os.environ}

    raw = yaml.safe_load(cfg_path.read_text())
    interpolated = _interpolate(raw, env)
    return AppConfig.model_validate(interpolated)
