"""Admin/Settings API — runtime configuration via the frontend.

Endpoints (all under `/admin/`):
  GET  /admin/config             — resolved config with secrets redacted
  POST /admin/config             — write-through to runtime_overrides.json
  POST /admin/test_db            — smoke a target_db spec (`SELECT 1`)
  POST /admin/test_metadata_db   — smoke the metadata DB
  POST /admin/jobs/rebuild       — kick off a rebuild (subset of stages)
  GET  /admin/jobs/{id}          — current status + accumulated log
  GET  /admin/jobs/{id}/stream   — SSE stream of new log lines

Rules:
  - Secrets never travel through these routes. The body of a POST may
    only mention env-var NAMES (e.g. `password_env: TARGET_DB_PASSWORD`),
    never the secret value itself.
  - Writes go to data/artifacts/runtime_overrides.json (gitignored). On
    boot, load_config() merges this overlay over configs/default.yaml.
  - The factory + provider registry stay the source of truth for which
    `kind`s are legal — admin can't invent new provider kinds.

Returned shapes match what configs/default.yaml looks like, so the
frontend reads/writes one consistent schema.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from text2sql.config import (
    RUNTIME_OVERRIDES_PATH,
    RUNTIME_SECRETS_PATH,
    load_config,
)

log = logging.getLogger(__name__)


router = APIRouter(prefix="/admin", tags=["admin"])


# ── Helpers ────────────────────────────────────────────────────────────────


_SECRET_KEY_HINTS = ("api_key", "password", "secret", "token", "key_env")


def _redact(obj: Any) -> Any:
    """Walk a dict/list and replace any value at a secret-shaped key with
    a fixed marker. Keeps `*_env` (the NAME of the env var) visible since
    those are not secrets themselves — they tell the user which env var
    the secret comes from."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if any(h in k.lower() for h in ("api_key", "password", "secret", "token")) and not k.endswith("_env"):
                out[k] = "***"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _load_overlay() -> dict[str, Any]:
    p = Path(RUNTIME_OVERRIDES_PATH)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.warning("runtime overrides parse failed: %s", e)
        return {}


def _write_overlay(overlay: dict[str, Any]) -> None:
    p = Path(RUNTIME_OVERRIDES_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(overlay, indent=2))


# ── Request / response shapes ──────────────────────────────────────────────


class _LLMSection(BaseModel):
    primary: str
    fallback: str | None = None
    task_routing: dict[str, str] = Field(default_factory=dict)
    providers: dict[str, dict[str, Any]]


class _SimpleSection(BaseModel):
    primary: str
    providers: dict[str, dict[str, Any]]


class _ResolvedConfigResponse(BaseModel):
    llm: _LLMSection
    embeddings: _SimpleSection
    vector_store: _SimpleSection
    target_db: _SimpleSection
    metadata_db: dict[str, Any]
    overlay: dict[str, Any]
    overlay_path: str
    env_present: dict[str, bool]


class _ConfigPatch(BaseModel):
    """A partial overlay — only fields the user wants to change. Server
    deep-merges this into the existing overlay file."""
    llm: dict[str, Any] | None = None
    embeddings: dict[str, Any] | None = None
    vector_store: dict[str, Any] | None = None
    target_db: dict[str, Any] | None = None
    metadata_db: dict[str, Any] | None = None


class _DbTestRequest(BaseModel):
    """Request to test a DB connection. Either reference an existing
    provider entry by name, or pass `kind`+spec inline. Secrets always
    come from env-var names, never the body."""
    provider: str | None = None
    kind: str | None = None
    spec: dict[str, Any] | None = None


class _DbTestResponse(BaseModel):
    ok: bool
    error: str | None = None
    elapsed_ms: float | None = None
    server_version: str | None = None


# ── Routes ─────────────────────────────────────────────────────────────────


@router.get("/config", response_model=_ResolvedConfigResponse)
def get_config() -> dict[str, Any]:
    cfg = load_config()
    # cfg is a Pydantic model; dump → redact → return.
    blob = cfg.model_dump()
    overlay = _load_overlay()
    # Tell the frontend which env vars are actually populated so it can
    # render green/red dots next to provider entries that need credentials.
    env_present: dict[str, bool] = {}
    for section in ("llm", "embeddings", "vector_store", "target_db"):
        providers = blob.get(section, {}).get("providers") or {}
        for _, prov in providers.items():
            for key, value in prov.items():
                if key.endswith("_env") and isinstance(value, str):
                    env_present[value] = bool(os.environ.get(value))
    return {
        "llm": _redact(blob["llm"]),
        "embeddings": _redact(blob["embeddings"]),
        "vector_store": _redact(blob["vector_store"]),
        "target_db": _redact(blob["target_db"]),
        "metadata_db": _redact(blob.get("metadata_db") or {}),
        "overlay": overlay,
        "overlay_path": str(RUNTIME_OVERRIDES_PATH),
        "env_present": env_present,
    }


@router.post("/config", response_model=_ResolvedConfigResponse)
def patch_config(patch: _ConfigPatch) -> dict[str, Any]:
    """Write a partial overlay. Server merges into runtime_overrides.json.

    Reject patches that mention secret-shaped keys (api_key, password,
    secret, token) — those must come from .env, not the UI body.
    """
    payload = {k: v for k, v in patch.model_dump().items() if v is not None}
    _reject_secret_values(payload)
    existing = _load_overlay()
    merged = _deep_merge(existing, payload)
    # Validate that the merged config still loads — bad overlays would
    # otherwise brick boot.
    from text2sql.config import _deep_merge as _cfg_deep_merge  # noqa: F401
    try:
        # Try loading with the overlay we're about to write AND dereference
        # every `primary` so an unknown provider name is caught here, not
        # at request time. Pydantic alone doesn't catch this — it stores
        # `primary` as a free string.
        with _temp_overlay(merged):
            test_cfg = load_config()
            _ = test_cfg.llm_for_task(next(iter(test_cfg.llm.task_routing)) if test_cfg.llm.task_routing else "sql_generation")
            _ = test_cfg.embedding_provider()
            _ = test_cfg.vector_store_provider()
            _ = test_cfg.target_db_provider()
    except Exception as e:
        raise HTTPException(400, f"overlay would invalidate config: {e}") from e
    _write_overlay(merged)
    return get_config()


@router.post("/test_db", response_model=_DbTestResponse)
def test_db(req: _DbTestRequest) -> _DbTestResponse:
    """Build a SqlEngine from the requested provider spec and run SELECT 1.

    The spec can either reference an already-registered provider by name
    (`provider: "mssql-northridge"`) or pass an inline `kind` + spec.
    """
    cfg = load_config()
    if req.provider:
        spec_dict = cfg.target_db.providers.get(req.provider)
        if spec_dict is None:
            raise HTTPException(400, f"no such target_db provider: {req.provider}")
        spec = spec_dict
    elif req.kind and req.spec:
        from text2sql.config import ProviderEntry
        spec = ProviderEntry(kind=req.kind, **req.spec)
    else:
        raise HTTPException(400, "pass `provider` or `kind`+`spec`")

    import time
    from text2sql.providers import build_sql_engine
    t0 = time.perf_counter()
    try:
        engine = build_sql_engine(spec)
        rows = engine.execute("SELECT 1 AS ok", limit=1)
        elapsed = (time.perf_counter() - t0) * 1000.0
        version = None
        try:
            # Best-effort version probe per dialect
            if engine.dialect == "postgresql":
                version = (engine.execute("SELECT version() AS v", limit=1) or [{}])[0].get("v")
            elif engine.dialect == "mssql":
                version = (engine.execute("SELECT @@VERSION AS v", limit=1) or [{}])[0].get("v")
        except Exception:
            pass
        return _DbTestResponse(ok=True, elapsed_ms=elapsed, server_version=str(version)[:200] if version else None)
    except Exception as e:
        return _DbTestResponse(ok=False, error=f"{type(e).__name__}: {e}",
                               elapsed_ms=(time.perf_counter() - t0) * 1000.0)


# ── Rebuild orchestrator (in-process job runner + SSE log) ──────────────────


import asyncio
import shlex
import subprocess
import threading
import time
import uuid
from collections import deque

from fastapi.responses import StreamingResponse


# Stage name → CLI command. Each stage runs the same `text2sql` subcommand
# the operator would run by hand, so the orchestrator never duplicates stage
# logic.
_STAGE_COMMANDS: dict[str, list[str]] = {
    "ingest":              ["text2sql", "ingest"],
    "classify":            ["text2sql", "map-tables-cmd"],
    "graph":               ["text2sql", "build-fk-graph"],
    "catalog":             ["text2sql", "build-table-catalog-cmd"],
    "index":               ["text2sql", "index-catalog"],
    "gold-seed":           ["text2sql", "gold-seed"],
}


class _Job:
    """Append-only log + status for one rebuild run.

    Lives in process memory; not durable across restarts. Frontend polls
    /admin/jobs/{id} for status or subscribes to the SSE stream for live
    log tailing.
    """

    def __init__(self, stages: list[str]) -> None:
        self.id = uuid.uuid4().hex
        self.stages = stages
        self.created_at = time.time()
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.status: str = "pending"      # pending | running | succeeded | failed | cancelled
        self.current_stage: str | None = None
        self.exit_code: int | None = None
        self._lines: deque[str] = deque(maxlen=10_000)
        # Each subscriber gets its own asyncio.Queue; new lines fan out.
        self._subscribers: list[asyncio.Queue[str]] = []
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)
            for q in list(self._subscribers):
                try:
                    q.put_nowait(line)
                except Exception:
                    pass

    def subscribe(self) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue()
        # Replay everything we have so subscribers never miss earlier output.
        for line in list(self._lines):
            q.put_nowait(line)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[str]) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def to_status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "stages": self.stages,
            "current_stage": self.current_stage,
            "exit_code": self.exit_code,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "log_tail": list(self._lines)[-200:],
        }


_JOBS: dict[str, _Job] = {}


def _run_job(job: _Job, env: dict[str, str]) -> None:
    """Run each stage in sequence. Streams stdout+stderr line-by-line into
    job._lines so subscribers see output as it happens.
    """
    job.status = "running"
    job.started_at = time.time()
    overall_rc = 0
    try:
        for stage in job.stages:
            cmd = _STAGE_COMMANDS.get(stage)
            if cmd is None:
                job.append(f"[orchestrator] unknown stage: {stage}; skipping")
                continue
            job.current_stage = stage
            job.append(f"\n[stage] ──────── {stage} ──────── ({shlex.join(cmd)})\n")
            proc = subprocess.Popen(
                cmd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                bufsize=1, text=True,
            )
            assert proc.stdout is not None
            for raw in proc.stdout:
                job.append(raw.rstrip("\n"))
            rc = proc.wait()
            job.append(f"[stage] {stage} exit={rc}")
            if rc != 0:
                overall_rc = rc
                job.append(f"[orchestrator] stage {stage} failed; aborting subsequent stages")
                break
        job.exit_code = overall_rc
        job.status = "succeeded" if overall_rc == 0 else "failed"
    except Exception as e:
        job.append(f"[orchestrator] crashed: {type(e).__name__}: {e}")
        job.exit_code = -1
        job.status = "failed"
    finally:
        job.current_stage = None
        job.finished_at = time.time()


class _RebuildRequest(BaseModel):
    stages: list[str] = Field(default_factory=lambda: list(_STAGE_COMMANDS.keys()))


@router.post("/jobs/rebuild")
def post_rebuild(req: _RebuildRequest) -> dict[str, Any]:
    """Kick off a background rebuild. Returns the job id immediately;
    follow up with GET /admin/jobs/{id} or /admin/jobs/{id}/stream."""
    invalid = [s for s in req.stages if s not in _STAGE_COMMANDS]
    if invalid:
        raise HTTPException(400, f"unknown stage(s): {invalid}; "
                                  f"valid={list(_STAGE_COMMANDS)}")
    if not req.stages:
        raise HTTPException(400, "empty stages list")
    job = _Job(req.stages)
    _JOBS[job.id] = job
    # Pass the parent's env to the subprocess so credentials / TARGET_DB / etc.
    # are inherited.
    env = dict(os.environ)
    job._thread = threading.Thread(target=_run_job, args=(job, env), daemon=True)
    job._thread.start()
    return job.to_status()


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return job.to_status()


@router.get("/jobs/{job_id}/stream")
async def stream_job(job_id: str) -> StreamingResponse:
    """Server-Sent Events: each new log line emitted as `data: <line>\\n\\n`.
    Closes when the job reaches a terminal state."""
    job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(404, "job not found")

    async def _gen():
        q = job.subscribe()
        try:
            # Frame format identical to /chat/stream so the frontend can
            # reuse the same SSE plumbing.
            yield f"data: {json.dumps({'type': 'status', **job.to_status()})}\n\n"
            while True:
                # Heartbeat every ~5s so dropped connections fail fast.
                try:
                    line = await asyncio.wait_for(q.get(), timeout=5.0)
                    yield f"data: {json.dumps({'type': 'line', 'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ":heartbeat\n\n"
                if job.status in ("succeeded", "failed", "cancelled"):
                    # Drain anything in the queue, then send final status.
                    while not q.empty():
                        line = q.get_nowait()
                        yield f"data: {json.dumps({'type': 'line', 'line': line})}\n\n"
                    yield f"data: {json.dumps({'type': 'status', **job.to_status()})}\n\n"
                    return
        finally:
            job.unsubscribe(q)

    return StreamingResponse(
        _gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/test_metadata_db", response_model=_DbTestResponse)
def test_metadata_db() -> _DbTestResponse:
    """Probe the configured metadata DB (Postgres) — gold store +
    conversations live here."""
    import time
    cfg = load_config()
    spec = cfg.metadata_db.model_dump()
    pw = os.environ.get(spec.get("password_env") or "") if spec.get("password_env") else ""
    pw = pw or os.environ.get("METADATA_DB_PASSWORD") or os.environ.get("TARGET_DB_PASSWORD") or "edfi"
    url = (
        f"postgresql+psycopg://{spec['user']}:{pw}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )
    t0 = time.perf_counter()
    try:
        engine = sa.create_engine(url, future=True)
        with engine.connect() as conn:
            row = conn.execute(sa.text("SELECT version() AS v")).mappings().first()
        return _DbTestResponse(ok=True,
                               elapsed_ms=(time.perf_counter() - t0) * 1000.0,
                               server_version=str((row or {}).get("v") or "")[:200])
    except Exception as e:
        return _DbTestResponse(ok=False, error=f"{type(e).__name__}: {e}",
                               elapsed_ms=(time.perf_counter() - t0) * 1000.0)


# ── Internal helpers ───────────────────────────────────────────────────────


def _reject_secret_values(payload: Any) -> None:
    """Walk the payload; if any leaf value has a secret-shaped key (and
    isn't the conventional `*_env` pointer), reject the request."""
    def walk(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                p = f"{path}.{k}" if path else k
                if any(h in k.lower() for h in ("api_key", "password", "secret", "token")) and not k.endswith("_env"):
                    raise HTTPException(
                        400,
                        f"refusing to write secret value at {p!r}; pass the env var "
                        f"NAME via `*_env` keys instead. Secrets live in .env, not "
                        f"runtime_overrides.json.",
                    )
                walk(v, p)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")
    walk(payload)


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class _temp_overlay:
    """Context manager: write `overlay` to RUNTIME_OVERRIDES_PATH for the
    duration of the block, then restore the original. Used to validate a
    proposed overlay before persisting it."""

    def __init__(self, overlay: dict[str, Any]) -> None:
        self._overlay = overlay
        self._original: bytes | None = None
        self._path = Path(RUNTIME_OVERRIDES_PATH)

    def __enter__(self) -> None:
        self._original = self._path.read_bytes() if self._path.exists() else None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._overlay, indent=2))

    def __exit__(self, *exc: Any) -> None:
        if self._original is None:
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass
        else:
            self._path.write_bytes(self._original)


# ── Connector endpoints — proper UI-driven connection forms ──────────────────


def _load_secrets() -> dict[str, str]:
    p = Path(RUNTIME_SECRETS_PATH)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _write_secrets(secrets: dict[str, str]) -> None:
    p = Path(RUNTIME_SECRETS_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    # 0600 — owner read/write only.
    p.write_text(json.dumps(secrets, indent=2))
    try:
        os.chmod(p, 0o600)
    except Exception:
        pass


class _DatabaseConnector(BaseModel):
    """One-form-fits-all DB connector — what the UI submits."""
    name: str = Field(..., description="A short name for this connection (becomes the provider key)")
    kind: str = Field(..., description="postgresql | mssql")
    host: str
    port: int
    database: str
    user: str
    password: str = Field("", description="Plaintext password — stored in runtime_secrets.json (gitignored)")
    set_primary: bool = True
    # MSSQL-specific knobs (ignored for postgresql)
    trust_server_certificate: bool = True
    encrypt: bool = False
    driver: str = "pymssql"
    # Postgres-specific
    schema_search_path: list[str] = Field(default_factory=lambda: ["edfi", "tpdm"])


class _LLMConnector(BaseModel):
    name: str
    kind: str = Field(..., description="azure_openai | openai | anthropic | openrouter | bedrock")
    set_primary: bool = True
    api_key: str = ""
    # azure_openai
    endpoint: str = ""
    api_version: str = ""
    deployment: str = ""
    # openai / anthropic / openrouter / bedrock
    model: str = ""
    # bedrock
    region: str = "us-west-2"
    # routing
    max_tokens: int = 4096
    temperature: float = 0.0


class _EmbeddingConnector(BaseModel):
    name: str
    kind: str = Field(..., description="azure_openai | openai | sentence_transformers | bedrock")
    set_primary: bool = True
    api_key: str = ""
    # cloud
    endpoint: str = ""
    api_version: str = ""
    deployment: str = ""
    model: str = ""
    # local sentence-transformers
    device: str = "cpu"
    # bedrock
    region: str = "us-west-2"
    family: str = "titan"
    # common
    dim: int = 0
    batch_size: int = 32


def _env_var_name(provider_name: str, suffix: str) -> str:
    """Map a provider key + suffix to an env-var-style name we'll store
    in the secrets file. e.g. (\"my-prod-db\", \"PASSWORD\") → \"MY_PROD_DB_PASSWORD\"."""
    safe = "".join(c.upper() if c.isalnum() else "_" for c in provider_name)
    return f"{safe}_{suffix}"


def _build_db_provider_entry(c: _DatabaseConnector) -> tuple[dict[str, Any], dict[str, str]]:
    """Translate a DatabaseConnector form into a YAML-shape provider dict
    + a (env-name → value) secrets-update dict."""
    secrets: dict[str, str] = {}
    pwd_env = _env_var_name(c.name, "PASSWORD")
    if c.password:
        secrets[pwd_env] = c.password
    entry: dict[str, Any] = {
        "kind": c.kind,
        "host": c.host,
        "port": c.port,
        "database": c.database,
        "user": c.user,
        "password_env": pwd_env,
    }
    if c.kind == "postgresql":
        entry["schema_search_path"] = list(c.schema_search_path)
    elif c.kind == "mssql":
        entry["trust_server_certificate"] = bool(c.trust_server_certificate)
        entry["encrypt"] = bool(c.encrypt)
        entry["driver"] = c.driver
    return entry, secrets


def _build_llm_provider_entry(c: _LLMConnector) -> tuple[dict[str, Any], dict[str, str]]:
    secrets: dict[str, str] = {}
    api_key_env = _env_var_name(c.name, "API_KEY")
    if c.api_key:
        secrets[api_key_env] = c.api_key
    entry: dict[str, Any] = {
        "kind": c.kind,
        "api_key_env": api_key_env,
        "max_tokens": int(c.max_tokens),
        "temperature": float(c.temperature),
    }
    if c.kind == "azure_openai":
        entry.update({"endpoint": c.endpoint, "api_version": c.api_version, "deployment": c.deployment})
    elif c.kind in ("openai", "anthropic", "openrouter"):
        entry["model"] = c.model
    elif c.kind == "bedrock":
        entry["model"] = c.model
        entry["region"] = c.region
    return entry, secrets


def _build_embedding_provider_entry(c: _EmbeddingConnector) -> tuple[dict[str, Any], dict[str, str]]:
    secrets: dict[str, str] = {}
    api_key_env = _env_var_name(c.name, "API_KEY")
    entry: dict[str, Any] = {"kind": c.kind, "batch_size": int(c.batch_size)}
    if c.kind == "azure_openai":
        if c.api_key:
            secrets[api_key_env] = c.api_key
        entry.update({"deployment": c.deployment, "endpoint": c.endpoint,
                      "api_version": c.api_version, "api_key_env": api_key_env,
                      "dim": int(c.dim or 3072)})
    elif c.kind == "openai":
        if c.api_key:
            secrets[api_key_env] = c.api_key
        entry.update({"model": c.model, "api_key_env": api_key_env, "dim": int(c.dim or 1536)})
    elif c.kind == "sentence_transformers":
        entry.update({"model": c.model, "device": c.device,
                      "dim": int(c.dim) if c.dim else None, "normalize": True})
        # No api_key for local models
        entry = {k: v for k, v in entry.items() if v is not None}
    elif c.kind == "bedrock":
        if c.api_key:
            secrets[api_key_env] = c.api_key
        entry.update({"model": c.model, "region": c.region,
                      "family": c.family, "api_key_env": api_key_env,
                      "dim": int(c.dim or 1024)})
    return entry, secrets


def _persist_provider(
    section: str,
    provider_name: str,
    entry: dict[str, Any],
    secrets_update: dict[str, str],
    set_primary: bool,
) -> dict[str, Any]:
    """Common write-path: merge entry into overlay + section providers,
    optionally set as primary, persist secrets file. Validates the merged
    config still loads before persisting."""
    # 1. Merge secrets first (load_config will read them at validation time)
    if secrets_update:
        secrets = _load_secrets()
        secrets.update(secrets_update)
        _write_secrets(secrets)

    # 2. Merge the provider entry into the overlay
    overlay = _load_overlay()
    section_overlay = dict(overlay.get(section) or {})
    providers = dict(section_overlay.get("providers") or {})
    providers[provider_name] = entry
    section_overlay["providers"] = providers
    if set_primary:
        section_overlay["primary"] = provider_name
    overlay[section] = section_overlay

    # 3. Validate by loading; rollback secrets on failure
    try:
        with _temp_overlay(overlay):
            test_cfg = load_config()
            if section == "target_db":
                _ = test_cfg.target_db_provider()
            elif section == "embeddings":
                _ = test_cfg.embedding_provider()
            elif section == "llm":
                _ = test_cfg.llm_for_task("sql_generation")
    except Exception as e:
        # Roll back the secrets we wrote since the overlay won't be persisted
        if secrets_update:
            secrets = _load_secrets()
            for k in secrets_update:
                secrets.pop(k, None)
            _write_secrets(secrets)
        raise HTTPException(400, f"connector failed validation: {e}") from e

    _write_overlay(overlay)
    return overlay


# ── Database connector endpoints ───────────────────────────────────────────


@router.post("/connector/database/test", response_model=_DbTestResponse)
def test_database_connector(c: _DatabaseConnector) -> _DbTestResponse:
    """Build a SqlEngine from form values (without persisting) and run
    SELECT 1. Lets the UI surface "✓ connected, SQL Server 2022" before
    the user clicks Save."""
    import time
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_sql_engine

    # Stash the password under a temp env var so build_sql_engine's
    # _resolve_secret picks it up. Cleaned up in finally.
    tmp_env = f"_TEST_{_env_var_name(c.name, 'PASSWORD')}"
    os.environ[tmp_env] = c.password
    spec_dict = {
        "kind": c.kind, "host": c.host, "port": c.port,
        "database": c.database, "user": c.user, "password_env": tmp_env,
    }
    if c.kind == "postgresql":
        spec_dict["schema_search_path"] = list(c.schema_search_path)
    elif c.kind == "mssql":
        spec_dict["trust_server_certificate"] = c.trust_server_certificate
        spec_dict["encrypt"] = c.encrypt
        spec_dict["driver"] = c.driver
    t0 = time.perf_counter()
    try:
        engine = build_sql_engine(ProviderEntry(**spec_dict))
        engine.execute("SELECT 1 AS ok", limit=1)
        elapsed = (time.perf_counter() - t0) * 1000.0
        version = None
        try:
            if engine.dialect == "postgresql":
                version = (engine.execute("SELECT version() AS v", limit=1) or [{}])[0].get("v")
            elif engine.dialect == "mssql":
                version = (engine.execute("SELECT @@VERSION AS v", limit=1) or [{}])[0].get("v")
        except Exception:
            pass
        return _DbTestResponse(ok=True, elapsed_ms=elapsed,
                               server_version=str(version)[:200] if version else None)
    except Exception as e:
        return _DbTestResponse(ok=False, error=f"{type(e).__name__}: {e}",
                               elapsed_ms=(time.perf_counter() - t0) * 1000.0)
    finally:
        os.environ.pop(tmp_env, None)


@router.post("/connector/database", response_model=_ResolvedConfigResponse)
def save_database_connector(c: _DatabaseConnector) -> dict[str, Any]:
    """Register this DB as a target_db provider and (optionally) make it
    primary. Persists the password to the gitignored secrets file."""
    entry, secrets = _build_db_provider_entry(c)
    _persist_provider("target_db", c.name, entry, secrets, c.set_primary)
    return get_config()


# ── LLM connector endpoints ────────────────────────────────────────────────


class _LLMTestResponse(BaseModel):
    ok: bool
    error: str | None = None
    elapsed_ms: float | None = None
    sample: str | None = None


@router.post("/connector/llm/test", response_model=_LLMTestResponse)
def test_llm_connector(c: _LLMConnector) -> _LLMTestResponse:
    import time
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_llm
    from text2sql.providers.base import LLMMessage

    tmp_env = f"_TEST_{_env_var_name(c.name, 'API_KEY')}"
    os.environ[tmp_env] = c.api_key
    spec_dict = {
        "kind": c.kind, "api_key_env": tmp_env,
        "max_tokens": int(c.max_tokens), "temperature": float(c.temperature),
    }
    if c.kind == "azure_openai":
        spec_dict.update({"endpoint": c.endpoint, "api_version": c.api_version, "deployment": c.deployment})
    elif c.kind == "bedrock":
        spec_dict.update({"model": c.model, "region": c.region})
    else:
        spec_dict["model"] = c.model
    t0 = time.perf_counter()
    try:
        llm = build_llm(ProviderEntry(**spec_dict))
        out = llm.complete([LLMMessage(role="user", content="Reply with the single word: PONG")], max_tokens=16)
        return _LLMTestResponse(ok=True,
                                elapsed_ms=(time.perf_counter() - t0) * 1000.0,
                                sample=(out or "")[:200])
    except Exception as e:
        return _LLMTestResponse(ok=False, error=f"{type(e).__name__}: {e}",
                                elapsed_ms=(time.perf_counter() - t0) * 1000.0)
    finally:
        os.environ.pop(tmp_env, None)


@router.post("/connector/llm", response_model=_ResolvedConfigResponse)
def save_llm_connector(c: _LLMConnector) -> dict[str, Any]:
    entry, secrets = _build_llm_provider_entry(c)
    _persist_provider("llm", c.name, entry, secrets, c.set_primary)
    return get_config()


# ── Embedding connector endpoints ──────────────────────────────────────────


class _EmbeddingTestResponse(BaseModel):
    ok: bool
    error: str | None = None
    elapsed_ms: float | None = None
    dim: int | None = None


@router.post("/connector/embedding/test", response_model=_EmbeddingTestResponse)
def test_embedding_connector(c: _EmbeddingConnector) -> _EmbeddingTestResponse:
    import time
    from text2sql.config import ProviderEntry
    from text2sql.providers import build_embedding

    tmp_env = f"_TEST_{_env_var_name(c.name, 'API_KEY')}"
    os.environ[tmp_env] = c.api_key
    entry, _ = _build_embedding_provider_entry(c)
    # Override api_key_env to the temp one
    if "api_key_env" in entry:
        entry["api_key_env"] = tmp_env
    t0 = time.perf_counter()
    try:
        emb = build_embedding(ProviderEntry(**entry))
        v = emb.embed(["smoke test"], kind="query")
        return _EmbeddingTestResponse(
            ok=True, dim=int(v.shape[1]),
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
        )
    except Exception as e:
        return _EmbeddingTestResponse(ok=False, error=f"{type(e).__name__}: {e}",
                                      elapsed_ms=(time.perf_counter() - t0) * 1000.0)
    finally:
        os.environ.pop(tmp_env, None)


@router.post("/connector/embedding", response_model=_ResolvedConfigResponse)
def save_embedding_connector(c: _EmbeddingConnector) -> dict[str, Any]:
    entry, secrets = _build_embedding_provider_entry(c)
    _persist_provider("embeddings", c.name, entry, secrets, c.set_primary)
    return get_config()
