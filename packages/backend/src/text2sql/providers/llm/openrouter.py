"""OpenRouter LLM provider — OpenAI-API-compatible router across many models.

Lets us point the same factory at z-ai/glm-5.1, anthropic/claude-sonnet-4.6,
deepseek/deepseek-v3.2, qwen/qwen3.5-*, etc. without writing a new provider per
model. Uses the OpenAI SDK with base_url=https://openrouter.ai/api/v1.

Not every OpenRouter model honors `response_format: {type: "json_schema", strict}`.
We probe at first call: try strict mode; if OpenRouter rejects with an
"unsupported"-shaped error, fall back to soft-instruction-in-system-prompt
and persist the negative result to a small JSON cache so future calls
short-circuit straight to the fallback.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from threading import Lock
from typing import Any, Iterator

from openai import OpenAI

from text2sql.config import REPO_ROOT, ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm

log = logging.getLogger(__name__)


_DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"


# Pinned positive list — these are confirmed-supporting and bypass the probe
# (saves the first-call cost on the most common models). Probe + cache
# handles everything else.
_STRICT_JSON_SAFE = frozenset({
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "openai/gpt-4.1",
    "openai/gpt-4.1-mini",
})


# ── Strict-schema probe cache ────────────────────────────────────────────────


_DEFAULT_CACHE_PATH = REPO_ROOT / "data/artifacts/.openrouter_strict_cache.json"
_CACHE_TTL_SECONDS = 7 * 24 * 3600  # 1 week


class _StrictCache:
    """JSON-file-backed cache of {model_id → {strict: bool, ts: float}}.

    Survives process restarts so we don't re-probe every cold start. Read on
    first use, written on every change. Small (one entry per OpenRouter
    model the user has touched), so we just rewrite the whole file.
    """

    def __init__(self, path: Path = _DEFAULT_CACHE_PATH) -> None:
        self._path = path
        self._lock = Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            try:
                if self._path.exists():
                    self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception as e:
                log.debug("strict-cache load failed (%s); starting empty", e)
                self._data = {}
            self._loaded = True

    def get(self, model: str) -> bool | None:
        """Return cached result if fresh, else None."""
        self._load()
        entry = self._data.get(model)
        if not entry:
            return None
        ts = float(entry.get("ts", 0.0))
        if time.time() - ts > _CACHE_TTL_SECONDS:
            return None
        return bool(entry.get("strict"))

    def put(self, model: str, strict: bool) -> None:
        self._load()
        with self._lock:
            self._data[model] = {"strict": strict, "ts": time.time()}
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
            except Exception as e:
                log.debug("strict-cache write failed: %s", e)


_STRICT_CACHE = _StrictCache()


def _probe_strict_schema_supported(model: str, error_text: str) -> bool:
    """Heuristic: does the OpenRouter error message indicate this model
    doesn't support strict response_format? Returns False if it looks like
    an unsupported error, True if it looks like a different (transient) error.
    """
    needle = error_text.lower()
    return not any(s in needle for s in (
        "response_format",
        "json_schema",
        "structured output",
        "not supported",
        "does not support",
    ))


class OpenRouterLLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._base_url: str = cfg.get("base_url", _DEFAULT_BASE_URL)
        api_key = _resolve_secret(cfg["api_key_env"])
        # Optional headers OpenRouter recommends for analytics.
        default_headers: dict[str, str] = {}
        if cfg.get("http_referer"):
            default_headers["HTTP-Referer"] = cfg["http_referer"]
        if cfg.get("x_title"):
            default_headers["X-Title"] = cfg["x_title"]
        self._client = OpenAI(
            api_key=api_key,
            base_url=self._base_url,
            default_headers=default_headers or None,
        )
        self._default_max_tokens: int = int(cfg.get("max_tokens", 4096))
        self._default_temperature: float = float(cfg.get("temperature", 0.0))
        # Free-form passthrough knobs (e.g. {"reasoning": {"enabled": false}}
        # for GLM, {"reasoning_effort": "low"} for OpenAI o-series, etc.)
        self._extra_body: dict[str, Any] = dict(cfg.get("extra_body") or {})
        # `reasoning` is OpenRouter's normalized field; if the user set it on
        # the spec directly, fold it in.
        if cfg.get("reasoning") is not None:
            self._extra_body["reasoning"] = cfg["reasoning"]

    @property
    def model_id(self) -> str:
        return f"openrouter:{self._model}"

    @property
    def capabilities(self) -> LLMCapabilities:
        # strict_json_schema is dynamic: pinned positive list short-circuits;
        # everything else consults the probe-result cache. If the model
        # hasn't been probed yet, we report True optimistically — the
        # complete() path will probe on first call and update the cache.
        # openai_tool_calling: most models on OpenRouter expose OpenAI-shape
        # tool_calls, but Anthropic-routed models behave differently. We
        # stay conservative and report False; explicit OpenAI-routed models
        # can be added later if needed.
        if self._model in _STRICT_JSON_SAFE:
            strict = True
        else:
            cached = _STRICT_CACHE.get(self._model)
            strict = True if cached is None else cached
        return LLMCapabilities(
            strict_json_schema=strict,
            token_streaming=True,
            openai_tool_calling=False,
            anthropic_tool_use=False,
        )

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        # Decide whether to attempt strict response_format for this model.
        # Pinned positive list always uses strict; otherwise consult the cache.
        # If the cache says unsupported OR is unknown, we have two paths:
        #   - cached negative: skip strict, go straight to soft-instruction
        #   - unknown: probe (try strict, fall back on documented error)
        try_strict = schema is not None and (
            self._model in _STRICT_JSON_SAFE
            or _STRICT_CACHE.get(self._model) is not False
        )
        return self._complete_inner(
            messages, schema=schema, temperature=temperature, max_tokens=max_tokens,
            use_strict=try_strict, allow_probe=schema is not None and self._model not in _STRICT_JSON_SAFE,
        )

    def _complete_inner(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None,
        temperature: float | None,
        max_tokens: int | None,
        use_strict: bool,
        allow_probe: bool,
    ) -> str:
        chat = [{"role": m.role, "content": m.content} for m in messages]
        schema_for_api: dict[str, Any] | None = None
        if schema is not None and use_strict:
            schema_for_api = schema
        elif schema is not None:
            # Soft fallback: fold schema into system prompt.
            instr = (
                "Reply with JSON only conforming to this JSON-Schema:\n"
                + json.dumps(schema)
            )
            if chat and chat[0]["role"] == "system":
                chat[0]["content"] = chat[0]["content"] + "\n\n" + instr
            else:
                chat.insert(0, {"role": "system", "content": instr})

        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": chat,
            "temperature": self._default_temperature if temperature is None else temperature,
            "max_tokens": max_tokens or self._default_max_tokens,
        }
        if schema_for_api is not None:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "structured", "schema": schema_for_api, "strict": True},
            }
        if self._extra_body:
            kwargs["extra_body"] = self._extra_body

        try:
            resp = self._client.chat.completions.create(**kwargs)
        except Exception as e:
            # Probe: if strict mode triggered an "unsupported"-shaped error,
            # cache the negative and retry with soft fallback.
            if allow_probe and use_strict and not _probe_strict_schema_supported(
                self._model, str(e)
            ):
                log.info(
                    "openrouter: %s does not support strict response_format "
                    "(error: %s); caching and falling back to soft-instruction.",
                    self._model, str(e)[:100],
                )
                _STRICT_CACHE.put(self._model, False)
                return self._complete_inner(
                    messages, schema=schema, temperature=temperature,
                    max_tokens=max_tokens, use_strict=False, allow_probe=False,
                )
            raise

        # Strict succeeded — record the positive result so future calls skip
        # any uncertainty (and the cache survives restarts).
        if use_strict and schema is not None and allow_probe:
            _STRICT_CACHE.put(self._model, True)

        text = resp.choices[0].message.content or ""
        return _strip_fences(text)

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]:
        stream = self._client.chat.completions.create(
            model=self._model,
            messages=[{"role": m.role, "content": m.content} for m in messages],
            temperature=self._default_temperature,
            max_tokens=self._default_max_tokens,
            stream=True,
        )
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


def _strip_fences(text: str) -> str:
    s = text.strip()
    if not s.startswith("```"):
        return s
    # Strip ```json or ``` prefix
    s = s.lstrip("`")
    if s.lower().startswith("json"):
        s = s[4:]
    s = s.strip()
    if s.endswith("```"):
        s = s[:-3].rstrip()
    return s


@register_llm("openrouter")
def _build(spec: ProviderEntry) -> OpenRouterLLM:
    return OpenRouterLLM(spec)
