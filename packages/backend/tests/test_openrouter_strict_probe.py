"""Probe + cache behavior for OpenRouter strict-schema fallback.

Verifies the runtime probe replaces the old hard-coded allow-list:
  1. First call to an unknown model with strict mode → server rejects with
     unsupported-shape error → provider catches, caches `False`, retries
     with soft fallback, succeeds.
  2. Second call to the same model → cache short-circuits straight to soft
     fallback (no second strict attempt, no rejection).
  3. Pinned positive model always goes strict (no probe needed).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMMessage
from text2sql.providers.llm.openrouter import OpenRouterLLM, _STRICT_CACHE


@pytest.fixture
def fresh_cache(tmp_path: Path):
    """Point the global cache at a temp file so tests don't pollute the
    real artifact, and reset its in-memory state."""
    new_path = tmp_path / "strict_cache.json"
    saved_path, saved_data, saved_loaded = _STRICT_CACHE._path, _STRICT_CACHE._data, _STRICT_CACHE._loaded
    _STRICT_CACHE._path = new_path
    _STRICT_CACHE._data = {}
    _STRICT_CACHE._loaded = True
    try:
        yield new_path
    finally:
        _STRICT_CACHE._path = saved_path
        _STRICT_CACHE._data = saved_data
        _STRICT_CACHE._loaded = saved_loaded


def _make_llm(model: str = "vendor/unknown-model") -> OpenRouterLLM:
    import os
    os.environ.setdefault("OPENROUTER_API_KEY", "fake")
    return OpenRouterLLM(ProviderEntry(
        kind="openrouter", model=model, api_key_env="OPENROUTER_API_KEY",
        max_tokens=64, temperature=0.0,
    ))


def _fake_response(payload: dict) -> object:
    """Mimic the OpenAI SDK ChatCompletion shape."""
    class _Msg:
        def __init__(self, content): self.content = content
    class _Choice:
        def __init__(self, content): self.message = _Msg(content)
    class _Resp:
        def __init__(self, content): self.choices = [_Choice(content)]
    return _Resp(json.dumps(payload))


def test_unknown_model_probes_then_caches_negative(fresh_cache):
    """First strict attempt → unsupported error → fallback succeeds → cache."""
    llm = _make_llm("vendor/unknown-model")
    schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"], "additionalProperties": False}
    call_log: list[dict] = []

    def fake_create(**kwargs):
        call_log.append(kwargs)
        if "response_format" in kwargs:
            raise RuntimeError("model 'vendor/unknown-model' does not support response_format json_schema")
        return _fake_response({"x": "ok"})

    with patch.object(llm._client.chat.completions, "create", side_effect=fake_create):
        out = llm.complete([LLMMessage(role="user", content="hi")], schema=schema)
        assert json.loads(out) == {"x": "ok"}

    # First strict attempt + soft retry → 2 calls total
    assert len(call_log) == 2
    assert "response_format" in call_log[0], "first attempt should try strict"
    assert "response_format" not in call_log[1], "second attempt should be soft fallback"
    # Cache persisted negative
    assert _STRICT_CACHE.get("vendor/unknown-model") is False
    assert fresh_cache.exists()


def test_second_call_short_circuits_via_cache(fresh_cache):
    """After negative cache, no strict attempt is made."""
    _STRICT_CACHE.put("vendor/unknown-model", False)
    llm = _make_llm("vendor/unknown-model")
    schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"], "additionalProperties": False}
    call_log: list[dict] = []

    def fake_create(**kwargs):
        call_log.append(kwargs)
        return _fake_response({"x": "cached"})

    with patch.object(llm._client.chat.completions, "create", side_effect=fake_create):
        llm.complete([LLMMessage(role="user", content="hi")], schema=schema)

    assert len(call_log) == 1, "cache must short-circuit to one call"
    assert "response_format" not in call_log[0], "must skip strict via cached negative"


def test_pinned_positive_always_strict(fresh_cache):
    """openai/gpt-4o is in the pinned list — always strict, no probe."""
    llm = _make_llm("openai/gpt-4o")
    schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"], "additionalProperties": False}
    call_log: list[dict] = []

    def fake_create(**kwargs):
        call_log.append(kwargs)
        return _fake_response({"x": "ok"})

    with patch.object(llm._client.chat.completions, "create", side_effect=fake_create):
        llm.complete([LLMMessage(role="user", content="hi")], schema=schema)

    assert len(call_log) == 1
    assert "response_format" in call_log[0], "pinned positive must use strict"


def test_strict_success_caches_positive(fresh_cache):
    """An unknown model that DOES accept strict gets a positive cache entry."""
    llm = _make_llm("vendor/strict-supporter")
    schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"], "additionalProperties": False}

    def fake_create(**kwargs):
        return _fake_response({"x": "ok"})

    with patch.object(llm._client.chat.completions, "create", side_effect=fake_create):
        llm.complete([LLMMessage(role="user", content="hi")], schema=schema)

    assert _STRICT_CACHE.get("vendor/strict-supporter") is True


def test_capabilities_dynamic_with_cache(fresh_cache):
    """`capabilities.strict_json_schema` reads the cache for unknown models."""
    llm = _make_llm("vendor/unknown-model")
    # Unknown → optimistic True (we'll probe on first call)
    assert llm.capabilities.strict_json_schema is True
    # Negative cache → False
    _STRICT_CACHE.put("vendor/unknown-model", False)
    assert llm.capabilities.strict_json_schema is False
    # Positive cache → True
    _STRICT_CACHE.put("vendor/unknown-model", True)
    assert llm.capabilities.strict_json_schema is True
