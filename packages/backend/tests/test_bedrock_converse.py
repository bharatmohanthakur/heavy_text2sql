"""Bedrock Converse API request/response shape tests (mocked transport).

Live verification needs a working Bedrock API key in the right region;
these tests verify the wire shape matches the AWS Converse docs without
that dependency. Once a working key is provided, the same code paths run
end-to-end via the live-stack tests.
"""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import httpx
import pytest

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMMessage
from text2sql.providers.llm.bedrock import (
    BedrockLLM,
    _STRUCTURED_TOOL_NAME,
    _extract_json_objects,
)


def _make_llm() -> BedrockLLM:
    os.environ.setdefault("BEDROCK_API_KEY", "fake-key")
    return BedrockLLM(ProviderEntry(
        kind="bedrock",
        model="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        region="us-east-1",
        api_key_env="BEDROCK_API_KEY",
        max_tokens=64,
        temperature=0.0,
    ))


# ── complete() request shape ────────────────────────────────────────────


def test_complete_uses_converse_endpoint_with_bearer_auth():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"output": {"message": {"role": "assistant", "content": [{"text": "PONG"}]}}},
        )

    llm = _make_llm()
    llm._client = httpx.Client(
        base_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )
    out = llm.complete([LLMMessage(role="user", content="ping")])

    assert out == "PONG"
    # URL is the Converse endpoint, NOT the legacy /invoke
    assert captured["url"].endswith("/converse")
    assert "/converse-stream" not in captured["url"]
    # Bearer auth (API-key path)
    assert captured["headers"]["authorization"] == "Bearer fake-key"
    # Converse body shape
    body = captured["body"]
    assert "messages" in body and "inferenceConfig" in body
    assert body["inferenceConfig"]["maxTokens"] == 64
    assert body["inferenceConfig"]["temperature"] == 0.0
    # Each message wraps content in [{"text": "..."}]
    assert body["messages"] == [{"role": "user", "content": [{"text": "ping"}]}]


def test_complete_separates_system_into_top_level_field():
    captured: dict = {}

    def handler(request):
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200, json={"output": {"message": {"content": [{"text": "ok"}]}}},
        )

    llm = _make_llm()
    llm._client = httpx.Client(
        base_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )
    llm.complete([
        LLMMessage(role="system", content="You are SYS"),
        LLMMessage(role="user", content="hello"),
    ])
    body = captured["body"]
    # System prompts go to a top-level "system" field as [{"text": ...}]
    assert body["system"] == [{"text": "You are SYS"}]
    # ... and are NOT in the messages list
    assert all(m["role"] != "system" for m in body["messages"])


def test_complete_with_schema_uses_strict_toolUse():
    """Strict-schema mode forces a structured_response tool call via
    Converse's toolConfig + toolChoice, mirroring the Anthropic provider."""
    captured: dict = {}

    def handler(request):
        captured["body"] = json.loads(request.content)
        # Mimic the Converse toolUse response shape
        return httpx.Response(200, json={"output": {"message": {"role": "assistant",
            "content": [{"toolUse": {
                "toolUseId": "tu_1",
                "name": _STRUCTURED_TOOL_NAME,
                "input": {"sql": "SELECT 1", "rationale": "ok"},
            }}],
        }}})

    llm = _make_llm()
    llm._client = httpx.Client(
        base_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )
    schema = {
        "type": "object", "additionalProperties": False,
        "properties": {"sql": {"type": "string"}, "rationale": {"type": "string"}},
        "required": ["sql", "rationale"],
    }
    out = llm.complete([LLMMessage(role="user", content="dummy")], schema=schema)

    parsed = json.loads(out)
    assert parsed == {"sql": "SELECT 1", "rationale": "ok"}
    body = captured["body"]
    # Tool config shape per Converse docs
    tc = body["toolConfig"]
    assert tc["toolChoice"] == {"tool": {"name": _STRUCTURED_TOOL_NAME}}
    spec = tc["tools"][0]["toolSpec"]
    assert spec["name"] == _STRUCTURED_TOOL_NAME
    assert spec["inputSchema"] == {"json": schema}


def test_complete_surfaces_aws_error_body():
    """Bedrock's real error message must reach the caller — masked errors
    were a real debugging pain point (per the earlier 403 incident)."""
    def handler(request):
        return httpx.Response(403, json={"Message": "Authentication failed: Please make sure your API Key is valid."})

    llm = _make_llm()
    llm._client = httpx.Client(
        base_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RuntimeError) as ei:
        llm.complete([LLMMessage(role="user", content="x")])
    msg = str(ei.value)
    assert "403" in msg
    assert "Authentication failed" in msg
    assert "us-east-1" in msg
    assert llm._model in msg


# ── Capabilities (Step F flips strict + streaming to True) ──────────────


def test_capabilities_after_converse_migration():
    llm = _make_llm()
    caps = llm.capabilities
    assert caps.strict_json_schema is True       # via toolUse
    assert caps.token_streaming is True           # via ConverseStream
    assert caps.openai_tool_calling is False
    assert caps.anthropic_tool_use is True


# ── stream() — JSON extraction from eventstream-wrapped bytes ───────────


def test_extract_json_objects_finds_embedded_payloads():
    # Eventstream frames wrap JSON inside binary headers; we just need to
    # find every complete top-level JSON object, even when interleaved.
    binary_garbage = b"\x00\x00\x00\x80\x00:event-type"  # eventstream header
    payload1 = b'{"delta":{"text":"Hel"}}'
    payload2 = b'{"delta":{"text":"lo"}}'
    buf = binary_garbage + payload1 + b"\x00\x00\x00\x80" + payload2 + b"\x00\xff"
    objs = list(_extract_json_objects(buf))
    assert objs == [{"delta": {"text": "Hel"}}, {"delta": {"text": "lo"}}]


def test_extract_json_objects_ignores_truncated_tail():
    """A trailing partial object must not raise; we wait for more bytes."""
    buf = b'{"delta":{"text":"done"}}{"delta":{"text":"par'  # truncated
    objs = list(_extract_json_objects(buf))
    assert objs == [{"delta": {"text": "done"}}]


def test_stream_falls_back_to_complete_on_transport_error():
    """If converse-stream fails for any reason, stream() must still emit
    something — yielding from a successful complete() is the documented
    fallback."""
    call_count = {"stream": 0, "complete": 0}

    def handler(request):
        if "/converse-stream" in str(request.url):
            call_count["stream"] += 1
            # Force the stream to fail
            raise httpx.ConnectError("simulated network failure")
        else:
            call_count["complete"] += 1
            return httpx.Response(200, json={"output": {"message": {"content": [{"text": "fallback"}]}}})

    llm = _make_llm()
    llm._client = httpx.Client(
        base_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )
    chunks = list(llm.stream([LLMMessage(role="user", content="x")]))
    assert chunks == ["fallback"]
    assert call_count["stream"] == 1
    assert call_count["complete"] == 1
