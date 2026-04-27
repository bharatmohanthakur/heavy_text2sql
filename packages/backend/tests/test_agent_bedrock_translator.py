"""Step L — Bedrock backend translator for the /chat agent loop.

Verifies _BedrockAnthropicToolBackend converts the loop's OpenAI-shape
interface to AWS Bedrock Converse API on both directions:

  Outgoing:
    - Anthropic-format content blocks renamed to Converse shape
      (text, tool_use, tool_result → text, toolUse, toolResult)
    - tools=[{type:"function", function:{name, parameters}}]
      → toolConfig.tools=[{toolSpec:{name, description, inputSchema:{json}}}]
    - tool_choice "auto" / "required" / {type:"tool",name} → Converse
      {auto:{}} / {any:{}} / {tool:{name}}
    - system messages → top-level system=[{text:...}], NOT in messages
    - URL: /model/{id}/converse-stream, Authorization: Bearer <key>

  Incoming streaming:
    - contentBlockStart{start:{toolUse:{toolUseId,name}}} → tool_call_delta
      with index, id, name, args=""
    - contentBlockDelta{delta:{text}} → text_delta
    - contentBlockDelta{delta:{toolUse:{input}}} → tool_call_delta with
      arguments_delta (incremental JSON)
    - end-of-stream → complete event with assembled OpenAI-shape tool_calls

Mocks the httpx.Client transport so no AWS call is made.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
import pytest

from text2sql.agent.loop import _BedrockAnthropicToolBackend


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_backend(*, region: str = "us-west-2") -> _BedrockAnthropicToolBackend:
    os.environ["BEDROCK_AGENT_KEY"] = "fake-key"
    return _BedrockAnthropicToolBackend(
        cfg={
            "kind": "bedrock",
            "model": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
            "region": region,
            "api_key_env": "BEDROCK_AGENT_KEY",
        },
        max_tokens=512,
    )


def _stream_response(events: list[dict[str, Any]]) -> bytes:
    """Synthesize an eventstream-like body: a JSON object per "frame".

    `_extract_json_objects` walks the bytes and pulls out every complete
    top-level JSON object — the binary framing AWS uses is irrelevant to
    the parser, so we just sprinkle some prefix/suffix bytes between
    payloads to match the production shape.
    """
    parts: list[bytes] = []
    for ev in events:
        parts.append(b"\x00\x00\x00\x80\x00:event-type")  # fake frame header
        parts.append(json.dumps(ev).encode("utf-8"))
        parts.append(b"\x00\xff")
    return b"".join(parts)


def _patch_transport(backend: _BedrockAnthropicToolBackend, handler) -> None:
    backend._client = httpx.Client(
        base_url=f"https://bedrock-runtime.{backend._region}.amazonaws.com",
        transport=httpx.MockTransport(handler),
    )


# ── Outgoing translation ────────────────────────────────────────────────────


def test_anthropic_blocks_to_converse_renames_keys():
    """Anthropic block format → Converse block format with key renames."""
    anth = [
        {"role": "user", "content": [{"type": "text", "text": "hi"}]},
        {"role": "assistant", "content": [
            {"type": "text", "text": "calling"},
            {"type": "tool_use", "id": "tu_1", "name": "run_sql",
             "input": {"sql": "SELECT 1"}},
        ]},
        {"role": "user", "content": [{"type": "tool_result",
                                       "tool_use_id": "tu_1",
                                       "content": '{"ok":true}'}]},
    ]
    out = _BedrockAnthropicToolBackend._anthropic_blocks_to_converse(anth)
    assert out[0] == {"role": "user", "content": [{"text": "hi"}]}
    asst = out[1]
    assert asst["role"] == "assistant"
    assert asst["content"][0] == {"text": "calling"}
    assert asst["content"][1] == {"toolUse": {
        "toolUseId": "tu_1", "name": "run_sql", "input": {"sql": "SELECT 1"},
    }}
    # tool_result is always wrapped in a Converse content list of {text:...}
    assert out[2]["content"][0] == {"toolResult": {
        "toolUseId": "tu_1",
        "content": [{"text": '{"ok":true}'}],
    }}


def test_anthropic_tools_to_converse_wraps_input_schema():
    anth_tools = [
        {"name": "run_sql", "description": "Execute.",
         "input_schema": {"type": "object",
                          "properties": {"sql": {"type": "string"}},
                          "required": ["sql"]}},
        {"name": "final_answer", "description": "Stop.",
         "input_schema": {"type": "object", "properties": {}}},
    ]
    out = _BedrockAnthropicToolBackend._anthropic_tools_to_converse(anth_tools)
    assert out[0]["toolSpec"]["name"] == "run_sql"
    assert out[0]["toolSpec"]["description"] == "Execute."
    assert out[0]["toolSpec"]["inputSchema"] == {"json": anth_tools[0]["input_schema"]}
    assert out[1]["toolSpec"]["inputSchema"]["json"]["properties"] == {}


def test_map_tool_choice_handles_string_and_struct_forms():
    M = _BedrockAnthropicToolBackend._map_tool_choice
    assert M("auto") == {"auto": {}}
    assert M("required") == {"any": {}}
    assert M("any") == {"any": {}}
    # Defensive: string "none" or unknown → auto
    assert M("none") == {"auto": {}}
    # OpenAI-shape forced tool selector → Converse {tool:{name}}
    assert M({"type": "tool", "name": "final_answer"}) == {"tool": {"name": "final_answer"}}
    assert M({"type": "auto"}) == {"auto": {}}
    assert M({"type": "any"}) == {"any": {}}


# ── Wire-shape: full request payload via mocked transport ──────────────────


def test_stream_chat_posts_to_converse_stream_with_bearer_auth():
    backend = _make_backend(region="us-east-1")
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        # Empty stream — just a messageStop so the loop exits cleanly.
        body = _stream_response([{"stopReason": "end_turn"}])
        return httpx.Response(200, content=body)

    _patch_transport(backend, handler)
    list(backend.stream_chat(
        messages=[
            {"role": "system", "content": "You are SYS."},
            {"role": "user", "content": "hi"},
        ],
        tools=[{"type": "function", "function": {
            "name": "x", "description": "do x",
            "parameters": {"type": "object", "properties": {}},
        }}],
    ))

    # URL is the streaming endpoint — NOT /converse, NOT /v1/messages
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/converse-stream")
    assert "/v1/messages" not in captured["url"]
    # Bedrock API-key auth path (NOT x-api-key)
    assert captured["headers"]["authorization"] == "Bearer fake-key"
    assert "x-api-key" not in captured["headers"]
    # Converse body shape:
    body = captured["body"]
    assert body["inferenceConfig"]["maxTokens"] == 512
    assert body["inferenceConfig"]["temperature"] == 0.0
    assert body["system"] == [{"text": "You are SYS."}]
    assert body["messages"] == [{"role": "user", "content": [{"text": "hi"}]}]
    # toolConfig.tools[].toolSpec.inputSchema.json shape
    spec = body["toolConfig"]["tools"][0]["toolSpec"]
    assert spec["name"] == "x"
    assert spec["inputSchema"] == {"json": {"type": "object", "properties": {}}}
    assert body["toolConfig"]["toolChoice"] == {"auto": {}}


def test_stream_chat_round_trips_assistant_tool_calls_and_tool_results():
    """An assistant message with tool_calls + a tool message with results
    must convert to Converse toolUse + toolResult blocks correctly."""
    backend = _make_backend()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, content=_stream_response([{"stopReason": "end_turn"}]))

    _patch_transport(backend, handler)
    list(backend.stream_chat(
        messages=[
            {"role": "user", "content": "How many?"},
            {"role": "assistant", "content": "", "tool_calls": [{
                "id": "call_xyz", "type": "function",
                "function": {"name": "run_sql",
                              "arguments": '{"sql": "SELECT COUNT(*) FROM Student"}'},
            }]},
            {"role": "tool", "content": '{"ok": true, "rows": [{"c": 100}]}',
             "tool_call_id": "call_xyz", "tool_name": "run_sql"},
        ],
        tools=[{"type": "function", "function": {
            "name": "run_sql",
            "parameters": {"type": "object",
                           "properties": {"sql": {"type": "string"}}},
        }}],
    ))

    msgs = captured["body"]["messages"]
    # User message preserved as Converse text block.
    assert msgs[0] == {"role": "user", "content": [{"text": "How many?"}]}
    # Assistant tool_call → toolUse block (Converse keys, not Anthropic keys)
    asst = msgs[1]
    assert asst["role"] == "assistant"
    tu = asst["content"][0]
    assert tu == {"toolUse": {"toolUseId": "call_xyz", "name": "run_sql",
                               "input": {"sql": "SELECT COUNT(*) FROM Student"}}}
    # Tool result → user-role message with toolResult; content wrapped in [{text}]
    tr = msgs[2]
    assert tr["role"] == "user"
    assert tr["content"][0] == {"toolResult": {
        "toolUseId": "call_xyz",
        "content": [{"text": '{"ok": true, "rows": [{"c": 100}]}'}],
    }}


# ── Incoming streaming translation ──────────────────────────────────────────


def test_stream_chat_translates_text_only():
    """A Converse stream with text deltas yields text_delta + complete (no tools)."""
    backend = _make_backend()

    body = _stream_response([
        {"role": "assistant"},                                      # messageStart
        {"contentBlockIndex": 0, "delta": {"text": "Hel"}},
        {"contentBlockIndex": 0, "delta": {"text": "lo"}},
        {"contentBlockIndex": 0},                                   # contentBlockStop
        {"stopReason": "end_turn"},                                 # messageStop
    ])

    def handler(request): return httpx.Response(200, content=body)
    _patch_transport(backend, handler)

    out = list(backend.stream_chat(
        messages=[{"role": "user", "content": "Hi"}],
        tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
    ))

    text_events = [e for e in out if e["type"] == "text_delta"]
    assert text_events == [
        {"type": "text_delta", "delta": "Hel"},
        {"type": "text_delta", "delta": "lo"},
    ]
    assert out[-1] == {"type": "complete", "content": "Hello", "tool_calls": []}


def test_stream_chat_translates_tool_use_block():
    """contentBlockStart(toolUse) + input partials → tool_call_delta events
    + complete event with OpenAI-shape tool_calls."""
    backend = _make_backend()

    body = _stream_response([
        {"role": "assistant"},
        {"contentBlockIndex": 0,
         "start": {"toolUse": {"toolUseId": "tu_abc", "name": "run_sql"}}},
        {"contentBlockIndex": 0,
         "delta": {"toolUse": {"input": '{"s'}}},
        {"contentBlockIndex": 0,
         "delta": {"toolUse": {"input": 'ql": "SELECT 1"}'}}},
        {"contentBlockIndex": 0},
        {"stopReason": "tool_use"},
    ])

    def handler(request): return httpx.Response(200, content=body)
    _patch_transport(backend, handler)

    out = list(backend.stream_chat(
        messages=[{"role": "user", "content": "count"}],
        tools=[{"type": "function", "function": {
            "name": "run_sql",
            "parameters": {"type": "object",
                           "properties": {"sql": {"type": "string"}}},
        }}],
    ))

    tool_deltas = [e for e in out if e["type"] == "tool_call_delta"]
    # Header (id+name, args=""), then two arg fragments
    assert tool_deltas[0]["id"] == "tu_abc"
    assert tool_deltas[0]["name"] == "run_sql"
    assert tool_deltas[0]["arguments_delta"] == ""
    assert tool_deltas[0]["index"] == 0
    assert tool_deltas[1]["arguments_delta"] == '{"s'
    assert tool_deltas[2]["arguments_delta"] == 'ql": "SELECT 1"}'

    final = out[-1]
    assert final["type"] == "complete"
    assert final["content"] == ""
    assert len(final["tool_calls"]) == 1
    tc = final["tool_calls"][0]
    assert tc["id"] == "tu_abc"
    assert tc["type"] == "function"
    assert tc["function"]["name"] == "run_sql"
    assert json.loads(tc["function"]["arguments"]) == {"sql": "SELECT 1"}


def test_stream_chat_handles_text_then_tool_use_in_same_turn():
    """Many models emit a text preamble before the toolUse block. Both
    must surface in the final complete event."""
    backend = _make_backend()

    body = _stream_response([
        {"contentBlockIndex": 0, "delta": {"text": "Calling tool. "}},
        {"contentBlockIndex": 0},
        {"contentBlockIndex": 1,
         "start": {"toolUse": {"toolUseId": "tu_1", "name": "x"}}},
        {"contentBlockIndex": 1, "delta": {"toolUse": {"input": "{}"}}},
        {"contentBlockIndex": 1},
        {"stopReason": "tool_use"},
    ])

    def handler(request): return httpx.Response(200, content=body)
    _patch_transport(backend, handler)

    out = list(backend.stream_chat(
        messages=[{"role": "user", "content": "go"}],
        tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
    ))

    text_deltas = [e for e in out if e["type"] == "text_delta"]
    tool_deltas = [e for e in out if e["type"] == "tool_call_delta"]
    assert text_deltas == [{"type": "text_delta", "delta": "Calling tool. "}]
    assert len(tool_deltas) >= 1
    final = out[-1]
    assert final["content"] == "Calling tool. "
    assert len(final["tool_calls"]) == 1
    assert final["tool_calls"][0]["function"]["name"] == "x"


def test_stream_chat_surfaces_aws_error_with_model_and_region():
    """Real AWS error text must reach the caller — not a generic message."""
    backend = _make_backend(region="us-west-2")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={
            "Message": "Authentication failed: Please make sure your API Key is valid.",
        })

    _patch_transport(backend, handler)
    with pytest.raises(RuntimeError) as ei:
        list(backend.stream_chat(
            messages=[{"role": "user", "content": "x"}],
            tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
        ))
    msg = str(ei.value)
    assert "403" in msg
    assert "Authentication failed" in msg
    assert backend._model in msg
    assert "us-west-2" in msg


# ── Dispatch wiring ─────────────────────────────────────────────────────────


def test_llm_client_dispatches_bedrock_to_bedrock_backend():
    """`_LLMClient` must select `_BedrockAnthropicToolBackend` for kind=bedrock."""
    from text2sql.agent.loop import _LLMClient
    from text2sql.config import ProviderEntry

    os.environ["BEDROCK_AGENT_KEY"] = "fake-key"
    spec = ProviderEntry(
        kind="bedrock",
        model="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        region="us-west-2",
        api_key_env="BEDROCK_AGENT_KEY",
    )
    client = _LLMClient(spec, max_tokens=128)
    assert isinstance(client._backend, _BedrockAnthropicToolBackend)
