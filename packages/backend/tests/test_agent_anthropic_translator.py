"""Step H — Anthropic backend translator for the agent loop.

Verifies the OpenAI ↔ Anthropic shape conversions on both directions:

  Outgoing:
    - OpenAI tools=[{type:"function", function:{name, parameters}}]
      → Anthropic tools=[{name, description, input_schema}]
    - System / user / assistant-with-tool_calls / tool messages → Anthropic
      `system=` field + alternating user/assistant turns with `tool_use`
      and `tool_result` content blocks

  Incoming:
    - content_block_start (tool_use) → tool_call_delta(index, id, name, args="")
    - content_block_delta (text_delta) → text_delta
    - content_block_delta (input_json_delta) → tool_call_delta(args partial)
    - end-of-stream → complete event with assembled OpenAI-shape tool_calls
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import MagicMock, patch

from text2sql.agent.loop import _AnthropicToolBackend


# ── Outgoing translation ────────────────────────────────────────────────────


def test_openai_tools_to_anthropic_keeps_name_desc_and_params():
    openai_tools = [
        {"type": "function", "function": {
            "name": "run_sql",
            "description": "Execute a SELECT.",
            "parameters": {"type": "object", "properties": {"sql": {"type": "string"}},
                           "required": ["sql"], "additionalProperties": False},
        }},
        {"type": "function", "function": {
            "name": "final_answer",
            "description": "Stop the loop.",
            "parameters": {"type": "object", "properties": {"summary": {"type": "string"}}},
        }},
    ]
    out = _AnthropicToolBackend._openai_tools_to_anthropic(openai_tools)
    assert len(out) == 2
    assert out[0]["name"] == "run_sql"
    assert out[0]["description"] == "Execute a SELECT."
    assert out[0]["input_schema"]["properties"] == {"sql": {"type": "string"}}
    assert out[1]["name"] == "final_answer"


def test_openai_messages_to_anthropic_basic_flow():
    """system + user + assistant(text) → system field + 2 messages."""
    msgs = [
        {"role": "system", "content": "You are a database analyst."},
        {"role": "user", "content": "How many students?"},
        {"role": "assistant", "content": "I'll check."},
    ]
    system, out = _AnthropicToolBackend._openai_messages_to_anthropic(msgs)
    assert system == "You are a database analyst."
    assert out == [
        {"role": "user", "content": [{"type": "text", "text": "How many students?"}]},
        {"role": "assistant", "content": [{"type": "text", "text": "I'll check."}]},
    ]


def test_openai_messages_to_anthropic_tool_call_round_trip():
    """assistant(tool_calls) + tool result → tool_use + tool_result blocks."""
    msgs = [
        {"role": "user", "content": "How many students?"},
        {"role": "assistant", "content": "", "tool_calls": [{
            "id": "call_xyz",
            "type": "function",
            "function": {"name": "run_sql", "arguments": '{"sql": "SELECT 1"}'},
        }]},
        {"role": "tool", "content": '{"ok": true, "rows": [...]}',
         "tool_call_id": "call_xyz", "tool_name": "run_sql"},
    ]
    system, out = _AnthropicToolBackend._openai_messages_to_anthropic(msgs)
    assert system == ""
    # User message intact
    assert out[0] == {"role": "user", "content": [{"type": "text", "text": "How many students?"}]}
    # Assistant tool_call → tool_use block (input is a parsed dict, NOT a string)
    asst = out[1]
    assert asst["role"] == "assistant"
    tu = asst["content"][0]
    assert tu == {"type": "tool_use", "id": "call_xyz", "name": "run_sql", "input": {"sql": "SELECT 1"}}
    # Tool result → user message with tool_result block
    tr = out[2]
    assert tr["role"] == "user"
    assert tr["content"][0] == {
        "type": "tool_result", "tool_use_id": "call_xyz",
        "content": '{"ok": true, "rows": [...]}',
    }


def test_openai_messages_concatenates_multiple_systems():
    msgs = [
        {"role": "system", "content": "PART ONE."},
        {"role": "system", "content": "PART TWO."},
        {"role": "user", "content": "go"},
    ]
    system, out = _AnthropicToolBackend._openai_messages_to_anthropic(msgs)
    assert system == "PART ONE.\n\nPART TWO."
    assert len(out) == 1


# ── Incoming streaming translation ──────────────────────────────────────────


def _make_event(ev_type: str, **kwargs: Any) -> Any:
    """Build a MagicMock matching the Anthropic SDK's event shape."""
    ev = MagicMock()
    ev.type = ev_type
    for k, v in kwargs.items():
        setattr(ev, k, v)
    return ev


def _make_content_block(block_type: str, **kwargs: Any) -> Any:
    blk = MagicMock()
    blk.type = block_type
    for k, v in kwargs.items():
        setattr(blk, k, v)
    return blk


def _make_delta(delta_type: str, **kwargs: Any) -> Any:
    d = MagicMock()
    d.type = delta_type
    for k, v in kwargs.items():
        setattr(d, k, v)
    return d


def _make_backend(monkeypatch=None) -> _AnthropicToolBackend:
    os.environ.setdefault("ANTHROPIC_API_KEY", "fake-key")
    cfg = {"kind": "anthropic", "model": "claude-sonnet-4-6", "api_key_env": "ANTHROPIC_API_KEY"}
    return _AnthropicToolBackend(cfg, max_tokens=512)


def test_stream_chat_translates_text_only():
    """A pure-text response yields text_delta + complete with empty tool_calls."""
    backend = _make_backend()
    captured_kwargs: dict[str, Any] = {}

    class _FakeStream:
        def __init__(self, events):
            self._events = events
        def __enter__(self): return iter(self._events)
        def __exit__(self, *a): return False

    events = [
        _make_event("message_start"),
        _make_event("content_block_start", index=0,
                    content_block=_make_content_block("text", text="")),
        _make_event("content_block_delta", index=0,
                    delta=_make_delta("text_delta", text="Hel")),
        _make_event("content_block_delta", index=0,
                    delta=_make_delta("text_delta", text="lo")),
        _make_event("content_block_stop", index=0),
        _make_event("message_stop"),
    ]
    with patch.object(backend._client.messages, "stream",
                      side_effect=lambda **kw: (captured_kwargs.update(kw), _FakeStream(events))[1]):
        out = list(backend.stream_chat(
            messages=[{"role": "user", "content": "Hi"}],
            tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
        ))

    # Outgoing wire shape was Anthropic-flavored
    assert "system" in captured_kwargs and captured_kwargs["model"] == "claude-sonnet-4-6"
    assert captured_kwargs["tools"][0]["name"] == "x"
    # Empty OpenAI parameters get expanded to the canonical "no-arg" object
    # schema Anthropic expects.
    assert captured_kwargs["tools"][0]["input_schema"] == {"type": "object", "properties": {}}
    assert captured_kwargs["tool_choice"] == {"type": "auto"}

    # Incoming events translated to OpenAI-shape internal events
    assert out[0] == {"type": "text_delta", "delta": "Hel"}
    assert out[1] == {"type": "text_delta", "delta": "lo"}
    assert out[-1] == {"type": "complete", "content": "Hello", "tool_calls": []}


def test_stream_chat_translates_tool_use():
    """A tool_use block yields tool_call_delta events and a complete event
    with OpenAI-shape tool_calls assembled."""
    backend = _make_backend()

    class _FakeStream:
        def __init__(self, events): self._events = events
        def __enter__(self): return iter(self._events)
        def __exit__(self, *a): return False

    events = [
        _make_event("message_start"),
        _make_event("content_block_start", index=0,
                    content_block=_make_content_block("tool_use",
                                                       id="toolu_abc",
                                                       name="run_sql")),
        _make_event("content_block_delta", index=0,
                    delta=_make_delta("input_json_delta",
                                       partial_json='{"s')),
        _make_event("content_block_delta", index=0,
                    delta=_make_delta("input_json_delta",
                                       partial_json='ql": "SELECT 1"}')),
        _make_event("content_block_stop", index=0),
        _make_event("message_stop"),
    ]
    with patch.object(backend._client.messages, "stream",
                      side_effect=lambda **kw: _FakeStream(events)):
        out = list(backend.stream_chat(
            messages=[{"role": "user", "content": "count"}],
            tools=[{"type": "function", "function": {
                "name": "run_sql", "description": "execute",
                "parameters": {"type": "object",
                               "properties": {"sql": {"type": "string"}}},
            }}],
        ))

    # First a header event (id + name, args=""), then partial deltas, then complete
    assert out[0]["type"] == "tool_call_delta"
    assert out[0]["id"] == "toolu_abc"
    assert out[0]["name"] == "run_sql"
    assert out[0]["arguments_delta"] == ""

    assert out[1] == {"type": "tool_call_delta", "index": 0,
                      "id": "toolu_abc", "name": "run_sql",
                      "arguments_delta": '{"s'}
    assert out[2] == {"type": "tool_call_delta", "index": 0,
                      "id": "toolu_abc", "name": "run_sql",
                      "arguments_delta": 'ql": "SELECT 1"}'}

    final = out[-1]
    assert final["type"] == "complete"
    assert final["content"] == ""
    assert len(final["tool_calls"]) == 1
    tc = final["tool_calls"][0]
    assert tc["id"] == "toolu_abc"
    assert tc["function"]["name"] == "run_sql"
    # OpenAI-shape: arguments is a JSON string
    assert json.loads(tc["function"]["arguments"]) == {"sql": "SELECT 1"}


def test_stream_chat_handles_text_and_tool_use_in_same_turn():
    """Anthropic can emit text + a tool_use in the same response. Both
    should be visible in the final complete event."""
    backend = _make_backend()

    class _FakeStream:
        def __init__(self, events): self._events = events
        def __enter__(self): return iter(self._events)
        def __exit__(self, *a): return False

    events = [
        _make_event("content_block_start", index=0,
                    content_block=_make_content_block("text", text="")),
        _make_event("content_block_delta", index=0,
                    delta=_make_delta("text_delta", text="Calling tool. ")),
        _make_event("content_block_stop", index=0),
        _make_event("content_block_start", index=1,
                    content_block=_make_content_block("tool_use",
                                                       id="toolu_1", name="x")),
        _make_event("content_block_delta", index=1,
                    delta=_make_delta("input_json_delta", partial_json='{}')),
        _make_event("content_block_stop", index=1),
        _make_event("message_stop"),
    ]
    with patch.object(backend._client.messages, "stream",
                      side_effect=lambda **kw: _FakeStream(events)):
        out = list(backend.stream_chat(
            messages=[{"role": "user", "content": "go"}],
            tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
        ))

    text_deltas = [e for e in out if e["type"] == "text_delta"]
    tool_deltas = [e for e in out if e["type"] == "tool_call_delta"]
    assert text_deltas == [{"type": "text_delta", "delta": "Calling tool. "}]
    assert len(tool_deltas) >= 1  # header + json delta(s)
    final = out[-1]
    assert final["content"] == "Calling tool. "
    assert len(final["tool_calls"]) == 1
    assert final["tool_calls"][0]["function"]["name"] == "x"


def test_stream_chat_maps_tool_choice_required_to_any():
    """OpenAI tool_choice='required' must map to Anthropic's 'any'."""
    backend = _make_backend()
    captured = {}

    class _FakeStream:
        def __init__(self, events): self._events = events
        def __enter__(self): return iter(self._events)
        def __exit__(self, *a): return False

    with patch.object(backend._client.messages, "stream",
                      side_effect=lambda **kw: (captured.update(kw), _FakeStream([]))[1]):
        list(backend.stream_chat(
            messages=[{"role": "user", "content": "x"}],
            tools=[{"type": "function", "function": {"name": "x", "parameters": {}}}],
            tool_choice="required",
        ))
    assert captured["tool_choice"] == {"type": "any"}
