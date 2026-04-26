"""Anthropic direct LLM provider."""

from __future__ import annotations

import json
from typing import Any, Iterator

from anthropic import Anthropic

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


# Anthropic supports server-enforced strict schema via tool_use blocks
# (per docs.anthropic.com — "Add `strict: true` to your tool definitions
# to ensure Claude's tool calls always match your schema exactly"). When
# the caller passes `schema=...`, we surface it as a single tool named
# `structured_response` with strict=true and tool_choice forcing it.
# anthropic_tool_use=True — the agent-loop translator (Step H) will
# dispatch via this flag.
_ANTHROPIC_CAPS = LLMCapabilities(
    strict_json_schema=True,
    token_streaming=True,
    openai_tool_calling=False,
    anthropic_tool_use=True,
)


_STRUCTURED_TOOL_NAME = "structured_response"


class AnthropicLLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._client = Anthropic(api_key=_resolve_secret(cfg["api_key_env"]))
        self._default_max_tokens: int = int(cfg.get("max_tokens", 4096))
        self._default_temperature: float = float(cfg.get("temperature", 0.0))

    @property
    def model_id(self) -> str:
        return f"anthropic:{self._model}"

    @property
    def capabilities(self) -> LLMCapabilities:
        return _ANTHROPIC_CAPS

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        # Anthropic separates system from the message list.
        system = "\n".join(m.content for m in messages if m.role == "system")
        chat = [
            {"role": m.role, "content": m.content}
            for m in messages if m.role in ("user", "assistant")
        ]
        kwargs: dict[str, Any] = {
            "model": self._model,
            "system": system,
            "messages": chat,
            "temperature": self._default_temperature if temperature is None else temperature,
            "max_tokens": max_tokens or self._default_max_tokens,
        }
        if schema is not None:
            # Server-enforced strict-schema via tool_use. Force the model to
            # emit exactly one tool call against `structured_response`; the
            # tool's input is guaranteed to match `input_schema` byte-for-byte
            # (with strict=true). Return the JSON-serialized input — callers
            # already json.loads(complete(...)) downstream.
            kwargs["tools"] = [{
                "name": _STRUCTURED_TOOL_NAME,
                "description": "Return the structured response.",
                "input_schema": schema,
                "strict": True,
            }]
            kwargs["tool_choice"] = {"type": "tool", "name": _STRUCTURED_TOOL_NAME}
        resp = self._client.messages.create(**kwargs)
        if schema is not None:
            for block in resp.content:
                if getattr(block, "type", None) == "tool_use" and block.name == _STRUCTURED_TOOL_NAME:
                    return json.dumps(block.input)
            # Fallback if the SDK or model behaved unexpectedly: surface text
            # so the caller's json.loads will raise a useful error.
            return "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        return "".join(block.text for block in resp.content if block.type == "text")

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]:
        system = "\n".join(m.content for m in messages if m.role == "system")
        chat = [
            {"role": m.role, "content": m.content}
            for m in messages if m.role in ("user", "assistant")
        ]
        with self._client.messages.stream(
            model=self._model,
            system=system,
            messages=chat,
            temperature=self._default_temperature,
            max_tokens=self._default_max_tokens,
        ) as s:
            for chunk in s.text_stream:
                yield chunk


@register_llm("anthropic")
def _build(spec: ProviderEntry) -> AnthropicLLM:
    return AnthropicLLM(spec)
