"""Anthropic direct LLM provider."""

from __future__ import annotations

import json
from typing import Any, Iterator

from anthropic import Anthropic

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


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
        # Schema-constrained output: nudge the model with an explicit instruction
        # since Anthropic's tool-use surface is heavier than Azure's strict mode.
        if schema is not None:
            system = (
                (system + "\n\n" if system else "")
                + "Reply with JSON only matching this JSON-Schema:\n"
                + json.dumps(schema)
            )
        resp = self._client.messages.create(
            model=self._model,
            system=system,
            messages=chat,
            temperature=self._default_temperature if temperature is None else temperature,
            max_tokens=max_tokens or self._default_max_tokens,
        )
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
