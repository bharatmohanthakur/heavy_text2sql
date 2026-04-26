"""OpenAI direct LLM provider."""

from __future__ import annotations

from typing import Any, Iterator

from openai import OpenAI

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


_OPENAI_CAPS = LLMCapabilities(
    strict_json_schema=True,
    token_streaming=True,
    openai_tool_calling=True,
    anthropic_tool_use=False,
)


class OpenAILLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._client = OpenAI(api_key=_resolve_secret(cfg["api_key_env"]))
        self._default_max_tokens: int = int(cfg.get("max_tokens", 4096))
        self._default_temperature: float = float(cfg.get("temperature", 0.0))

    @property
    def model_id(self) -> str:
        return f"openai:{self._model}"

    @property
    def capabilities(self) -> LLMCapabilities:
        return _OPENAI_CAPS

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": self._default_temperature if temperature is None else temperature,
            "max_tokens": max_tokens or self._default_max_tokens,
        }
        if schema is not None:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "structured", "schema": schema, "strict": True},
            }
        if tools is not None:
            kwargs["tools"] = tools
            if schema is not None:
                # OpenAI docs: structured outputs unsupported with parallel
                # function calls. Defensive default.
                kwargs["parallel_tool_calls"] = False
        resp = self._client.chat.completions.create(**kwargs)
        return resp.choices[0].message.content or ""

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


@register_llm("openai")
def _build(spec: ProviderEntry) -> OpenAILLM:
    return OpenAILLM(spec)
