"""Azure OpenAI LLM provider — wraps gpt-4o (or any deployment) for chat completion."""

from __future__ import annotations

from typing import Any, Iterator

from openai import AzureOpenAI

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


class AzureOpenAILLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._deployment: str = cfg["deployment"]
        self._client = AzureOpenAI(
            azure_endpoint=cfg["endpoint"],
            api_key=_resolve_secret(cfg["api_key_env"]),
            api_version=cfg["api_version"],
        )
        self._default_max_tokens: int = int(cfg.get("max_tokens", 4096))
        self._default_temperature: float = float(cfg.get("temperature", 0.0))

    @property
    def model_id(self) -> str:
        return f"azure:{self._deployment}"

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {
            "model": self._deployment,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": self._default_temperature if temperature is None else temperature,
            "max_tokens": max_tokens or self._default_max_tokens,
        }
        if schema is not None:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "structured", "schema": schema, "strict": True},
            }
        resp = self._client.chat.completions.create(**kwargs)
        return resp.choices[0].message.content or ""

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]:
        stream = self._client.chat.completions.create(
            model=self._deployment,
            messages=[{"role": m.role, "content": m.content} for m in messages],
            temperature=self._default_temperature,
            max_tokens=self._default_max_tokens,
            stream=True,
        )
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


@register_llm("azure_openai")
def _build(spec: ProviderEntry) -> AzureOpenAILLM:
    return AzureOpenAILLM(spec)
