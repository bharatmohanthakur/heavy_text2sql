"""OpenRouter LLM provider — OpenAI-API-compatible router across many models.

Lets us point the same factory at z-ai/glm-5.1, anthropic/claude-sonnet-4.6,
deepseek/deepseek-v3.2, qwen/qwen3.5-*, etc. without writing a new provider per
model. Uses the OpenAI SDK with base_url=https://openrouter.ai/api/v1.

Not every OpenRouter model honors `response_format: {type: "json_schema", strict}`.
For models that don't, we degrade to "Reply with JSON only matching: <schema>"
in the system prompt — same fallback the Anthropic provider uses.
"""

from __future__ import annotations

import json
from typing import Any, Iterator

from openai import OpenAI

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


_DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"


# Models known NOT to support response_format=json_schema strict on
# OpenRouter today (best-effort allow-list of those that DO).
_STRICT_JSON_SAFE = frozenset({
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "openai/gpt-4.1",
    "openai/gpt-4.1-mini",
})


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

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        chat = [{"role": m.role, "content": m.content} for m in messages]

        if schema is not None and self._model not in _STRICT_JSON_SAFE:
            # Fold the schema into the system prompt for models that don't
            # support strict json_schema response_format.
            instr = (
                "Reply with JSON only conforming to this JSON-Schema:\n"
                + json.dumps(schema)
            )
            if chat and chat[0]["role"] == "system":
                chat[0]["content"] = chat[0]["content"] + "\n\n" + instr
            else:
                chat.insert(0, {"role": "system", "content": instr})
            schema_for_api = None
        else:
            schema_for_api = schema

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
        resp = self._client.chat.completions.create(**kwargs)
        text = resp.choices[0].message.content or ""
        # Some models wrap JSON in code fences when we asked via prompt.
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
