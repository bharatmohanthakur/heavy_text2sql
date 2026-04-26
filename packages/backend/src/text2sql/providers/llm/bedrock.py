"""AWS Bedrock LLM provider — Anthropic Claude on Bedrock via API-key auth.

Bedrock now supports a long-lived API key in addition to SigV4. We use the
key path (`Authorization: Bearer <BEDROCK_API_KEY>`) because it works without
boto3, IAM roles, or STS. The wire format for Anthropic models on Bedrock is
the same Anthropic Messages payload (`anthropic_version`, `system`,
`messages`, `max_tokens`, `temperature`) — only the URL and the auth header
change.

This implements the canonical `LLMProvider` surface (`complete()`) used by
the `/query` pipeline. The `/chat` agent loop currently uses OpenAI's
tool-calling shape and so cannot drive a Bedrock model directly — that
needs a separate Bedrock-flavored agent client (Anthropic tool_use blocks).
"""

from __future__ import annotations

import json
from typing import Any, Iterator

import httpx

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm


# Step F will migrate to the Converse API and flip strict_json_schema=True
# (via toolUse strict mode) and token_streaming=True (via ConverseStream).
# anthropic_tool_use=True because Bedrock-Anthropic supports tool_use blocks
# natively, which the Step H translator will use as the agent backend.
_BEDROCK_CAPS = LLMCapabilities(
    strict_json_schema=False,
    token_streaming=False,
    openai_tool_calling=False,
    anthropic_tool_use=True,
)


class BedrockLLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._region: str = cfg.get("region", "us-west-2")
        self._api_key: str = _resolve_secret(cfg["api_key_env"])
        self._anthropic_version: str = cfg.get(
            "anthropic_version", "bedrock-2023-05-31"
        )
        self._default_max_tokens: int = int(cfg.get("max_tokens", 4096))
        self._default_temperature: float = float(cfg.get("temperature", 0.0))
        self._timeout_s: float = float(cfg.get("timeout_s", 120.0))
        self._client = httpx.Client(
            base_url=f"https://bedrock-runtime.{self._region}.amazonaws.com",
            timeout=self._timeout_s,
        )

    @property
    def model_id(self) -> str:
        return f"bedrock:{self._model}"

    @property
    def capabilities(self) -> LLMCapabilities:
        return _BEDROCK_CAPS

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        # Anthropic-on-Bedrock separates system prompts from the message list.
        system = "\n".join(m.content for m in messages if m.role == "system")
        chat = [
            {"role": m.role, "content": m.content}
            for m in messages if m.role in ("user", "assistant")
        ]
        # Schema-constrained output: same trick as the direct Anthropic
        # provider — nudge with an instruction in the system prompt rather
        # than the heavier tool_use route.
        if schema is not None:
            system = (
                (system + "\n\n" if system else "")
                + "Reply with JSON only matching this JSON-Schema:\n"
                + json.dumps(schema)
            )
        payload: dict[str, Any] = {
            "anthropic_version": self._anthropic_version,
            "max_tokens": max_tokens if max_tokens is not None else self._default_max_tokens,
            "temperature": temperature if temperature is not None else self._default_temperature,
            "messages": chat,
        }
        if system:
            payload["system"] = system

        resp = self._client.post(
            f"/model/{self._model}/invoke",
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
            },
            json=payload,
        )
        if resp.status_code >= 400:
            # Surface AWS's actual reason — the default httpx error swallows
            # the JSON body and just shows the URL, which is useless when
            # debugging access / region / model availability problems.
            raise RuntimeError(
                f"bedrock invoke {resp.status_code} (model={self._model}, "
                f"region={self._region}): {resp.text[:400]}"
            )
        body = resp.json()
        # Standard Anthropic payload: {"content": [{"type": "text", "text": "..."}], ...}
        parts = body.get("content") or []
        text = "".join(
            p.get("text", "") for p in parts if p.get("type") == "text"
        )
        return text

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]:
        # Bedrock supports server-sent streaming via /invoke-with-response-stream
        # but that's a different binary frame format (vnd.amazon.eventstream).
        # For now, fall back to a single-shot complete.
        yield self.complete(messages)


@register_llm("bedrock")
def _build(spec: ProviderEntry) -> BedrockLLM:
    return BedrockLLM(spec)
