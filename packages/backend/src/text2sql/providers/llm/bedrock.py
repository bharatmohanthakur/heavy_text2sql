"""AWS Bedrock LLM provider — Converse API via long-lived API-key auth.

The Converse API is Bedrock's vendor-neutral message+tool+streaming surface
(https://docs.aws.amazon.com/bedrock/latest/userguide/conversation-inference.html).
We use it instead of the per-model `InvokeModel` path because:

  - One request shape works across Anthropic, Cohere, Mistral, Meta, etc.
  - Native tool use (`toolConfig`, `toolUse`, `toolResult`) — same wire
    contract regardless of the underlying model family.
  - Native streaming via `ConverseStream` — server-sent eventstream frames
    yield real per-token deltas (we parse the JSON inside each event).

Auth: long-lived Bedrock API key passed as `Authorization: Bearer <key>`.
Per AWS docs (https://docs.aws.amazon.com/bedrock/latest/userguide/api-keys.html):

  - API keys ARE accepted by Converse, ConverseStream, InvokeModel, and
    InvokeModelWithResponseStream.
  - **API keys are region-locked** — the key works only in the region
    where it was generated. Cross-region inference profiles like
    `global.anthropic.claude-opus-4-5` need a key generated in the
    profile's home region; otherwise calls return 403 "Authentication
    failed: Please make sure your API Key is valid." even when the key
    itself is valid.

Strict schema: we declare a single tool `structured_response` with the
caller's schema and `toolChoice: {tool: {name: "structured_response"}}` —
this is the AWS-documented way to enforce schema conformance and is
equivalent to Anthropic's strict tool_use upgrade in providers/llm/anthropic.py.

The `/chat` agent loop today only accepts OpenAI-shape providers; the
Step H translator will let it dispatch via `anthropic_tool_use=True` to
Bedrock-on-Anthropic (and OpenRouter, and direct Anthropic).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterator

import httpx

from text2sql.config import ProviderEntry
from text2sql.providers.base import LLMCapabilities, LLMMessage, LLMProvider
from text2sql.providers.factory import _resolve_secret, register_llm

log = logging.getLogger(__name__)


_BEDROCK_CAPS = LLMCapabilities(
    strict_json_schema=True,    # via Converse toolUse + toolChoice forcing
    token_streaming=True,       # via ConverseStream eventstream
    openai_tool_calling=False,  # Bedrock uses toolUse, not OpenAI tool_calls
    anthropic_tool_use=True,    # Step H translator can drive /chat via this
)


_STRUCTURED_TOOL_NAME = "structured_response"


class BedrockLLM(LLMProvider):
    def __init__(self, spec: ProviderEntry) -> None:
        cfg = spec.model_dump()
        self._model: str = cfg["model"]
        self._region: str = cfg.get("region", "us-west-2")
        self._api_key: str = _resolve_secret(cfg["api_key_env"])
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

    # ── Request building ─────────────────────────────────────────────────

    def _build_converse_payload(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None,
        temperature: float | None,
        max_tokens: int | None,
    ) -> dict[str, Any]:
        """Convert our LLMMessage list to Converse-API shape."""
        # System messages live in their own top-level field, not in messages.
        system_blocks = [
            {"text": m.content} for m in messages if m.role == "system"
        ]
        chat = [
            {"role": m.role, "content": [{"text": m.content}]}
            for m in messages if m.role in ("user", "assistant")
        ]
        payload: dict[str, Any] = {
            "messages": chat,
            "inferenceConfig": {
                "maxTokens": max_tokens if max_tokens is not None else self._default_max_tokens,
                "temperature": temperature if temperature is not None else self._default_temperature,
            },
        }
        if system_blocks:
            payload["system"] = system_blocks
        if schema is not None:
            # Strict-schema enforcement via tool_use. Converse's tool config
            # passes the JSON schema as `inputSchema.json` and forces the
            # tool via `toolChoice.tool.name`.
            payload["toolConfig"] = {
                "tools": [{
                    "toolSpec": {
                        "name": _STRUCTURED_TOOL_NAME,
                        "description": "Return the structured response.",
                        "inputSchema": {"json": schema},
                    },
                }],
                "toolChoice": {"tool": {"name": _STRUCTURED_TOOL_NAME}},
            }
        return payload

    # ── Sync complete ────────────────────────────────────────────────────

    def complete(
        self,
        messages: list[LLMMessage],
        *,
        schema: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        payload = self._build_converse_payload(
            messages, schema=schema, temperature=temperature, max_tokens=max_tokens,
        )
        resp = self._client.post(
            f"/model/{self._model}/converse",
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
            },
            json=payload,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"bedrock converse {resp.status_code} (model={self._model}, "
                f"region={self._region}): {resp.text[:400]}"
            )
        body = resp.json()
        # Converse response: {"output": {"message": {"role": "assistant",
        # "content": [{"text": "..."}|{"toolUse": {...}}]}}, ...}
        msg = (body.get("output") or {}).get("message") or {}
        blocks = msg.get("content") or []
        if schema is not None:
            for blk in blocks:
                tu = blk.get("toolUse")
                if tu and tu.get("name") == _STRUCTURED_TOOL_NAME:
                    return json.dumps(tu.get("input") or {})
            # Fallback: surface concatenated text so json.loads raises a
            # useful error for the caller to catch.
            return "".join(b.get("text", "") for b in blocks if "text" in b)
        return "".join(b.get("text", "") for b in blocks if "text" in b)

    # ── Streaming ────────────────────────────────────────────────────────

    def stream(self, messages: list[LLMMessage]) -> Iterator[str]:
        """Yield text deltas from ConverseStream.

        Converse streams as AWS eventstream binary frames (each frame has a
        prelude + headers + JSON payload + CRC). httpx + the documented
        SSE-style decoding doesn't fit cleanly. AWS SDK v3 / boto3 parse
        these for us, but we want zero AWS-SDK dependency for the API-key
        path. Instead we use the documented HTTP-level fallback: each
        eventstream chunk in the body contains a JSON payload after a
        framing header — we scan for the `{` start of each JSON object,
        parse, and emit any `delta.text` we find inside
        `contentBlockDelta` events.

        For models that don't accept text streaming (or for tool_use
        streams we don't currently surface), we degrade to a one-shot
        complete().
        """
        payload = self._build_converse_payload(
            messages, schema=None, temperature=None, max_tokens=None,
        )
        try:
            with self._client.stream(
                "POST",
                f"/model/{self._model}/converse-stream",
                headers={
                    "content-type": "application/json",
                    "accept": "application/vnd.amazon.eventstream",
                    "Authorization": f"Bearer {self._api_key}",
                },
                json=payload,
            ) as resp:
                if resp.status_code >= 400:
                    body = resp.read().decode(errors="replace")
                    raise RuntimeError(
                        f"bedrock converse-stream {resp.status_code} "
                        f"(model={self._model}, region={self._region}): {body[:400]}"
                    )
                buffer = b""
                for chunk in resp.iter_bytes():
                    if not chunk:
                        continue
                    buffer += chunk
                    # Scan for embedded JSON objects. Each Converse event
                    # frame has a JSON body bracketed somewhere in the
                    # eventstream wrapper — the delimiter we care about is
                    # a balanced `{...}` starting with one of the known
                    # event-shape keys. We extract every complete object.
                    for obj in _extract_json_objects(buffer):
                        delta = (obj.get("delta") or {}).get("text")
                        if isinstance(delta, str) and delta:
                            yield delta
                    # Keep only the trailing partial bytes for the next iter
                    last = buffer.rfind(b"}")
                    buffer = buffer[last + 1 :] if last >= 0 else buffer
        except RuntimeError:
            raise
        except Exception as e:
            log.warning(
                "bedrock streaming failed (%s); falling back to one-shot complete()", e
            )
            yield self.complete(messages)


def _extract_json_objects(buf: bytes) -> Iterator[dict[str, Any]]:
    """Scan a bytes buffer for complete top-level JSON objects.

    The eventstream framing wraps each JSON payload with a binary header.
    Rather than parse the eventstream protocol, we walk the bytes and find
    every `{...}` that decodes cleanly. False positives (truncated objects)
    are filtered by JSONDecodeError. Order is preserved.
    """
    n = len(buf)
    i = 0
    while i < n:
        if buf[i:i + 1] != b"{":
            i += 1
            continue
        depth = 0
        in_str = False
        esc = False
        j = i
        while j < n:
            c = buf[j:j + 1]
            if esc:
                esc = False
            elif c == b"\\":
                esc = True
            elif c == b'"':
                in_str = not in_str
            elif not in_str:
                if c == b"{":
                    depth += 1
                elif c == b"}":
                    depth -= 1
                    if depth == 0:
                        try:
                            yield json.loads(buf[i:j + 1])
                        except Exception:
                            pass
                        i = j + 1
                        break
            j += 1
        else:
            return  # incomplete object; wait for more bytes
        if j >= n:
            return


@register_llm("bedrock")
def _build(spec: ProviderEntry) -> BedrockLLM:
    return BedrockLLM(spec)
