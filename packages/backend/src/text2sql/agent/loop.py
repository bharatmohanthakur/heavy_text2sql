"""Agent loop — drives an LLM through tool calls until it emits final_answer.

The loop is intentionally simple:
  1. Load conversation history from store.
  2. Append the new user message.
  3. Send the chat (system prompt + history + tools) to the LLM.
  4. If LLM returns tool_calls: persist assistant turn, execute each tool,
     persist tool result message(s), loop.
  5. If LLM emits the `final_answer` tool: persist final assistant turn,
     terminate, return AgentResult.
  6. Hard cap at `max_steps` tool calls per user turn so a misbehaving
     model can't run forever.

Two entry points:
  AgentRunner.run(conv_id, user_msg) -> AgentResult        (sync)
  AgentRunner.run_stream(conv_id, user_msg) -> Iterator    (yields events)

Streaming events are JSON-serializable dicts with a `kind` field. Frontends
render them as a live transcript.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterator

from openai import AzureOpenAI, OpenAI

from text2sql.agent.conversation_store import ConversationStore
from text2sql.agent.tools import ToolContext, ToolRegistry, ToolResult, default_registry
from text2sql.config import ProviderEntry

log = logging.getLogger(__name__)


_DEFAULT_SYSTEM_PROMPT = """\
You are a database analyst agent for an Ed-Fi ODS. Answer the user's question
by calling the available tools, then call final_answer to terminate.

ABSOLUTE RULE — NEVER VIOLATE:
  Every factual claim about the data (any count, sum, list, or specific row)
  MUST come from a successful run_sql result in THIS conversation. You do
  NOT know the data. You do NOT remember row counts. If a number isn't in a
  tool result you produced this turn, you have no way to know it. Hallucinated
  numbers are a critical failure. Even if you "know" the answer, you must
  still execute SQL to get it.

  - You may NEVER call final_answer with a numeric or list answer unless a
    prior run_sql in this conversation succeeded and produced that answer.
  - The only exception is meta-questions ("what tools do you have?",
    "what does the schema look like?") that do not require data.

MANDATORY workflow for any data question — do these in order, do not skip:
  1. classify_domains  — get the routed Ed-Fi domains.
  2. find_similar_queries  (MANDATORY)  — retrieve the top-3 approved gold
     SQL examples. They encode the exact dialect, casing, table choices,
     and join shape that work against THIS database. The `tables_used`
     field on each example is your shortlist — use those tables directly.
     DO NOT replace them with whatever search_tables returns. Skipping
     this step OR ignoring its output is a critical failure.
  3. resolve_entity  — for any noun mapping to a value in the DB (Hispanic,
     Pre-K, Algebra I, "9th Grade", etc.). Descriptors return the bridge
     join chain. Always call this for descriptor-coded values BEFORE
     writing the WHERE clause.
  4. search_tables (k=8 minimum)  — pass the user's question VERBATIM as
     the `query`. Do NOT paraphrase or extract keywords — the retriever
     uses the literal phrasing for hybrid (vector+BM25) ranking, and a
     paraphrase loses signal. Even if find_similar_queries already gave
     you a candidate set, run search_tables to get the auto-computed
     Steiner `join_tree`. The `join_tree.tree_nodes` is your final table set; the
     `join_tree.join_clauses` are your JOINs. Both are produced by the
     same FK-graph + inheritance walk the canonical pipeline uses, so
     trust them — they include bridge tables (like
     StudentEducationOrganizationAssociation, EducationOrganization,
     Descriptor) that text-similarity alone wouldn't surface.
  5. inspect_table  — for each table in `join_tree.tree_nodes`, fetch
     real columns to choose the right WHERE/SELECT columns.
  6. find_join_path  — only call this if you need a join tree for a
     DIFFERENT subset of tables than search_tables already returned.
  7. run_sql  — compose and execute. If it errors, FIX and call run_sql
     AGAIN, up to 3 attempts. Reasons SQL fails to validate include:
       - column doesn't exist on that table (re-inspect_table)
       - join column wrong (use find_join_path's exact clauses)
       - missing bracket quoting on a reserved word
     NEVER give up and call final_answer with sql=null for a data question
     after a single SQL failure — repair and retry.
  8. final_answer  — pass the SUCCESSFUL SQL and row_count verbatim.

When you cannot find an obvious path:
  - Re-read the find_similar_queries output. The closest example's
    `tables_used` list is almost certainly your answer.
  - Try resolve_entity on the key noun (e.g. "grade level" → returns
    GradeLevelDescriptor and the bridge table).
  - **Re-run `search_tables` with k=15 and a different phrasing** —
    the first try may have missed the right table. Common Ed-Fi
    "demographic-ish" data lives on `StudentEducationOrganizationAssociation`
    (race, ethnicity, sex, language) NOT on `StudentDemographic`.
  - Inspect the candidate's columns with inspect_table BEFORE giving up —
    a hit you skipped might have exactly the column you need.
  - Only AFTER all the above fail may you call final_answer with sql=null
    explaining what's actually missing.

Ed-Fi joining rules (this database uses Ed-Fi inheritance):
  - School / LocalEducationAgency / StateEducationAgency / PostSecondaryInstitution
    INHERIT from EducationOrganization. The human-readable name lives on
    EducationOrganization.NameOfInstitution. To return school/LEA names you
    MUST join through EducationOrganization on the matching id (SchoolId =
    EducationOrganizationId for schools).
  - Student names live on edfi.Student (FirstName, LastSurname).
  - When the user mentions a descriptor value (Hispanic, "9th Grade",
    "Pre-K"), the predicate goes on edfi.Descriptor.CodeValue, joined
    through the child descriptor table that resolve_entity returns.
  - Enrollment year is edfi.StudentSchoolAssociation.SchoolYear (an int
    like 2023). DO NOT compute YEAR(EntryDate) for that — they are not the
    same thing.

Hard rules:
  - Target dialect is MSSQL T-SQL. Use [bracket] quoting and TOP N (not LIMIT).
  - Use ONLY columns that appeared in inspect_table's output. If a column
    isn't there, it doesn't exist.
  - When a list is requested, cap at exactly 50 rows (SELECT TOP 50 ...).
  - When the question is an aggregate (count/sum/avg), don't add TOP.
  - You MUST call final_answer exactly once, and only AFTER a successful
    run_sql.
"""


# ── Public types ─────────────────────────────────────────────────────────────


@dataclass
class AgentStep:
    kind: str                                  # "tool_call" | "tool_result" | "assistant" | "error"
    name: str | None = None                    # tool name (if applicable)
    arguments: dict[str, Any] | None = None
    tool_call_id: str | None = None
    result: dict[str, Any] | None = None       # decoded tool result data
    error: str | None = None
    content: str = ""                          # assistant free text (rare; usually empty)
    elapsed_ms: float | None = None


@dataclass
class AgentResult:
    conversation_id: uuid.UUID
    final_summary: str
    final_sql: str | None
    final_row_count: int | None
    steps: list[AgentStep] = field(default_factory=list)
    aborted: bool = False
    abort_reason: str | None = None
    total_ms: float = 0.0
    # Post-processing artifacts: result rows, chart spec, NL description.
    # These run AFTER the agent terminates — never as LLM-callable tools —
    # so they always fire regardless of what the agent did.
    final_rows: list[dict[str, Any]] = field(default_factory=list)
    viz: dict[str, Any] | None = None
    vega_lite: dict[str, Any] | None = None
    description: str = ""


# ── LLM client adapter ───────────────────────────────────────────────────────


class _AssembledMessage:
    """Mutable accumulator that mirrors a chat-completion .message after a
    streamed run. Holds the full assistant text and a list of tool_calls
    where each tool_call carries id/name/arguments-as-string-so-far."""

    def __init__(self) -> None:
        self.content: str = ""
        # tool_calls indexed by the LLM's own `index` field (a small int).
        self._tcs: dict[int, dict[str, Any]] = {}

    def apply_delta(self, delta: Any) -> dict[str, Any] | None:
        """Apply one streaming delta. Returns a JSON-serializable event dict
        describing what changed (for SSE), or None if nothing user-visible."""
        ev: dict[str, Any] | None = None
        text_delta = getattr(delta, "content", None) or ""
        if text_delta:
            self.content += text_delta
            ev = {"type": "text_delta", "delta": text_delta}
        tcs = getattr(delta, "tool_calls", None) or []
        for tc in tcs:
            idx = getattr(tc, "index", 0)
            cur = self._tcs.setdefault(idx, {"id": "", "name": "", "arguments": ""})
            if getattr(tc, "id", None):
                cur["id"] = tc.id
            fn = getattr(tc, "function", None)
            if fn is not None:
                if getattr(fn, "name", None):
                    cur["name"] = fn.name
                args_delta = getattr(fn, "arguments", None) or ""
                if args_delta:
                    cur["arguments"] += args_delta
                    ev = {
                        "type": "tool_call_delta",
                        "index": idx,
                        "id": cur["id"],
                        "name": cur["name"],
                        "arguments_delta": args_delta,
                    }
        return ev

    @property
    def tool_calls(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for idx in sorted(self._tcs):
            tc = self._tcs[idx]
            if not tc["name"]:
                continue
            out.append({
                "id": tc["id"] or f"call_{idx}",
                "type": "function",
                "function": {
                    "name": tc["name"],
                    "arguments": tc["arguments"] or "{}",
                },
            })
        return out


class _LLMClient:
    """Capability-dispatching streaming chat client.

    The agent loop assumes one wire shape internally — OpenAI-style
    tool_calls. To support Anthropic-style tool_use providers, we keep
    the loop's internal event shape unchanged and translate at the
    boundary: outgoing OpenAI tools/messages → vendor-native; incoming
    vendor-native deltas → OpenAI-shape `text_delta` / `tool_call_delta`.

    Backends:
      - kind in {azure_openai, openai}   → `_OpenAIToolBackend` (passthrough)
      - kind in {anthropic}              → `_AnthropicToolBackend` (translator)

    Bedrock-Anthropic via Converse uses the same tool_use semantics as
    direct Anthropic; a `_BedrockAnthropicToolBackend` adapter is a
    natural follow-on (capability flag `anthropic_tool_use=True` already
    declared on the bedrock provider).
    """

    def __init__(self, spec: ProviderEntry, *, max_tokens: int = 1500) -> None:
        cfg = spec.model_dump()
        kind = cfg["kind"]
        self._max_tokens = max_tokens
        if kind in ("azure_openai", "openai"):
            self._backend: _ToolBackend = _OpenAIToolBackend(cfg, max_tokens=max_tokens)
        elif kind == "anthropic":
            self._backend = _AnthropicToolBackend(cfg, max_tokens=max_tokens)
        else:
            raise ValueError(
                f"The /chat agent loop requires an LLM with either "
                f"openai_tool_calling or anthropic_tool_use capability. "
                f"The provider configured for `llm.task_routing.sql_generation` "
                f"is kind={kind!r}, which has neither.\n\n"
                f"Either:\n"
                f"  1. Edit configs/default.yaml — set "
                f"llm.task_routing.sql_generation to a provider with one of "
                f"the supported kinds: 'azure_openai', 'openai', 'anthropic'.\n"
                f"  2. Use /query (the canonical pipeline) — it works with "
                f"all 5 LLM providers."
            )

    def stream_chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        tool_choice: str | dict[str, Any] = "auto",
        temperature: float = 0.0,
    ) -> Iterator[dict[str, Any]]:
        """Yield internal events (unified across backends):
          {"type": "text_delta", "delta": "..."}
          {"type": "tool_call_delta", "index": N, "name": "...", "arguments_delta": "..."}
          {"type": "complete", "content": "...", "tool_calls": [...]}
        """
        yield from self._backend.stream_chat(
            messages, tools, tool_choice=tool_choice, temperature=temperature,
        )


# ── Backend protocol ─────────────────────────────────────────────────────────


class _ToolBackend:
    """Protocol — each backend is a streaming chat client whose tool-call
    wire shape gets translated to/from the loop's internal OpenAI-style
    event shape."""

    def stream_chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        tool_choice: str | dict[str, Any] = "auto",
        temperature: float = 0.0,
    ) -> Iterator[dict[str, Any]]:
        raise NotImplementedError


# ── OpenAI / Azure OpenAI backend (passthrough) ──────────────────────────────


class _OpenAIToolBackend(_ToolBackend):
    """Native OpenAI-shape tool calling. Internal event shape == wire shape,
    so this is a thin streaming pass-through."""

    def __init__(self, cfg: dict[str, Any], *, max_tokens: int) -> None:
        self._max_tokens = max_tokens
        if cfg["kind"] == "azure_openai":
            self._client = AzureOpenAI(
                azure_endpoint=cfg["endpoint"],
                api_key=os.environ[cfg["api_key_env"]],
                api_version=cfg["api_version"],
            )
            self._model = cfg["deployment"]
        else:
            self._client = OpenAI(api_key=os.environ[cfg["api_key_env"]])
            self._model = cfg["model"]

    def stream_chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        tool_choice: str | dict[str, Any] = "auto",
        temperature: float = 0.0,
    ) -> Iterator[dict[str, Any]]:
        stream = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
            temperature=temperature,
            max_tokens=self._max_tokens,
            stream=True,
        )
        acc = _AssembledMessage()
        for chunk in stream:
            choices = chunk.choices or []
            if not choices:
                continue
            ev = acc.apply_delta(choices[0].delta)
            if ev is not None:
                yield ev
        yield {
            "type": "complete",
            "content": acc.content,
            "tool_calls": acc.tool_calls,
        }


# ── Anthropic backend (translator) ───────────────────────────────────────────


class _AnthropicToolBackend(_ToolBackend):
    """Translates the loop's OpenAI-shape interface to Anthropic's Messages
    API:

      - Outgoing tools=[{type:"function", function:{name, parameters}}]
        → tools=[{name, description, input_schema}]
      - Outgoing message history with system / tool / assistant-with-tool_calls
        roles → Anthropic's `system=` field + alternating user/assistant turns
        with `tool_use` and `tool_result` content blocks
      - Incoming SSE events (message_start, content_block_start/delta/stop,
        message_stop) → loop's `text_delta` / `tool_call_delta` / `complete`

    Tool-use input streams as JSON-fragment deltas (`input_json_delta`); we
    forward each fragment as the loop's `tool_call_delta.arguments_delta`
    so the browser sees JSON arguments build up token-by-token, exactly
    like OpenAI.
    """

    def __init__(self, cfg: dict[str, Any], *, max_tokens: int) -> None:
        # Lazy import so the dependency is not required for OpenAI users.
        from anthropic import Anthropic
        self._client = Anthropic(api_key=os.environ[cfg["api_key_env"]])
        self._model = cfg["model"]
        self._max_tokens = max_tokens

    # ── Outgoing translation ────────────────────────────────────────────────

    @staticmethod
    def _openai_tools_to_anthropic(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for t in tools:
            fn = t.get("function") or {}
            out.append({
                "name": fn.get("name") or t.get("name") or "tool",
                "description": fn.get("description") or t.get("description") or "",
                "input_schema": fn.get("parameters") or t.get("parameters") or {
                    "type": "object", "properties": {}
                },
            })
        return out

    @staticmethod
    def _openai_messages_to_anthropic(
        messages: list[dict[str, Any]],
    ) -> tuple[str, list[dict[str, Any]]]:
        """Returns (system_prompt, anthropic_messages).

        Conversion rules:
          - role=system → concatenated into the system prompt.
          - role=user → user message with single text block.
          - role=assistant with `tool_calls` → assistant message with
            `tool_use` content blocks (one per tool call).
          - role=assistant without tool_calls → assistant message with text.
          - role=tool → user message with `tool_result` content block keyed
            on the originating tool_call_id (Anthropic conflates tool
            results into the user turn).
        """
        system_chunks: list[str] = []
        out: list[dict[str, Any]] = []

        for m in messages:
            role = m.get("role")
            if role == "system":
                system_chunks.append(m.get("content") or "")
                continue
            if role == "user":
                out.append({"role": "user", "content": [{"type": "text", "text": m.get("content") or ""}]})
                continue
            if role == "assistant":
                blocks: list[dict[str, Any]] = []
                text = m.get("content") or ""
                if text:
                    blocks.append({"type": "text", "text": text})
                for tc in m.get("tool_calls") or []:
                    fn = tc.get("function") or {}
                    raw_args = fn.get("arguments") or "{}"
                    try:
                        parsed = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                    except Exception:
                        parsed = {}
                    blocks.append({
                        "type": "tool_use",
                        "id": tc.get("id") or "",
                        "name": fn.get("name") or "",
                        "input": parsed,
                    })
                out.append({"role": "assistant", "content": blocks})
                continue
            if role == "tool":
                out.append({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": m.get("tool_call_id") or "",
                        "content": m.get("content") or "",
                    }],
                })
                continue
        return "\n\n".join(system_chunks), out

    # ── Incoming streaming ──────────────────────────────────────────────────

    def stream_chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        tool_choice: str | dict[str, Any] = "auto",
        temperature: float = 0.0,
    ) -> Iterator[dict[str, Any]]:
        anthropic_system, anthropic_messages = self._openai_messages_to_anthropic(messages)
        anthropic_tools = self._openai_tools_to_anthropic(tools)

        kwargs: dict[str, Any] = {
            "model": self._model,
            "system": anthropic_system,
            "messages": anthropic_messages,
            "tools": anthropic_tools,
            "max_tokens": self._max_tokens,
            "temperature": temperature,
        }
        # Translate tool_choice — Anthropic uses {"type": "auto"|"any"|"tool", "name": ?}
        if isinstance(tool_choice, str):
            if tool_choice == "auto":
                kwargs["tool_choice"] = {"type": "auto"}
            elif tool_choice == "required" or tool_choice == "any":
                kwargs["tool_choice"] = {"type": "any"}
            elif tool_choice == "none":
                # Anthropic Messages API doesn't have a "none" tool_choice;
                # the workaround is to omit `tools` entirely, but the loop
                # always passes them. Default to auto.
                kwargs["tool_choice"] = {"type": "auto"}
        elif isinstance(tool_choice, dict):
            # Pass through {"type": "tool", "name": "..."} shape unchanged
            kwargs["tool_choice"] = tool_choice

        # Anthropic streaming: track which tool_use index each delta belongs
        # to so we can map content_block_index → our own zero-based tool index.
        text_acc = ""
        # tool_use index assigned in order of arrival
        tool_calls: list[dict[str, Any]] = []
        block_to_tool_idx: dict[int, int] = {}

        with self._client.messages.stream(**kwargs) as stream:
            for ev in stream:
                ev_type = getattr(ev, "type", None)
                if ev_type == "content_block_start":
                    blk = getattr(ev, "content_block", None)
                    blk_idx = getattr(ev, "index", 0)
                    if blk is not None and getattr(blk, "type", None) == "tool_use":
                        my_idx = len(tool_calls)
                        tool_calls.append({
                            "id": getattr(blk, "id", "") or f"call_{my_idx}",
                            "name": getattr(blk, "name", "") or "",
                            "arguments": "",
                        })
                        block_to_tool_idx[blk_idx] = my_idx
                        # Emit a tool_call_delta with empty args so the UI
                        # can render the tool name immediately (matches the
                        # OpenAI flow where the first delta carries id+name).
                        yield {
                            "type": "tool_call_delta",
                            "index": my_idx,
                            "id": tool_calls[my_idx]["id"],
                            "name": tool_calls[my_idx]["name"],
                            "arguments_delta": "",
                        }
                elif ev_type == "content_block_delta":
                    delta = getattr(ev, "delta", None)
                    if delta is None:
                        continue
                    blk_idx = getattr(ev, "index", 0)
                    delta_type = getattr(delta, "type", None)
                    if delta_type == "text_delta":
                        text = getattr(delta, "text", "") or ""
                        if text:
                            text_acc += text
                            yield {"type": "text_delta", "delta": text}
                    elif delta_type == "input_json_delta":
                        partial = getattr(delta, "partial_json", "") or ""
                        if not partial:
                            continue
                        my_idx = block_to_tool_idx.get(blk_idx)
                        if my_idx is None:
                            continue
                        tool_calls[my_idx]["arguments"] += partial
                        yield {
                            "type": "tool_call_delta",
                            "index": my_idx,
                            "id": tool_calls[my_idx]["id"],
                            "name": tool_calls[my_idx]["name"],
                            "arguments_delta": partial,
                        }
                # message_start / content_block_stop / message_delta /
                # message_stop are not user-visible; we ignore them.

        # Emit the same `complete` event the OpenAI backend produces so the
        # outer loop can persist + execute tools without caring which
        # backend ran.
        yield {
            "type": "complete",
            "content": text_acc,
            "tool_calls": [
                {
                    "id": tc["id"],
                    "type": "function",
                    "function": {
                        "name": tc["name"],
                        "arguments": tc["arguments"] or "{}",
                    },
                }
                for tc in tool_calls
                if tc["name"]
            ],
        }


# ── Runner ───────────────────────────────────────────────────────────────────


class AgentRunner:
    def __init__(
        self,
        *,
        conv_store: ConversationStore,
        tool_ctx: ToolContext,
        llm_spec: ProviderEntry,
        registry: ToolRegistry | None = None,
        system_prompt: str | None = None,
        max_steps: int = 12,
        max_completion_tokens: int = 1500,
    ) -> None:
        self.conv_store = conv_store
        self.tool_ctx = tool_ctx
        self.registry = registry or default_registry()
        self.system_prompt = system_prompt or _DEFAULT_SYSTEM_PROMPT
        self.max_steps = max_steps
        self._llm = _LLMClient(llm_spec, max_tokens=max_completion_tokens)

    # ── Sync entry point ─────────────────────────────────────────────────────

    def run(
        self,
        conversation_id: uuid.UUID | None,
        user_message: str,
    ) -> AgentResult:
        events: list[AgentStep] = []
        result: AgentResult | None = None
        for ev in self._iterate(conversation_id, user_message):
            kind = ev.get("kind")
            if kind == "step":
                events.append(ev["step"])
            elif kind == "result":
                result = ev["result"]
                result.steps = events
            # text_delta / tool_call_delta / conversation_id are streaming-only;
            # the sync caller only sees the assembled tool_call/tool_result steps.
        assert result is not None, "agent loop terminated without a result"
        return result

    # ── Streaming entry point ────────────────────────────────────────────────

    def run_stream(
        self,
        conversation_id: uuid.UUID | None,
        user_message: str,
    ) -> Iterator[dict[str, Any]]:
        """Yield JSON-serializable events suitable for SSE.

        Event kinds:
          {"kind": "conversation_id", "id": "<uuid>"}
          {"kind": "step", "step": <AgentStep dict>}
          {"kind": "result", "result": <AgentResult dict>}
        """
        for ev in self._iterate(conversation_id, user_message):
            yield self._serialize_event(ev)

    # ── Internal generator ───────────────────────────────────────────────────

    def _iterate(
        self,
        conversation_id: uuid.UUID | None,
        user_message: str,
    ) -> Iterator[dict[str, Any]]:
        t_start = time.perf_counter()
        # 1. Establish conversation
        if conversation_id is None:
            conv = self.conv_store.create_conversation(title=user_message[:80])
            conversation_id = conv.id
        else:
            if self.conv_store.get_conversation(conversation_id) is None:
                conv = self.conv_store.create_conversation(title=user_message[:80])
                conversation_id = conv.id
        yield {"kind": "conversation_id", "id": conversation_id}

        # 2. Persist user message
        self.conv_store.append_message(
            conversation_id, role="user", content=user_message,
        )

        # 3. Build chat messages: system + persisted history
        history = self.conv_store.history(conversation_id)
        chat_messages: list[dict[str, Any]] = [
            {"role": "system", "content": self.system_prompt}
        ] + [m.to_chat_message() for m in history]

        tools_spec = self.registry.to_openai_tools()
        steps_taken = 0
        terminal_payload: dict[str, Any] | None = None
        aborted = False
        abort_reason: str | None = None

        while steps_taken < self.max_steps:
            # 4. Stream the next LLM turn token-by-token, forwarding deltas
            #    out as SSE events as they arrive. _iterate is a generator,
            #    so each yield reaches the wire immediately.
            assistant_text = ""
            persisted_calls: list[dict[str, Any]] = []
            try:
                for ev in self._llm.stream_chat(
                    chat_messages, tools_spec, tool_choice="auto",
                ):
                    if ev["type"] == "text_delta":
                        yield {"kind": "text_delta", "delta": ev["delta"]}
                    elif ev["type"] == "tool_call_delta":
                        yield {
                            "kind": "tool_call_delta",
                            "index": ev["index"],
                            "id": ev.get("id") or "",
                            "name": ev.get("name") or "",
                            "arguments_delta": ev["arguments_delta"],
                        }
                    elif ev["type"] == "complete":
                        assistant_text = ev["content"]
                        persisted_calls = ev["tool_calls"]
            except Exception as e:
                log.exception("LLM call failed")
                aborted = True
                abort_reason = f"LLM error: {e}"
                break

            # 5a. No tool calls — model may have answered in text
            if not persisted_calls:
                self.conv_store.append_message(
                    conversation_id, role="assistant", content=assistant_text,
                )
                step = AgentStep(kind="assistant", content=assistant_text)
                yield {"kind": "step", "step": step}
                terminal_payload = {
                    "summary": assistant_text or "(no response)",
                    "sql": None, "row_count": None,
                }
                break

            # 5b. Persist the assistant turn that requested tool calls
            self.conv_store.append_message(
                conversation_id,
                role="assistant",
                content=assistant_text,
                tool_calls=persisted_calls,
            )
            chat_messages.append({
                "role": "assistant",
                "content": assistant_text,
                "tool_calls": persisted_calls,
            })

            # 6. Execute each tool call sequentially
            for tc in persisted_calls:
                steps_taken += 1
                t0 = time.perf_counter()
                name = tc["function"]["name"]
                args_raw = tc["function"]["arguments"] or "{}"
                tc_id = tc["id"]
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                except Exception:
                    args = {}
                yield {
                    "kind": "step",
                    "step": AgentStep(
                        kind="tool_call",
                        name=name,
                        arguments=args,
                        tool_call_id=tc_id,
                    ),
                }
                tool_result: ToolResult = self.registry.execute(name, args, self.tool_ctx)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                tool_payload = tool_result.to_json()

                self.conv_store.append_message(
                    conversation_id,
                    role="tool",
                    content=tool_payload,
                    tool_call_id=tc_id,
                    tool_name=name,
                )
                chat_messages.append({
                    "role": "tool",
                    "content": tool_payload,
                    "tool_call_id": tc_id,
                })
                yield {
                    "kind": "step",
                    "step": AgentStep(
                        kind="tool_result",
                        name=name,
                        tool_call_id=tc_id,
                        result=(tool_result.data if tool_result.ok else None),
                        error=tool_result.error,
                        elapsed_ms=elapsed_ms,
                    ),
                }

                if tool_result.is_terminal and tool_result.ok:
                    terminal_payload = dict(tool_result.data)
                    break

            if terminal_payload is not None:
                break

        else:
            aborted = True
            abort_reason = f"hit max_steps={self.max_steps} without final_answer"

        # 7. Build final result
        if terminal_payload is None and not aborted:
            terminal_payload = {
                "summary": "(agent stopped without final_answer)",
                "sql": None, "row_count": None,
            }
        result = AgentResult(
            conversation_id=conversation_id,
            final_summary=(terminal_payload or {}).get("summary") or "",
            final_sql=(terminal_payload or {}).get("sql"),
            final_row_count=(terminal_payload or {}).get("row_count"),
            aborted=aborted,
            abort_reason=abort_reason,
            total_ms=(time.perf_counter() - t_start) * 1000.0,
        )

        # 8. Post-process: run viz + description on the last successful
        #    run_sql payload. Mirrors old pipeline stages [9-10] which
        #    happen AFTER execute and AFTER the LLM is done. The agent
        #    cannot opt out of this — it always runs when there are rows.
        last_rows, last_sql = self._last_run_sql_rows(conversation_id)
        if last_rows is not None:
            result.final_rows = last_rows
            if result.final_sql is None and last_sql:
                result.final_sql = last_sql
            if result.final_row_count is None:
                result.final_row_count = len(last_rows)
            yield {"kind": "post_process_started", "row_count": len(last_rows)}
            try:
                viz_out = self.tool_ctx.viz_describer.annotate(
                    nl_question=user_message,
                    rows=last_rows,
                    sql=last_sql or result.final_sql or "",
                ) if self.tool_ctx.viz_describer is not None else None
            except Exception as e:
                log.warning("post-process viz/desc failed: %s", e)
                viz_out = None
            if viz_out is not None:
                if viz_out.spec is not None:
                    result.viz = {
                        "kind": viz_out.spec.kind,
                        "x": viz_out.spec.x,
                        "y": viz_out.spec.y,
                        "color": viz_out.spec.color,
                        "title": viz_out.spec.title,
                        "rationale": viz_out.spec.rationale,
                    }
                    try:
                        result.vega_lite = viz_out.spec.to_vega_lite(last_rows)
                    except Exception as e:
                        log.debug("vega_lite build failed: %s", e)
                if viz_out.description:
                    result.description = viz_out.description
            yield {
                "kind": "viz",
                "rows": last_rows,
                "viz": result.viz,
                "vega_lite": result.vega_lite,
                "description": result.description,
            }

        yield {"kind": "result", "result": result}

    def _last_run_sql_rows(
        self, conversation_id: uuid.UUID,
    ) -> tuple[list[dict[str, Any]] | None, str | None]:
        """Walk the persisted conversation in reverse for the most recent
        successful run_sql tool result. Returns (rows, sql) or (None, None)."""
        try:
            history = self.conv_store.history(conversation_id)
        except Exception:
            return None, None
        for m in reversed(history):
            if m.role != "tool" or m.tool_name != "run_sql":
                continue
            try:
                payload = json.loads(m.content)
            except Exception:
                continue
            if payload.get("ok") and isinstance(payload.get("rows"), list):
                return payload["rows"], payload.get("sql") or None
        return None, None

    # ── Event serialization for SSE ──────────────────────────────────────────

    @staticmethod
    def _serialize_event(ev: dict[str, Any]) -> dict[str, Any]:
        # Token-level deltas + viz pass through unchanged.
        kind = ev.get("kind")
        if kind in ("text_delta", "tool_call_delta", "viz", "post_process_started"):
            return ev
        if kind == "step":
            s: AgentStep = ev["step"]
            return {
                "kind": "step",
                "step": {
                    "kind": s.kind,
                    "name": s.name,
                    "arguments": s.arguments,
                    "tool_call_id": s.tool_call_id,
                    "result": s.result,
                    "error": s.error,
                    "content": s.content,
                    "elapsed_ms": s.elapsed_ms,
                },
            }
        if ev.get("kind") == "result":
            r: AgentResult = ev["result"]
            return {
                "kind": "result",
                "result": {
                    "conversation_id": str(r.conversation_id),
                    "final_summary": r.final_summary,
                    "final_sql": r.final_sql,
                    "final_row_count": r.final_row_count,
                    "aborted": r.aborted,
                    "abort_reason": r.abort_reason,
                    "total_ms": r.total_ms,
                    "rows": r.final_rows,
                    "viz": r.viz,
                    "vega_lite": r.vega_lite,
                    "description": r.description,
                },
            }
        if ev.get("kind") == "conversation_id":
            return {"kind": "conversation_id", "id": str(ev["id"])}
        return ev
