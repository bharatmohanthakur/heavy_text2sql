"use client";

import { useEffect, useRef, useState } from "react";
import { AgentStep, ConversationSummary, StreamEvent, VizSpec, api, streamChat } from "@/lib/api";
import { RowsTable } from "@/components/RowsTable";
import { VegaChart } from "@/components/VegaChart";

type DraftToolCall = {
  index: number;
  id: string;
  name: string;
  arguments: string; // accumulated raw JSON string as it streams in
};

type DisplayMessage =
  | { kind: "user"; content: string }
  | {
      kind: "agent";
      steps: AgentStep[];
      drafts: DraftToolCall[]; // currently-streaming tool calls (cleared per LLM turn once executed)
      assistantText: string;   // streaming free text
      summary: string;
      sql: string | null;
      rowCount: number | null;
      // Post-process artifacts (viz/desc) — populated AFTER the agent
      // terminates, regardless of what tools the LLM chose to call.
      rows: Record<string, unknown>[];
      viz: VizSpec | null;
      vegaLite: Record<string, unknown> | null;
      description: string;
      postProcessing: boolean;
    };

export default function ChatPage() {
  const [convId, setConvId] = useState<string | null>(null);
  const [convs, setConvs] = useState<ConversationSummary[]>([]);
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    refreshConversations();
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  async function refreshConversations() {
    try {
      const r = await api.conversations();
      setConvs(r.conversations);
    } catch (e) {
      console.warn("conversations failed", e);
    }
  }

  async function loadConversation(id: string) {
    try {
      const detail = await api.conversation(id);
      const msgs: DisplayMessage[] = [];
      const emptyAgent = (steps: AgentStep[], summary = "", sql: string | null = null, rowCount: number | null = null) => ({
        kind: "agent" as const,
        steps,
        drafts: [],
        assistantText: "",
        summary,
        sql,
        rowCount,
        rows: [],
        viz: null,
        vegaLite: null,
        description: "",
        postProcessing: false,
      });
      let pending: AgentStep[] = [];
      for (const m of detail.messages) {
        if (m.role === "user") {
          if (pending.length) {
            msgs.push(emptyAgent(pending));
            pending = [];
          }
          msgs.push({ kind: "user", content: m.content });
        } else if (m.role === "assistant" && m.tool_calls) {
          for (const tc of m.tool_calls) {
            let args: Record<string, unknown> | null = null;
            try { args = JSON.parse(tc.function.arguments || "{}"); } catch { args = null; }
            pending.push({
              kind: "tool_call",
              name: tc.function.name,
              arguments: args,
              tool_call_id: tc.id,
              result: null, error: null, content: "", elapsed_ms: null,
            });
          }
        } else if (m.role === "tool") {
          let parsed: Record<string, unknown> | null = null;
          let error: string | null = null;
          try {
            const obj = JSON.parse(m.content) as Record<string, unknown>;
            if (obj.ok === false) error = String(obj.error ?? "error");
            else parsed = obj;
          } catch { /* ignore */ }
          pending.push({
            kind: "tool_result",
            name: m.tool_name,
            arguments: null,
            tool_call_id: m.tool_call_id,
            result: parsed, error, content: "", elapsed_ms: null,
          });
          if (m.tool_name === "final_answer" && parsed) {
            msgs.push(emptyAgent(
              pending,
              String(parsed.summary ?? ""),
              parsed.sql ? String(parsed.sql) : null,
              typeof parsed.row_count === "number" ? parsed.row_count : null,
            ));
            pending = [];
          }
        } else if (m.role === "assistant" && m.content) {
          msgs.push(emptyAgent(pending, m.content));
          pending = [];
        }
      }
      if (pending.length) {
        msgs.push(emptyAgent(pending));
      }
      setMessages(msgs);
      setConvId(id);
    } catch (e) {
      console.warn("load conversation failed", e);
    }
  }

  async function send() {
    const text = input.trim();
    if (!text || streaming) return;
    setInput("");
    setMessages((prev) => [
      ...prev,
      { kind: "user", content: text },
      {
        kind: "agent",
        steps: [],
        drafts: [],
        assistantText: "",
        summary: "",
        sql: null,
        rowCount: null,
        rows: [],
        viz: null,
        vegaLite: null,
        description: "",
        postProcessing: false,
      },
    ]);
    setStreaming(true);
    abortRef.current = new AbortController();

    // Immutable-update helper: replace the LAST message (the agent bubble we
    // just pushed) with a new object built from the patcher. Mutating in
    // place means React batches our setState calls into a single render and
    // the user sees no streaming.
    function patchLastAgent(patch: (a: Extract<DisplayMessage, { kind: "agent" }>) => Extract<DisplayMessage, { kind: "agent" }>) {
      setMessages((prev) => {
        if (prev.length === 0) return prev;
        const last = prev[prev.length - 1];
        if (last.kind !== "agent") return prev;
        return [...prev.slice(0, -1), patch(last)];
      });
    }

    try {
      await streamChat(text, convId, (ev: StreamEvent) => {
        if (ev.kind === "conversation_id") {
          setConvId(ev.id);
        } else if (ev.kind === "text_delta") {
          patchLastAgent((a) => ({ ...a, assistantText: a.assistantText + ev.delta }));
        } else if (ev.kind === "tool_call_delta") {
          patchLastAgent((a) => {
            const drafts = [...a.drafts];
            const i = drafts.findIndex((d) => d.index === ev.index);
            if (i < 0) {
              drafts.push({
                index: ev.index,
                id: ev.id,
                name: ev.name,
                arguments: ev.arguments_delta,
              });
            } else {
              drafts[i] = {
                ...drafts[i],
                id: ev.id || drafts[i].id,
                name: ev.name || drafts[i].name,
                arguments: drafts[i].arguments + ev.arguments_delta,
              };
            }
            return { ...a, drafts };
          });
        } else if (ev.kind === "step") {
          // When the assembled tool_call lands, it supersedes the draft for
          // that tool_call_id. Promote drafts → steps and clear them once
          // the tool_result arrives (which always follows the tool_call).
          patchLastAgent((a) => {
            const next = { ...a, steps: [...a.steps, ev.step] };
            if (ev.step.kind === "tool_result") {
              next.drafts = a.drafts.filter((d) => d.id !== ev.step.tool_call_id);
            }
            return next;
          });
        } else if (ev.kind === "post_process_started") {
          patchLastAgent((a) => ({ ...a, postProcessing: true }));
        } else if (ev.kind === "viz") {
          patchLastAgent((a) => ({
            ...a,
            postProcessing: false,
            rows: ev.rows,
            viz: ev.viz,
            vegaLite: ev.vega_lite,
            description: ev.description,
          }));
        } else if (ev.kind === "result") {
          patchLastAgent((a) => ({
            ...a,
            drafts: [],
            summary: ev.result.final_summary,
            sql: ev.result.final_sql,
            rowCount: ev.result.final_row_count,
            rows: ev.result.rows && ev.result.rows.length ? ev.result.rows : a.rows,
            viz: ev.result.viz ?? a.viz,
            vegaLite: ev.result.vega_lite ?? a.vegaLite,
            description: ev.result.description ?? a.description,
            postProcessing: false,
          }));
        } else if (ev.kind === "error") {
          patchLastAgent((a) => ({ ...a, summary: `Error: ${ev.error}` }));
        }
      }, abortRef.current.signal);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      patchLastAgent((a) => ({ ...a, summary: `Stream error: ${msg}` }));
    } finally {
      setStreaming(false);
      abortRef.current = null;
      refreshConversations();
    }
  }

  function newConversation() {
    abortRef.current?.abort();
    setConvId(null);
    setMessages([]);
  }

  return (
    <div className="flex gap-6 h-[calc(100vh-12rem)]">
      <aside className="w-64 shrink-0 flex flex-col gap-2 overflow-hidden">
        <button
          onClick={newConversation}
          className="border border-border rounded px-3 py-2 text-sm hover:border-accent hover:text-accent text-left"
        >
          + New conversation
        </button>
        <div className="flex-1 overflow-auto space-y-1">
          {convs.map((c) => (
            <button
              key={c.id}
              onClick={() => loadConversation(c.id)}
              className={`w-full text-left text-xs border rounded px-2 py-2 ${
                c.id === convId ? "border-accent text-accent" : "border-border text-muted hover:border-accent"
              }`}
              title={c.dialect ? `${c.title} · ${c.dialect}` : c.title}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="truncate flex-1">{c.title || "(untitled)"}</span>
                {c.dialect && <DialectBadge dialect={c.dialect} />}
              </div>
            </button>
          ))}
        </div>
      </aside>

      <section className="flex-1 flex flex-col">
        <div ref={scrollRef} className="flex-1 overflow-auto space-y-4 pr-2">
          {messages.length === 0 && (
            <div className="text-sm text-muted">
              Ask the agent a question. It will call classify_domains → search_tables →
              inspect_table → run_sql → final_answer, with each step streamed live.
            </div>
          )}
          {messages.map((m, i) => (
            <MessageBubble key={i} m={m} />
          ))}
        </div>

        <div className="mt-4 flex gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                send();
              }
            }}
            placeholder={streaming ? "Agent is working…" : "Ask anything about the Ed-Fi ODS"}
            disabled={streaming}
            className="flex-1 border border-border bg-panel rounded px-3 py-2 text-sm focus:outline-none focus:border-accent"
          />
          <button
            onClick={send}
            disabled={streaming || !input.trim()}
            className="border border-accent text-accent rounded px-4 py-2 text-sm disabled:opacity-50"
          >
            {streaming ? "…" : "Send"}
          </button>
        </div>
        {convId && (
          <div className="text-xs text-muted mt-2">conversation: {convId.slice(0, 8)}…</div>
        )}
      </section>
    </div>
  );
}

function DialectBadge({ dialect }: { dialect: string }) {
  const palette: Record<string, string> = {
    mssql: "border-blue-500 text-blue-400",
    sqlite: "border-emerald-500 text-emerald-400",
    postgresql: "border-cyan-500 text-cyan-400",
  };
  const klass = palette[dialect] ?? "border-border text-muted";
  return (
    <span className={`shrink-0 text-[10px] uppercase tracking-wide border rounded px-1 py-px ${klass}`}>
      {dialect}
    </span>
  );
}

function MessageBubble({ m }: { m: DisplayMessage }) {
  if (m.kind === "user") {
    return (
      <div className="flex justify-end">
        <div className="border border-accent text-accent rounded-lg px-3 py-2 max-w-2xl text-sm whitespace-pre-wrap">
          {m.content}
        </div>
      </div>
    );
  }
  const hasAnything =
    m.steps.length > 0 ||
    m.drafts.length > 0 ||
    m.assistantText ||
    m.summary;
  return (
    <div className="border border-border rounded-lg p-3 space-y-2">
      {(m.steps.length > 0 || m.drafts.length > 0) && (
        <details open className="text-xs">
          <summary className="cursor-pointer text-muted hover:text-accent">
            {m.steps.length} step{m.steps.length === 1 ? "" : "s"}
            {m.drafts.length > 0 && (
              <span className="ml-1 text-accent">
                · {m.drafts.length} streaming…
              </span>
            )}
          </summary>
          <div className="mt-2 space-y-1">
            {m.steps.map((s, i) => (
              <StepLine key={i} step={s} />
            ))}
            {m.drafts.map((d) => (
              <DraftLine key={`draft-${d.id || d.index}`} draft={d} />
            ))}
          </div>
        </details>
      )}
      {m.assistantText && !m.summary && (
        <div className="text-sm whitespace-pre-wrap text-muted italic">{m.assistantText}</div>
      )}
      {m.summary && (
        <div className="text-sm whitespace-pre-wrap">{m.summary}</div>
      )}
      {m.sql && (
        <pre className="bg-panel border border-border rounded px-2 py-1 text-xs overflow-x-auto">
          {m.sql}
        </pre>
      )}
      {m.rowCount !== null && (
        <div className="text-xs text-muted">{m.rowCount} row{m.rowCount === 1 ? "" : "s"}</div>
      )}
      {m.postProcessing && (
        <div className="text-xs text-muted italic">Generating chart + description…</div>
      )}
      {m.description && (
        <div className="border-l-2 border-accent pl-3 text-sm text-muted leading-relaxed">
          {m.description}
        </div>
      )}
      {m.vegaLite && (
        <div className="border border-border rounded p-3 bg-panel w-full">
          <div className="text-xs text-muted mb-2">
            Chart{m.viz?.kind ? ` — ${m.viz.kind}` : ""}
            {m.viz?.title ? `: ${m.viz.title}` : ""}
          </div>
          <div className="w-full">
            <VegaChart spec={m.vegaLite} />
          </div>
        </div>
      )}
      {m.rows.length > 0 && (
        <details>
          <summary className="cursor-pointer text-xs text-muted hover:text-accent">
            {m.rows.length} row{m.rows.length === 1 ? "" : "s"} (table)
          </summary>
          <div className="mt-2">
            <RowsTable rows={m.rows} />
          </div>
        </details>
      )}
      {!hasAnything && (
        <div className="text-xs text-muted">Thinking…</div>
      )}
    </div>
  );
}

function DraftLine({ draft }: { draft: DraftToolCall }) {
  return (
    <div className="font-mono text-accent">
      → <span>{draft.name || "…"}</span>
      <span className="ml-1 text-muted">
        ({draft.arguments.length > 200 ? draft.arguments.slice(0, 200) + "…" : draft.arguments}
        <span className="inline-block w-1 ml-0.5 animate-pulse">▎</span>)
      </span>
    </div>
  );
}

function StepLine({ step }: { step: AgentStep }) {
  if (step.kind === "tool_call") {
    const args = step.arguments ? JSON.stringify(step.arguments) : "";
    return (
      <div className="font-mono text-muted">
        → <span className="text-accent">{step.name}</span>
        {args && <span className="ml-1 truncate">({args.length > 200 ? args.slice(0, 200) + "…" : args})</span>}
      </div>
    );
  }
  if (step.kind === "tool_result") {
    const ok = !step.error;
    const body = ok
      ? (step.result ? JSON.stringify(step.result) : "(ok)")
      : (step.error || "");
    return (
      <div className={`font-mono ${ok ? "text-muted" : "text-red-400"}`}>
        ← <span className="text-accent">{step.name}</span>
        {step.elapsed_ms !== null && <span className="ml-1">[{step.elapsed_ms.toFixed(0)}ms]</span>}
        <span className="ml-1">{body.length > 220 ? body.slice(0, 220) + "…" : body}</span>
      </div>
    );
  }
  if (step.kind === "error") {
    return <div className="font-mono text-red-400">⚠ {step.error}</div>;
  }
  return <div className="font-mono text-muted">{step.content}</div>;
}
