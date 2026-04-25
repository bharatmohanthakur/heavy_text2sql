// Typed client for the text2sql FastAPI backend.
// Same-origin under /api in dev (Next rewrites proxy to localhost:8011).

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "/api";

export type DomainScore = { name: string; table_count: number };

export type TableSummary = {
  fqn: string;
  schema: string;
  table: string;
  description: string;
  domains: string[];
  is_descriptor: boolean;
  row_count: number | null;
  column_count: number;
};

export type TableDetail = TableSummary & {
  description_source: string;
  is_association: boolean;
  is_extension: boolean;
  primary_key: string[];
  parent_neighbors: string[];
  child_neighbors: string[];
  aggregate_root: string | null;
  columns: ColumnInfo[];
  sample_rows: Record<string, unknown>[];
};

export type ColumnInfo = {
  name: string;
  data_type: string | null;
  nullable: boolean | null;
  description: string;
  description_source: string;
  is_identifying: boolean;
  sample_values: string[];
  distinct_count: number | null;
};

export type VizSpec = {
  kind: "bar" | "line" | "point" | "stat" | "table";
  x: string | null;
  y: string | null;
  color: string | null;
  title: string;
  rationale: string;
};

export type ResolvedEntity = {
  fqn: string;
  column: string;
  value: string;
  score: number;
  tier: string;
  descriptor_type: string;
  child_fqn: string;
  descriptor_id: number | null;
};

export type RepairAttempt = {
  sql: string;
  rationale: string;
  error: string | null;
  accepted: boolean;
};

export type QueryResponse = {
  nl_question: string;
  sql: string;
  rationale: string;
  rows: Record<string, unknown>[];
  row_count: number | null;
  executed: boolean;
  validated: boolean;
  error: string | null;
  description: string;
  viz: VizSpec | null;
  viz_vega_lite: Record<string, unknown> | null;
  domains: { domains: string[]; reasoning: string } | null;
  selected_tables: string[];
  join_tree: { nodes: string[]; edge_count: number; total_weight: number } | null;
  resolved_entities: ResolvedEntity[];
  few_shot_count: number;
  repair_attempts: RepairAttempt[];
  timings_ms: Record<string, number>;
};

export type GoldRecord = {
  id: string;
  nl_question: string;
  sql_text: string;
  tables_used: string[];
  domains_used: string[];
  approval_status: "pending" | "approved" | "rejected";
  exec_check_passed: boolean;
  author: string;
  approved_by: string | null;
  note: string;
  sql_ast_flat?: string;
  created_at: string | null;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body || path}`);
  }
  return res.json();
}

export const api = {
  health: () => request<{ status: string; tables: number; domains: number; gold_store: boolean }>(
    "/health",
  ),

  query: (question: string, max_rows = 100) =>
    request<QueryResponse>("/query", {
      method: "POST",
      body: JSON.stringify({ question, execute: true, max_rows }),
    }),

  domains: () => request<{ total: number; domains: DomainScore[] }>("/domains"),

  tables: (opts: { domain?: string; descriptors?: boolean; limit?: number } = {}) => {
    const p = new URLSearchParams();
    if (opts.domain) p.set("domain", opts.domain);
    if (opts.descriptors === false) p.set("descriptors", "false");
    if (opts.limit) p.set("limit", String(opts.limit));
    const qs = p.toString();
    return request<{ total: number; tables: TableSummary[] }>(
      `/tables${qs ? `?${qs}` : ""}`,
    );
  },

  table: (fqn: string) => request<TableDetail>(`/tables/${encodeURIComponent(fqn)}`),

  goldList: (status?: string, limit = 50) => {
    const p = new URLSearchParams();
    if (status) p.set("status", status);
    p.set("limit", String(limit));
    return request<{ total: number; gold: GoldRecord[] }>(`/gold?${p.toString()}`);
  },

  goldCreate: (body: {
    nl_question: string;
    sql: string;
    tables_used?: string[];
    author?: string;
    note?: string;
  }) => request<GoldRecord>("/gold", { method: "POST", body: JSON.stringify(body) }),

  goldApprove: (id: string, reviewer: string) =>
    request<GoldRecord>(`/gold/${id}/approve`, {
      method: "POST",
      body: JSON.stringify({ reviewer }),
    }),

  goldReject: (id: string, reviewer: string, reason = "") =>
    request<GoldRecord>(`/gold/${id}/reject`, {
      method: "POST",
      body: JSON.stringify({ reviewer, reason }),
    }),

  // ── Agentic chat ──────────────────────────────────────────────────────
  conversations: () =>
    request<{ total: number; conversations: ConversationSummary[] }>(`/conversations`),

  conversation: (id: string) =>
    request<ConversationDetail>(`/conversations/${id}`),

  chat: (message: string, conversation_id?: string) =>
    request<ChatResponse>(`/chat`, {
      method: "POST",
      body: JSON.stringify({ message, conversation_id }),
    }),
};

// ── Chat types ────────────────────────────────────────────────────────────

export type ConversationSummary = {
  id: string;
  title: string;
  created_at: string;
  last_active: string;
};

export type ConversationMessage = {
  seq: number;
  role: "user" | "assistant" | "tool";
  content: string;
  tool_calls: { id: string; type: string; function: { name: string; arguments: string } }[] | null;
  tool_call_id: string | null;
  tool_name: string | null;
  created_at: string;
};

export type ConversationDetail = ConversationSummary & {
  messages: ConversationMessage[];
};

export type AgentStep = {
  kind: "tool_call" | "tool_result" | "assistant" | "error";
  name: string | null;
  arguments: Record<string, unknown> | null;
  tool_call_id: string | null;
  result: Record<string, unknown> | null;
  error: string | null;
  content: string;
  elapsed_ms: number | null;
};

export type ChatResponse = {
  conversation_id: string;
  summary: string;
  sql: string | null;
  row_count: number | null;
  aborted: boolean;
  abort_reason: string | null;
  total_ms: number;
  steps: AgentStep[];
};

export type StreamEvent =
  | { kind: "conversation_id"; id: string }
  | { kind: "text_delta"; delta: string }
  | {
      kind: "tool_call_delta";
      index: number;
      id: string;
      name: string;
      arguments_delta: string;
    }
  | { kind: "step"; step: AgentStep }
  | { kind: "post_process_started"; row_count: number }
  | {
      kind: "viz";
      rows: Record<string, unknown>[];
      viz: VizSpec | null;
      vega_lite: Record<string, unknown> | null;
      description: string;
    }
  | {
      kind: "result";
      result: {
        conversation_id: string;
        final_summary: string;
        final_sql: string | null;
        final_row_count: number | null;
        aborted: boolean;
        abort_reason: string | null;
        total_ms: number;
        rows?: Record<string, unknown>[];
        viz?: VizSpec | null;
        vega_lite?: Record<string, unknown> | null;
        description?: string;
      };
    }
  | { kind: "error"; error: string };

/**
 * Open an SSE-style stream against POST /chat/stream.
 *
 * Native EventSource only supports GET, so we read the response body manually
 * and parse `data: <json>\n\n` frames. Between dispatched events we yield to
 * a paint frame so React can commit a render per token-delta — without this,
 * multiple frames inside one network chunk get batched into a single render
 * and the UI looks frozen until the very end.
 */
const yieldToPaint = () =>
  typeof window !== "undefined" && typeof requestAnimationFrame === "function"
    ? new Promise<void>((r) => requestAnimationFrame(() => r()))
    : Promise.resolve();

export async function streamChat(
  message: string,
  conversation_id: string | null,
  onEvent: (ev: StreamEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch(`${BASE}/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, conversation_id }),
    signal,
  });
  if (!res.ok || !res.body) {
    const text = await res.text().catch(() => "");
    throw new Error(`stream failed ${res.status}: ${text}`);
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      for (const line of frame.split("\n")) {
        if (line.startsWith("data: ")) {
          const json = line.slice(6);
          try {
            onEvent(JSON.parse(json) as StreamEvent);
            // Force React to commit a render before the next event so the
            // user actually sees the typing effect.
            await yieldToPaint();
          } catch (e) {
            console.warn("bad SSE frame", json, e);
          }
        }
      }
    }
  }
}
