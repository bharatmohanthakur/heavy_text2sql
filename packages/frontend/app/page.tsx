"use client";

import { useState } from "react";
import { api, QueryResponse } from "@/lib/api";
import { RowsTable } from "@/components/RowsTable";
import { VegaChart } from "@/components/VegaChart";

const EXAMPLES = [
  "How many students are enrolled in each school?",
  "List all schools and their grade levels offered",
  "How many Hispanic students are in the district?",
  "Count of staff by sex",
  "Total enrollment count",
];

export default function QueryPage() {
  const [question, setQuestion] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [resp, setResp] = useState<QueryResponse | null>(null);

  async function run(q: string) {
    setBusy(true);
    setError(null);
    setResp(null);
    try {
      const r = await api.query(q);
      setResp(r);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold mb-3">Ask a question</h1>
        <textarea
          className="w-full bg-panel border border-border rounded p-3 text-sm font-mono focus:outline-none focus:border-accent"
          placeholder="e.g. How many Hispanic students are in the district?"
          rows={3}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />
        <div className="flex items-center gap-3 mt-2">
          <button
            onClick={() => run(question)}
            disabled={busy || !question.trim()}
            className="px-4 py-1.5 bg-accent text-bg rounded text-sm font-semibold disabled:opacity-40"
          >
            {busy ? "Running…" : "Run"}
          </button>
          <span className="text-xs text-muted">examples →</span>
          {EXAMPLES.map((ex) => (
            <button
              key={ex}
              onClick={() => {
                setQuestion(ex);
                run(ex);
              }}
              className="text-xs text-muted hover:text-accent underline truncate max-w-[180px]"
            >
              {ex}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div className="border border-red-500/40 bg-red-500/10 text-red-300 rounded p-3 text-sm">
          {error}
        </div>
      )}

      {resp && <ResultPanel resp={resp} />}
    </div>
  );
}

function ResultPanel({ resp }: { resp: QueryResponse }) {
  return (
    <div className="space-y-5">
      {resp.description && (
        <div className="border border-border bg-panel rounded p-4">
          <h2 className="text-sm font-semibold text-muted mb-1">Summary</h2>
          <p className="text-sm leading-relaxed">{resp.description}</p>
        </div>
      )}

      {resp.viz_vega_lite && (
        <div className="border border-border bg-panel rounded p-4">
          <h2 className="text-sm font-semibold text-muted mb-3">
            Chart — {resp.viz?.kind}
          </h2>
          <VegaChart spec={resp.viz_vega_lite} />
        </div>
      )}

      <div className="border border-border bg-panel rounded p-4">
        <div className="flex justify-between items-baseline mb-2">
          <h2 className="text-sm font-semibold text-muted">
            Rows ({resp.row_count ?? 0})
          </h2>
          <span className="text-xs text-muted">
            {Object.entries(resp.timings_ms || {})
              .sort(([, a], [, b]) => b - a)
              .slice(0, 3)
              .map(([k, v]) => `${k}=${v.toFixed(0)}ms`)
              .join("  ")}
          </span>
        </div>
        <RowsTable rows={resp.rows} />
      </div>

      <details className="border border-border bg-panel rounded p-4">
        <summary className="text-sm font-semibold text-muted cursor-pointer">
          SQL & pipeline detail
        </summary>
        <div className="mt-3 space-y-3">
          <div>
            <h3 className="text-xs uppercase tracking-wide text-muted mb-1">SQL</h3>
            <pre className="sql">{resp.sql}</pre>
            {resp.rationale && (
              <p className="text-xs text-muted mt-1">{resp.rationale}</p>
            )}
          </div>
          {resp.domains && (
            <div>
              <h3 className="text-xs uppercase tracking-wide text-muted mb-1">
                Domains
              </h3>
              <div>
                {resp.domains.domains.map((d) => (
                  <span key={d} className="tag">
                    {d}
                  </span>
                ))}
              </div>
              <p className="text-xs text-muted mt-1">{resp.domains.reasoning}</p>
            </div>
          )}
          {resp.resolved_entities.length > 0 && (
            <div>
              <h3 className="text-xs uppercase tracking-wide text-muted mb-1">
                Resolved entities
              </h3>
              <ul className="text-xs space-y-1 font-mono">
                {resp.resolved_entities.map((e, i) => (
                  <li key={i}>
                    <span className="text-accent">{e.value}</span> →{" "}
                    <span className="text-muted">
                      {e.fqn}.{e.column}
                    </span>
                    {e.descriptor_type && (
                      <span className="ml-2 tag">{e.descriptor_type}</span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {resp.selected_tables.length > 0 && (
            <div>
              <h3 className="text-xs uppercase tracking-wide text-muted mb-1">
                Selected tables
              </h3>
              <div>
                {resp.selected_tables.map((t) => (
                  <span key={t} className="tag font-mono">
                    {t}
                  </span>
                ))}
              </div>
            </div>
          )}
          {resp.repair_attempts.length > 1 && (
            <div>
              <h3 className="text-xs uppercase tracking-wide text-muted mb-1">
                Repair attempts ({resp.repair_attempts.length})
              </h3>
              <ul className="text-xs space-y-1">
                {resp.repair_attempts.map((a, i) => (
                  <li key={i} className={a.accepted ? "text-accent" : "text-red-400"}>
                    [{i}] {a.accepted ? "OK" : a.error?.slice(0, 200)}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </details>
    </div>
  );
}
