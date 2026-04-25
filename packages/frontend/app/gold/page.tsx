"use client";

import { useEffect, useState } from "react";
import { api, GoldRecord } from "@/lib/api";

const STATUSES = ["", "pending", "approved", "rejected"] as const;

export default function GoldPage() {
  const [records, setRecords] = useState<GoldRecord[]>([]);
  const [status, setStatus] = useState<string>("");
  const [creating, setCreating] = useState(false);
  const [draft, setDraft] = useState({ nl: "", sql: "", tables: "" });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setBusy(true);
    try {
      const r = await api.goldList(status || undefined, 200);
      setRecords(r.gold);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status]);

  async function submitDraft() {
    setError(null);
    try {
      await api.goldCreate({
        nl_question: draft.nl,
        sql: draft.sql,
        tables_used: draft.tables.split(",").map((s) => s.trim()).filter(Boolean),
        author: "ui",
      });
      setDraft({ nl: "", sql: "", tables: "" });
      setCreating(false);
      load();
    } catch (e) {
      setError(String(e));
    }
  }

  async function approve(id: string) {
    await api.goldApprove(id, "ui");
    load();
  }
  async function reject(id: string) {
    await api.goldReject(id, "ui", "rejected from UI");
    load();
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Gold SQL</h1>
        <button
          onClick={() => setCreating((v) => !v)}
          className="text-sm bg-accent text-bg px-3 py-1 rounded font-semibold"
        >
          {creating ? "Cancel" : "New"}
        </button>
      </div>

      {creating && (
        <div className="border border-border bg-panel rounded p-4 space-y-2">
          <input
            className="w-full bg-bg border border-border rounded px-2 py-1.5 text-sm"
            placeholder="NL question"
            value={draft.nl}
            onChange={(e) => setDraft({ ...draft, nl: e.target.value })}
          />
          <textarea
            className="w-full bg-bg border border-border rounded px-2 py-1.5 text-xs font-mono"
            placeholder="SQL"
            rows={5}
            value={draft.sql}
            onChange={(e) => setDraft({ ...draft, sql: e.target.value })}
          />
          <input
            className="w-full bg-bg border border-border rounded px-2 py-1.5 text-xs font-mono"
            placeholder="tables_used (comma-separated fqns)"
            value={draft.tables}
            onChange={(e) => setDraft({ ...draft, tables: e.target.value })}
          />
          <button
            onClick={submitDraft}
            disabled={!draft.nl || !draft.sql}
            className="bg-accent text-bg px-3 py-1 text-sm rounded disabled:opacity-40"
          >
            Submit (pending)
          </button>
        </div>
      )}

      <div className="flex gap-2">
        {STATUSES.map((s) => (
          <button
            key={s || "all"}
            onClick={() => setStatus(s)}
            className={`text-xs px-2 py-1 rounded border ${
              status === s ? "border-accent text-accent" : "border-border text-muted"
            }`}
          >
            {s || "all"}
          </button>
        ))}
        {busy && <span className="text-xs text-muted ml-2">loading…</span>}
        <span className="text-xs text-muted ml-auto">{records.length} records</span>
      </div>

      {error && <div className="text-red-400 text-sm">{error}</div>}

      <div className="space-y-3">
        {records.map((r) => (
          <details key={r.id} className="border border-border bg-panel rounded p-3">
            <summary className="cursor-pointer text-sm flex items-center gap-2">
              <span
                className={`tag ${
                  r.approval_status === "approved"
                    ? "!bg-accent !text-bg"
                    : r.approval_status === "rejected"
                    ? "!bg-red-500/20 !text-red-300"
                    : ""
                }`}
              >
                {r.approval_status}
              </span>
              <span className="flex-1">{r.nl_question}</span>
              <span className="text-xs text-muted">
                {(r.tables_used || []).slice(0, 3).join(" · ")}
              </span>
            </summary>
            <pre className="sql mt-2">{r.sql_text}</pre>
            <div className="text-xs text-muted mt-2">
              author={r.author || "?"}
              {r.approved_by && ` · reviewer=${r.approved_by}`}
              {r.exec_check_passed && " · ✓ exec OK"}
            </div>
            {r.approval_status === "pending" && (
              <div className="mt-2 flex gap-2">
                <button
                  onClick={() => approve(r.id)}
                  className="text-xs bg-accent text-bg px-2 py-1 rounded"
                >
                  Approve
                </button>
                <button
                  onClick={() => reject(r.id)}
                  className="text-xs border border-border text-muted px-2 py-1 rounded"
                >
                  Reject
                </button>
              </div>
            )}
          </details>
        ))}
      </div>
    </div>
  );
}
