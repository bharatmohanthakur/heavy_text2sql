"use client";

import { useEffect, useState } from "react";
import { AdminConfig, DbTestResult, JobStatus, ProviderEntry, api, streamJob } from "@/lib/api";

/* Settings page — view + edit the resolved runtime config.
 *
 * Each card shows the section's primary, the registered providers, and
 * (where applicable) inline editors that POST a partial overlay to
 * /admin/config. Secret credentials never leave .env; the UI only
 * changes selectors (which provider is primary, which task routes
 * where, etc.).
 *
 * Switching the embedding primary surfaces a re-index banner when the
 * new provider has a different output dimension than the current FAISS
 * index — the index built for one dim cannot serve queries from another. */

export default function SettingsPage() {
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tests, setTests] = useState<Record<string, DbTestResult & { provider: string; pending?: boolean }>>({});
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    api.adminConfig().then(setConfig).catch((e) => setError(String(e)));
  }, []);

  if (error) {
    return <div className="text-red-400 text-sm">Failed to load /admin/config: {error}</div>;
  }
  if (!config) {
    return <div className="text-muted text-sm">Loading…</div>;
  }

  async function runTest(provider: string) {
    setTests((t) => ({ ...t, [provider]: { provider, ok: false, error: null, elapsed_ms: null, server_version: null, pending: true } }));
    try {
      const r = await api.adminTestDb(provider);
      setTests((t) => ({ ...t, [provider]: { ...r, provider } }));
    } catch (e) {
      setTests((t) => ({ ...t, [provider]: { provider, ok: false, error: String(e), elapsed_ms: null, server_version: null } }));
    }
  }

  async function runMetadataTest() {
    const key = "__metadata__";
    setTests((t) => ({ ...t, [key]: { provider: key, ok: false, error: null, elapsed_ms: null, server_version: null, pending: true } }));
    try {
      const r = await api.adminTestMetadataDb();
      setTests((t) => ({ ...t, [key]: { ...r, provider: key } }));
    } catch (e) {
      setTests((t) => ({ ...t, [key]: { provider: key, ok: false, error: String(e), elapsed_ms: null, server_version: null } }));
    }
  }

  async function patch(partial: Partial<AdminConfig>) {
    setSaving(true);
    setSaveError(null);
    try {
      const next = await api.adminPatchConfig(partial);
      setConfig(next);
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold text-accent">Settings</h1>
        <p className="text-sm text-muted mt-1">
          Resolved runtime configuration. Edits persist to{" "}
          <code className="text-accent">{config.overlay_path}</code>; secrets
          stay in <code className="text-accent">.env</code> and are never
          written by the UI.
        </p>
        <div className="mt-2 text-xs h-4">
          {saving && <span className="text-muted">Saving…</span>}
          {!saving && saveError && (
            <span className="text-red-400">Save failed: {saveError}</span>
          )}
        </div>
      </header>

      <Card title="Target Database" subtitle="The populated ODS the platform queries.">
        <div className="text-sm mb-2">
          <span className="text-muted">primary:</span>{" "}
          <span className="text-accent font-mono">{config.target_db.primary}</span>
        </div>
        <ProviderTable
          providers={config.target_db.providers}
          envPresent={config.env_present}
          highlight={config.target_db.primary}
          showTest
          tests={tests}
          onTest={runTest}
        />
      </Card>

      <Card title="Metadata Database" subtitle="Postgres — gold SQL store + conversation history.">
        <div className="font-mono text-sm border border-border rounded p-3 bg-panel space-y-1">
          {Object.entries(config.metadata_db).map(([k, v]) => (
            <div key={k} className="flex">
              <span className="w-44 text-muted">{k}:</span>
              <span>{String(v ?? "")}</span>
            </div>
          ))}
        </div>
        <div className="mt-3 flex items-center gap-3">
          <button
            onClick={runMetadataTest}
            disabled={tests["__metadata__"]?.pending}
            className="border border-accent text-accent rounded px-3 py-1 text-xs hover:bg-accent/10 disabled:opacity-50"
          >
            {tests["__metadata__"]?.pending ? "Testing…" : "Test connection"}
          </button>
          <TestResultLine result={tests["__metadata__"]} />
        </div>
      </Card>

      <Card title="LLM" subtitle="Primary + per-task routing. Each task slot picks one provider.">
        <div className="text-sm mb-3 flex flex-wrap gap-x-6 gap-y-2 items-center">
          <span>
            <span className="text-muted">primary:</span>{" "}
            <ProviderSelect
              providers={config.llm.providers}
              value={config.llm.primary}
              disabled={saving}
              onChange={(v) => patch({ llm: { primary: v } as never })}
            />
          </span>
          {config.llm.fallback && (
            <span>
              <span className="text-muted">fallback:</span>{" "}
              <ProviderSelect
                providers={config.llm.providers}
                value={config.llm.fallback}
                disabled={saving}
                onChange={(v) => patch({ llm: { fallback: v } as never })}
              />
            </span>
          )}
        </div>

        <div className="mb-4">
          <div className="text-xs text-muted uppercase tracking-wide mb-1">Task routing</div>
          <div className="border border-border rounded p-3 bg-panel space-y-2">
            {Object.entries(config.llm.task_routing).map(([task, provider]) => (
              <div key={task} className="flex items-center gap-3 text-xs">
                <span className="w-44 text-muted font-mono">{task}</span>
                <ProviderSelect
                  providers={config.llm.providers}
                  value={provider}
                  disabled={saving}
                  onChange={(v) =>
                    patch({
                      llm: {
                        task_routing: { ...config.llm.task_routing, [task]: v },
                      } as never,
                    })
                  }
                />
              </div>
            ))}
          </div>
        </div>

        <div className="text-xs text-muted uppercase tracking-wide mb-1">Providers</div>
        <ProviderTable
          providers={config.llm.providers}
          envPresent={config.env_present}
          highlight={config.llm.primary}
        />
      </Card>

      <Card title="Embedding" subtitle="Vectors used by the table retriever, gold few-shots, and entity resolver Tier 3.">
        {(() => {
          const currentDim = (config.embeddings.providers[config.embeddings.primary]?.dim as number | undefined) ?? null;
          // We can't directly query the on-disk FAISS dim from here, but
          // any change to `primary` whose dim differs from the current
          // primary's dim is, by construction, a re-index trigger. Show
          // a banner whenever the overlay carries an embeddings.primary
          // override (i.e. the user just changed it).
          const overlayPrimary = (config.overlay as { embeddings?: { primary?: string } })?.embeddings?.primary;
          const overlayDim = overlayPrimary ? (config.embeddings.providers[overlayPrimary]?.dim as number | undefined) ?? null : null;
          const dimChanged = overlayPrimary && overlayDim != null && currentDim != null && overlayDim !== currentDim;
          if (dimChanged) {
            return (
              <div className="mb-3 border border-yellow-500/50 bg-yellow-500/10 rounded p-3 text-xs text-yellow-300">
                ⚠ Embedding dim changed ({String(overlayDim)} ≠ previously indexed). The FAISS
                index needs rebuilding before /query and /chat can use the new vectors —
                run <code className="text-yellow-100">text2sql index-catalog</code> from the
                CLI, or wait for the rebuild orchestrator panel (K4).
              </div>
            );
          }
          return null;
        })()}

        <div className="text-sm mb-3 flex items-center gap-3">
          <span className="text-muted">primary:</span>
          <ProviderSelect
            providers={config.embeddings.providers}
            value={config.embeddings.primary}
            disabled={saving}
            onChange={(v) => patch({ embeddings: { primary: v } as never })}
          />
          {(() => {
            const dim = config.embeddings.providers[config.embeddings.primary]?.dim;
            return dim ? <span className="text-xs text-muted">dim: <span className="text-accent">{String(dim)}</span></span> : null;
          })()}
        </div>
        <ProviderTable
          providers={config.embeddings.providers}
          envPresent={config.env_present}
          highlight={config.embeddings.primary}
        />
      </Card>

      <Card title="Vector Store" subtitle="Where embedded vectors live.">
        <div className="text-sm mb-2">
          <span className="text-muted">primary:</span>{" "}
          <span className="text-accent font-mono">{config.vector_store.primary}</span>
        </div>
        <ProviderTable
          providers={config.vector_store.providers}
          envPresent={config.env_present}
          highlight={config.vector_store.primary}
        />
      </Card>

      <Card title="Runtime overrides" subtitle="UI-driven changes layered on top of YAML — written via /admin/config.">
        <pre className="bg-panel border border-border rounded p-3 text-xs overflow-x-auto">
          {JSON.stringify(config.overlay, null, 2)}
        </pre>
      </Card>

      <Card title="Rebuild" subtitle="Run any subset of build stages. Each stage spawns the same `text2sql` CLI command an operator would run; output streams here live.">
        <RebuildPanel />
      </Card>
    </div>
  );
}

// ── Rebuild panel ──────────────────────────────────────────────────────────

const ALL_STAGES: { id: string; label: string; help: string }[] = [
  { id: "ingest",   label: "Ingest Ed-Fi metadata",     help: "Fetch ApiModel.json + ForeignKeys.sql." },
  { id: "classify", label: "Classify tables",            help: "Map tables → Ed-Fi domains." },
  { id: "graph",    label: "FK graph + APSP",            help: "Build the rustworkx graph + dist/next-hop." },
  { id: "catalog",  label: "Table catalog (LLM descriptions)", help: "One-time, ~5 min. Uses catalog_description LLM slot." },
  { id: "index",    label: "Embed + FAISS index",        help: "Re-embed catalog with the current embedding provider." },
  { id: "gold-seed",label: "Gold few-shot seed",         help: "Load gold_queries_bootstrap.yaml, exec-validate against the live DB." },
];

function RebuildPanel() {
  const [selected, setSelected] = useState<Set<string>>(new Set(["index"]));
  const [job, setJob] = useState<JobStatus | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [running, setRunning] = useState(false);

  function toggle(id: string) {
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  async function start() {
    if (running || selected.size === 0) return;
    setRunning(true);
    setLines([]);
    setJob(null);
    try {
      const created = await api.adminRebuild(Array.from(selected));
      setJob(created);
      streamJob(created.id, (ev) => {
        if (ev.type === "line") {
          setLines((l) => [...l, ev.line]);
        } else if (ev.type === "status") {
          setJob({ ...ev });
        }
      }).catch((e) => {
        setLines((l) => [...l, `[stream error] ${e}`]);
      }).finally(() => {
        setRunning(false);
      });
    } catch (e) {
      setLines((l) => [...l, `[start failed] ${e instanceof Error ? e.message : String(e)}`]);
      setRunning(false);
    }
  }

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2">
        {ALL_STAGES.map((s) => (
          <label key={s.id} className="flex items-start gap-2 text-xs cursor-pointer">
            <input
              type="checkbox"
              checked={selected.has(s.id)}
              disabled={running}
              onChange={() => toggle(s.id)}
              className="mt-0.5"
            />
            <span>
              <span className="font-mono text-accent">{s.label}</span>
              <span className="block text-muted">{s.help}</span>
            </span>
          </label>
        ))}
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={start}
          disabled={running || selected.size === 0}
          className="border border-accent text-accent rounded px-3 py-1 text-sm disabled:opacity-50"
        >
          {running ? "Running…" : `Run ${selected.size} stage${selected.size === 1 ? "" : "s"}`}
        </button>
        {job && (
          <span className="text-xs text-muted">
            job <span className="font-mono">{job.id.slice(0, 8)}</span>
            {" — "}
            <span className={
              job.status === "succeeded" ? "text-emerald-400"
              : job.status === "failed" ? "text-red-400"
              : "text-accent"
            }>{job.status}</span>
            {job.current_stage && <> · stage <span className="text-accent">{job.current_stage}</span></>}
            {job.exit_code != null && <> · exit {job.exit_code}</>}
          </span>
        )}
      </div>

      {(running || lines.length > 0) && (
        <pre className="bg-black/60 border border-border rounded p-3 text-xs font-mono overflow-x-auto whitespace-pre"
             style={{ maxHeight: 400, overflowY: "auto" }}>
          {lines.join("\n") || "(waiting for output…)"}
        </pre>
      )}
    </div>
  );
}

// ── Subcomponents ──────────────────────────────────────────────────────────

function Card({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <section className="border border-border rounded-lg p-4 bg-panel/40">
      <h2 className="font-semibold">{title}</h2>
      {subtitle && <p className="text-xs text-muted mt-0.5 mb-3">{subtitle}</p>}
      {children}
    </section>
  );
}

function envVarFor(prov: ProviderEntry): string | null {
  for (const k of Object.keys(prov)) {
    if (k.endsWith("_env") && typeof prov[k] === "string") {
      return prov[k] as string;
    }
  }
  return null;
}

function ProviderTable({
  providers,
  envPresent,
  highlight,
  showTest = false,
  tests,
  onTest,
}: {
  providers: Record<string, ProviderEntry>;
  envPresent: Record<string, boolean>;
  highlight: string;
  showTest?: boolean;
  tests?: Record<string, DbTestResult & { provider: string; pending?: boolean }>;
  onTest?: (provider: string) => void;
}) {
  return (
    <div className="border border-border rounded divide-y divide-border bg-panel/60">
      {Object.entries(providers).map(([name, prov]) => {
        const envName = envVarFor(prov);
        const credOk = envName == null ? null : !!envPresent[envName];
        const isPrimary = name === highlight;
        return (
          <div key={name} className="px-3 py-2 text-xs flex items-center gap-3 flex-wrap">
            <span className="w-2 h-2 rounded-full inline-block flex-shrink-0"
                  style={{ background: credOk == null ? "transparent" :
                                       credOk ? "#5dd2c2" : "#e57373" }}
                  title={envName ? `env ${envName} ${credOk ? "present" : "MISSING"}` : "no credential needed"} />
            <span className={`font-mono ${isPrimary ? "text-accent font-semibold" : ""}`}>{name}</span>
            <span className="text-muted">kind={String(prov.kind)}</span>
            {(["model", "deployment"] as const).map((k) =>
              prov[k] ? <span key={k} className="text-muted">{k}={String(prov[k])}</span> : null,
            )}
            {prov.dim ? <span className="text-muted">dim={String(prov.dim)}</span> : null}
            {envName && <span className="text-muted">env={envName}</span>}
            {showTest && onTest && (
              <button
                onClick={() => onTest(name)}
                disabled={tests?.[name]?.pending}
                className="ml-auto border border-border rounded px-2 py-0.5 hover:border-accent hover:text-accent disabled:opacity-50"
              >
                {tests?.[name]?.pending ? "Testing…" : "Test"}
              </button>
            )}
            {showTest && tests?.[name] && !tests[name].pending && (
              <TestResultLine result={tests[name]} compact />
            )}
          </div>
        );
      })}
    </div>
  );
}

function ProviderSelect({
  providers,
  value,
  disabled,
  onChange,
}: {
  providers: Record<string, ProviderEntry>;
  value: string;
  disabled?: boolean;
  onChange: (next: string) => void;
}) {
  return (
    <select
      value={value}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
      className="border border-border bg-panel rounded px-2 py-0.5 text-xs font-mono text-accent focus:outline-none focus:border-accent disabled:opacity-50"
    >
      {Object.keys(providers).map((name) => (
        <option key={name} value={name}>{name}</option>
      ))}
    </select>
  );
}

function TestResultLine({ result, compact = false }: {
  result?: DbTestResult & { pending?: boolean };
  compact?: boolean;
}) {
  if (!result || result.pending) return null;
  if (result.ok) {
    const v = result.server_version ? ` — ${result.server_version.slice(0, 80)}…` : "";
    return (
      <span className={`text-xs text-emerald-400 ${compact ? "" : "ml-2"}`} title={result.server_version || ""}>
        ✓ {result.elapsed_ms?.toFixed(0)}ms{compact ? "" : v}
      </span>
    );
  }
  return (
    <span className="text-xs text-red-400" title={result.error || ""}>
      ✗ {(result.error || "").slice(0, 80)}
    </span>
  );
}
