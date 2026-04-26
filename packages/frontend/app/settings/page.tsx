"use client";

import { useEffect, useState } from "react";
import { AdminConfig, DbTestResult, ProviderEntry, api } from "@/lib/api";

/* Settings page — read-only viewer of the resolved runtime config.
 * Editor cards (LLM routing, embedding switcher) land in K3.
 *
 * The page is one column of cards. Each card shows the section's primary
 * provider, the full list of registered providers (one row each), with
 * a green/red dot signaling whether the credential env var that provider
 * needs is actually populated. The two DB cards have a "Test connection"
 * button that pings the live DB. */

export default function SettingsPage() {
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tests, setTests] = useState<Record<string, DbTestResult & { provider: string; pending?: boolean }>>({});

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

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold text-accent">Settings</h1>
        <p className="text-sm text-muted mt-1">
          Resolved runtime configuration. Read-only for now — editing comes next.
          Secrets stay in <code className="text-accent">.env</code>; everything else
          lives in <code className="text-accent">{config.overlay_path}</code>.
        </p>
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
        <div className="text-sm mb-2">
          <span className="text-muted">primary:</span>{" "}
          <span className="text-accent font-mono">{config.llm.primary}</span>
          {config.llm.fallback && (
            <span className="ml-4">
              <span className="text-muted">fallback:</span>{" "}
              <span className="text-accent font-mono">{config.llm.fallback}</span>
            </span>
          )}
        </div>

        <div className="mb-4">
          <div className="text-xs text-muted uppercase tracking-wide mb-1">Task routing</div>
          <div className="border border-border rounded p-2 bg-panel font-mono text-xs space-y-1">
            {Object.entries(config.llm.task_routing).map(([task, provider]) => (
              <div key={task} className="flex">
                <span className="w-44 text-muted">{task}:</span>
                <span className="text-accent">{provider}</span>
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
        <div className="text-sm mb-2">
          <span className="text-muted">primary:</span>{" "}
          <span className="text-accent font-mono">{config.embeddings.primary}</span>
          {(() => {
            const p = config.embeddings.providers[config.embeddings.primary];
            const dim = p?.dim;
            return dim ? <span className="ml-4 text-muted">dim: <span className="text-accent">{String(dim)}</span></span> : null;
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
