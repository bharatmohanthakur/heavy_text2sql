"use client";

import { useEffect, useState } from "react";
import { AdminConfig, JobStatus, api, streamJob } from "@/lib/api";

/* Settings — three connector forms (Database, LLM, Embedding) plus a
 * Rebuild panel. Each form takes the kind, the credential fields, runs
 * a Test, and on Save registers a new provider entry and (optionally)
 * sets it as the primary for that section.
 *
 * Secrets (passwords, API keys) entered via the UI persist in
 * data/artifacts/runtime_secrets.json, gitignored. They never go in
 * the YAML and never leave the box. The /admin/config GET still
 * redacts them on the wire. */

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "/api";

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`);
  return r.json();
}

export default function SettingsPage() {
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [error, setError] = useState<string | null>(null);

  function refresh() {
    api.adminConfig().then(setConfig).catch((e) => setError(String(e)));
  }
  useEffect(refresh, []);

  if (error) return <div className="text-red-400 text-sm">Failed to load /admin/config: {error}</div>;
  if (!config) return <div className="text-muted text-sm">Loading…</div>;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold text-accent">Settings</h1>
        <p className="text-sm text-muted mt-1">
          Connect a database, an LLM, or an embedding model. Test before
          saving. Credentials persist locally and are never written to YAML.
        </p>
      </header>

      <div className="border border-border rounded-lg p-4 bg-panel/40">
        <div className="text-xs text-muted">Active selections</div>
        <div className="mt-2 grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
          <ActiveBadge label="Target DB" value={config.target_db.primary} />
          <ActiveBadge label="LLM" value={config.llm.primary} />
          <ActiveBadge label="Embedding" value={config.embeddings.primary} />
          <ActiveBadge label="Vector store" value={config.vector_store.primary} />
        </div>
      </div>

      <DatabaseConnectorForm config={config} onSaved={refresh} />
      <LLMConnectorForm config={config} onSaved={refresh} />
      <EmbeddingConnectorForm config={config} onSaved={refresh} />
      <RebuildPanel />

      <details className="text-xs text-muted">
        <summary className="cursor-pointer">Show registered providers (read-only)</summary>
        <div className="mt-3 space-y-3">
          <ProviderList title="Target databases" providers={config.target_db.providers} primary={config.target_db.primary} />
          <ProviderList title="LLMs" providers={config.llm.providers} primary={config.llm.primary} />
          <ProviderList title="Embeddings" providers={config.embeddings.providers} primary={config.embeddings.primary} />
        </div>
      </details>
    </div>
  );
}

// ── Active selections row ───────────────────────────────────────────────

function ActiveBadge({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs text-muted">{label}</div>
      <div className="font-mono text-sm text-accent truncate">{value}</div>
    </div>
  );
}

// ── Database connector ──────────────────────────────────────────────────

type TestState = { pending: boolean; ok?: boolean; error?: string; elapsed_ms?: number; detail?: string };

function DatabaseConnectorForm({ config, onSaved }: { config: AdminConfig; onSaved: () => void }) {
  const [name, setName] = useState("my-ods");
  const [kind, setKind] = useState<"postgresql" | "mssql">("postgresql");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState(5432);
  const [database, setDatabase] = useState("EdFi_Ods");
  const [user, setUser] = useState("edfi");
  const [password, setPassword] = useState("");
  const [setPrimary, setSetPrimary] = useState(true);
  const [trust, setTrust] = useState(true);
  const [encrypt, setEncrypt] = useState(false);
  const [test, setTest] = useState<TestState>({ pending: false });
  const [save, setSave] = useState<TestState>({ pending: false });

  // Adjust default port when kind changes
  useEffect(() => {
    setPort(kind === "postgresql" ? 5432 : 1433);
  }, [kind]);

  function form() {
    return {
      name, kind, host, port, database, user, password,
      set_primary: setPrimary,
      trust_server_certificate: trust,
      encrypt,
      driver: "pymssql",
      schema_search_path: ["edfi", "tpdm"],
    };
  }

  async function runTest() {
    setTest({ pending: true });
    try {
      const r = await postJson<{ ok: boolean; error?: string; elapsed_ms?: number; server_version?: string }>("/admin/connector/database/test", form());
      setTest({ pending: false, ok: r.ok, error: r.error || undefined, elapsed_ms: r.elapsed_ms, detail: r.server_version || undefined });
    } catch (e) {
      setTest({ pending: false, ok: false, error: String(e) });
    }
  }

  async function runSave() {
    setSave({ pending: true });
    try {
      await postJson("/admin/connector/database", form());
      setSave({ pending: false, ok: true, detail: "Saved." });
      onSaved();
    } catch (e) {
      setSave({ pending: false, ok: false, error: String(e) });
    }
  }

  return (
    <FormCard
      title="Connect a database"
      subtitle="The populated ODS the platform queries. Credentials persist to runtime_secrets.json (gitignored)."
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <Field label="Connection name">
          <input className={inputCls} value={name} onChange={(e) => setName(e.target.value)} />
        </Field>
        <Field label="Type">
          <select className={inputCls} value={kind} onChange={(e) => setKind(e.target.value as "postgresql" | "mssql")}>
            <option value="postgresql">PostgreSQL</option>
            <option value="mssql">MSSQL Server / Azure SQL</option>
          </select>
        </Field>
        <Field label="Host">
          <input className={inputCls} value={host} onChange={(e) => setHost(e.target.value)} placeholder="127.0.0.1" />
        </Field>
        <Field label="Port">
          <input className={inputCls} type="number" value={port} onChange={(e) => setPort(Number(e.target.value))} />
        </Field>
        <Field label="Database">
          <input className={inputCls} value={database} onChange={(e) => setDatabase(e.target.value)} />
        </Field>
        <Field label="Username">
          <input className={inputCls} value={user} onChange={(e) => setUser(e.target.value)} />
        </Field>
        <Field label="Password">
          <input className={inputCls} type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="••••••••" />
        </Field>
        <div className="flex items-end gap-3 text-xs text-muted">
          <Toggle label="Set as primary target DB" checked={setPrimary} onChange={setSetPrimary} />
          {kind === "mssql" && (
            <>
              <Toggle label="Trust certificate" checked={trust} onChange={setTrust} />
              <Toggle label="Encrypt" checked={encrypt} onChange={setEncrypt} />
            </>
          )}
        </div>
      </div>

      <FormActions
        onTest={runTest}
        onSave={runSave}
        test={test}
        save={save}
      />
    </FormCard>
  );
}

// ── LLM connector ──────────────────────────────────────────────────────

function LLMConnectorForm({ config, onSaved }: { config: AdminConfig; onSaved: () => void }) {
  const [name, setName] = useState("my-llm");
  const [kind, setKind] = useState<"azure_openai" | "openai" | "anthropic" | "openrouter" | "bedrock">("openai");
  const [apiKey, setApiKey] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [apiVersion, setApiVersion] = useState("2025-03-01-preview");
  const [deployment, setDeployment] = useState("gpt-4o");
  const [model, setModel] = useState("gpt-4o-mini");
  const [region, setRegion] = useState("us-west-2");
  const [maxTokens, setMaxTokens] = useState(4096);
  const [setPrimary, setSetPrimary] = useState(true);
  const [test, setTest] = useState<TestState>({ pending: false });
  const [save, setSave] = useState<TestState>({ pending: false });

  function form() {
    return {
      name, kind, set_primary: setPrimary,
      api_key: apiKey,
      endpoint, api_version: apiVersion, deployment,
      model, region,
      max_tokens: maxTokens, temperature: 0,
    };
  }

  async function runTest() {
    setTest({ pending: true });
    try {
      const r = await postJson<{ ok: boolean; error?: string; elapsed_ms?: number; sample?: string }>("/admin/connector/llm/test", form());
      setTest({ pending: false, ok: r.ok, error: r.error || undefined, elapsed_ms: r.elapsed_ms, detail: r.sample || undefined });
    } catch (e) {
      setTest({ pending: false, ok: false, error: String(e) });
    }
  }

  async function runSave() {
    setSave({ pending: true });
    try {
      await postJson("/admin/connector/llm", form());
      setSave({ pending: false, ok: true, detail: "Saved." });
      onSaved();
    } catch (e) {
      setSave({ pending: false, ok: false, error: String(e) });
    }
  }

  return (
    <FormCard
      title="Connect an LLM"
      subtitle="The model that generates SQL, runs the agent loop, and writes summaries."
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <Field label="Connection name">
          <input className={inputCls} value={name} onChange={(e) => setName(e.target.value)} />
        </Field>
        <Field label="Provider">
          <select className={inputCls} value={kind} onChange={(e) => setKind(e.target.value as typeof kind)}>
            <option value="azure_openai">Azure OpenAI</option>
            <option value="openai">OpenAI</option>
            <option value="anthropic">Anthropic</option>
            <option value="openrouter">OpenRouter</option>
            <option value="bedrock">AWS Bedrock</option>
          </select>
        </Field>
        <Field label="API key" full>
          <input className={inputCls} type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="••••••••" />
        </Field>
        {kind === "azure_openai" && (
          <>
            <Field label="Endpoint" full>
              <input className={inputCls} value={endpoint} onChange={(e) => setEndpoint(e.target.value)} placeholder="https://my-resource.openai.azure.com" />
            </Field>
            <Field label="API version">
              <input className={inputCls} value={apiVersion} onChange={(e) => setApiVersion(e.target.value)} />
            </Field>
            <Field label="Deployment">
              <input className={inputCls} value={deployment} onChange={(e) => setDeployment(e.target.value)} placeholder="gpt-4o" />
            </Field>
          </>
        )}
        {(kind === "openai" || kind === "anthropic" || kind === "openrouter") && (
          <Field label="Model" full>
            <input className={inputCls} value={model} onChange={(e) => setModel(e.target.value)}
              placeholder={kind === "anthropic" ? "claude-sonnet-4-6" : kind === "openrouter" ? "z-ai/glm-5.1" : "gpt-4o-mini"} />
          </Field>
        )}
        {kind === "bedrock" && (
          <>
            <Field label="Model">
              <input className={inputCls} value={model} onChange={(e) => setModel(e.target.value)} placeholder="us.anthropic.claude-sonnet-4-5-20250929-v1:0" />
            </Field>
            <Field label="Region">
              <input className={inputCls} value={region} onChange={(e) => setRegion(e.target.value)} placeholder="us-west-2" />
            </Field>
          </>
        )}
        <Field label="Max tokens">
          <input className={inputCls} type="number" value={maxTokens} onChange={(e) => setMaxTokens(Number(e.target.value))} />
        </Field>
        <div className="flex items-end gap-3 text-xs text-muted">
          <Toggle label="Set as primary LLM" checked={setPrimary} onChange={setSetPrimary} />
        </div>
      </div>

      <FormActions onTest={runTest} onSave={runSave} test={test} save={save} />
    </FormCard>
  );
}

// ── Embedding connector ────────────────────────────────────────────────

function EmbeddingConnectorForm({ config, onSaved }: { config: AdminConfig; onSaved: () => void }) {
  const [name, setName] = useState("my-embedding");
  const [kind, setKind] = useState<"azure_openai" | "openai" | "sentence_transformers" | "bedrock">("sentence_transformers");
  const [apiKey, setApiKey] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [apiVersion, setApiVersion] = useState("2025-03-01-preview");
  const [deployment, setDeployment] = useState("text-embedding-3-large");
  const [model, setModel] = useState("BAAI/bge-large-en-v1.5");
  const [device, setDevice] = useState<"cpu" | "cuda" | "mps">("cpu");
  const [region, setRegion] = useState("us-west-2");
  const [family, setFamily] = useState<"titan" | "cohere">("titan");
  const [dim, setDim] = useState(1024);
  const [batch, setBatch] = useState(32);
  const [setPrimary, setSetPrimary] = useState(true);
  const [test, setTest] = useState<TestState>({ pending: false });
  const [save, setSave] = useState<TestState>({ pending: false });

  function form() {
    return {
      name, kind, set_primary: setPrimary,
      api_key: apiKey,
      endpoint, api_version: apiVersion, deployment,
      model, device, region, family,
      dim: Number(dim) || 0,
      batch_size: Number(batch) || 32,
    };
  }

  async function runTest() {
    setTest({ pending: true });
    try {
      const r = await postJson<{ ok: boolean; error?: string; elapsed_ms?: number; dim?: number }>("/admin/connector/embedding/test", form());
      setTest({ pending: false, ok: r.ok, error: r.error || undefined, elapsed_ms: r.elapsed_ms, detail: r.dim ? `dim=${r.dim}` : undefined });
    } catch (e) {
      setTest({ pending: false, ok: false, error: String(e) });
    }
  }
  async function runSave() {
    setSave({ pending: true });
    try {
      await postJson("/admin/connector/embedding", form());
      setSave({ pending: false, ok: true, detail: "Saved. Re-index recommended." });
      onSaved();
    } catch (e) {
      setSave({ pending: false, ok: false, error: String(e) });
    }
  }

  return (
    <FormCard
      title="Connect an embedding model"
      subtitle="Powers table retrieval, gold few-shots, and the entity resolver. Local models run offline."
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <Field label="Connection name">
          <input className={inputCls} value={name} onChange={(e) => setName(e.target.value)} />
        </Field>
        <Field label="Type">
          <select className={inputCls} value={kind} onChange={(e) => setKind(e.target.value as typeof kind)}>
            <option value="sentence_transformers">Local (Hugging Face / sentence-transformers)</option>
            <option value="azure_openai">Azure OpenAI</option>
            <option value="openai">OpenAI</option>
            <option value="bedrock">AWS Bedrock</option>
          </select>
        </Field>

        {kind === "azure_openai" && (
          <>
            <Field label="API key" full>
              <input className={inputCls} type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} />
            </Field>
            <Field label="Endpoint" full>
              <input className={inputCls} value={endpoint} onChange={(e) => setEndpoint(e.target.value)} />
            </Field>
            <Field label="API version">
              <input className={inputCls} value={apiVersion} onChange={(e) => setApiVersion(e.target.value)} />
            </Field>
            <Field label="Deployment">
              <input className={inputCls} value={deployment} onChange={(e) => setDeployment(e.target.value)} />
            </Field>
          </>
        )}
        {kind === "openai" && (
          <>
            <Field label="API key" full>
              <input className={inputCls} type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} />
            </Field>
            <Field label="Model" full>
              <input className={inputCls} value={model} onChange={(e) => setModel(e.target.value)} placeholder="text-embedding-3-large" />
            </Field>
          </>
        )}
        {kind === "sentence_transformers" && (
          <>
            <Field label="Hugging Face model" full>
              <input className={inputCls} value={model} onChange={(e) => setModel(e.target.value)} placeholder="BAAI/bge-large-en-v1.5" />
            </Field>
            <Field label="Device">
              <select className={inputCls} value={device} onChange={(e) => setDevice(e.target.value as typeof device)}>
                <option value="cpu">CPU</option>
                <option value="cuda">GPU (CUDA)</option>
                <option value="mps">Apple Silicon (MPS)</option>
              </select>
            </Field>
          </>
        )}
        {kind === "bedrock" && (
          <>
            <Field label="API key" full>
              <input className={inputCls} type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} />
            </Field>
            <Field label="Model">
              <input className={inputCls} value={model} onChange={(e) => setModel(e.target.value)} placeholder="amazon.titan-embed-text-v2:0" />
            </Field>
            <Field label="Region">
              <input className={inputCls} value={region} onChange={(e) => setRegion(e.target.value)} />
            </Field>
            <Field label="Family">
              <select className={inputCls} value={family} onChange={(e) => setFamily(e.target.value as typeof family)}>
                <option value="titan">Titan</option>
                <option value="cohere">Cohere</option>
              </select>
            </Field>
          </>
        )}

        <Field label="Dimension (optional)">
          <input className={inputCls} type="number" value={dim} onChange={(e) => setDim(Number(e.target.value))} />
        </Field>
        <Field label="Batch size">
          <input className={inputCls} type="number" value={batch} onChange={(e) => setBatch(Number(e.target.value))} />
        </Field>

        <div className="flex items-end gap-3 text-xs text-muted">
          <Toggle label="Set as primary embedding model" checked={setPrimary} onChange={setSetPrimary} />
        </div>
      </div>

      <FormActions onTest={runTest} onSave={runSave} test={test} save={save} />
    </FormCard>
  );
}

// ── Rebuild panel (unchanged from K4) ──────────────────────────────────

const ALL_STAGES: { id: string; label: string; help: string }[] = [
  { id: "ingest",    label: "Ingest Ed-Fi metadata",  help: "Fetch ApiModel.json + ForeignKeys.sql." },
  { id: "classify",  label: "Classify tables",         help: "Map tables → Ed-Fi domains." },
  { id: "graph",     label: "FK graph + APSP",         help: "Build the rustworkx graph + dist/next-hop." },
  { id: "catalog",   label: "Table catalog (LLM)",     help: "One-time, ~5 min. Uses catalog_description LLM slot." },
  { id: "index",     label: "Embed + FAISS index",     help: "Re-embed catalog with the current embedding provider." },
  { id: "gold-seed", label: "Gold few-shot seed",      help: "Load gold_queries_bootstrap.yaml, exec-validate against the live DB." },
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
        if (ev.type === "line") setLines((l) => [...l, ev.line]);
        else if (ev.type === "status") setJob({ ...ev });
      })
        .catch((e) => setLines((l) => [...l, `[stream error] ${e}`]))
        .finally(() => setRunning(false));
    } catch (e) {
      setLines((l) => [...l, `[start failed] ${e instanceof Error ? e.message : String(e)}`]);
      setRunning(false);
    }
  }

  return (
    <FormCard title="Rebuild" subtitle="Run any subset of build stages. Output streams here.">
      <div className="grid grid-cols-2 gap-2 mb-3">
        {ALL_STAGES.map((s) => (
          <label key={s.id} className="flex items-start gap-2 text-xs cursor-pointer">
            <input type="checkbox" checked={selected.has(s.id)} disabled={running}
                   onChange={() => toggle(s.id)} className="mt-0.5" />
            <span>
              <span className="font-mono text-accent">{s.label}</span>
              <span className="block text-muted">{s.help}</span>
            </span>
          </label>
        ))}
      </div>

      <div className="flex items-center gap-3">
        <button onClick={start} disabled={running || selected.size === 0}
                className="border border-accent text-accent rounded px-3 py-1 text-sm disabled:opacity-50">
          {running ? "Running…" : `Run ${selected.size} stage${selected.size === 1 ? "" : "s"}`}
        </button>
        {job && (
          <span className="text-xs text-muted">
            job <span className="font-mono">{job.id.slice(0, 8)}</span>
            {" — "}
            <span className={job.status === "succeeded" ? "text-emerald-400" : job.status === "failed" ? "text-red-400" : "text-accent"}>
              {job.status}
            </span>
            {job.current_stage && <> · stage <span className="text-accent">{job.current_stage}</span></>}
            {job.exit_code != null && <> · exit {job.exit_code}</>}
          </span>
        )}
      </div>

      {(running || lines.length > 0) && (
        <pre className="bg-black/60 border border-border rounded p-3 mt-3 text-xs font-mono overflow-x-auto whitespace-pre"
             style={{ maxHeight: 400, overflowY: "auto" }}>
          {lines.join("\n") || "(waiting for output…)"}
        </pre>
      )}
    </FormCard>
  );
}

// ── Shared form components ─────────────────────────────────────────────

const inputCls =
  "w-full border border-border bg-panel rounded px-2 py-1.5 text-sm focus:outline-none focus:border-accent disabled:opacity-50";

function FormCard({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <section className="border border-border rounded-lg p-4 bg-panel/40">
      <h2 className="font-semibold">{title}</h2>
      {subtitle && <p className="text-xs text-muted mt-0.5 mb-3">{subtitle}</p>}
      {children}
    </section>
  );
}

function Field({ label, full, children }: { label: string; full?: boolean; children: React.ReactNode }) {
  return (
    <label className={`block ${full ? "md:col-span-2" : ""}`}>
      <span className="text-xs text-muted">{label}</span>
      {children}
    </label>
  );
}

function Toggle({ label, checked, onChange }: { label: string; checked: boolean; onChange: (b: boolean) => void }) {
  return (
    <label className="inline-flex items-center gap-1.5 cursor-pointer">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span>{label}</span>
    </label>
  );
}

function FormActions({ onTest, onSave, test, save }: {
  onTest: () => void; onSave: () => void;
  test: TestState; save: TestState;
}) {
  return (
    <div className="mt-4 flex flex-wrap items-center gap-3">
      <button onClick={onTest} disabled={test.pending}
              className="border border-border rounded px-3 py-1 text-sm hover:border-accent hover:text-accent disabled:opacity-50">
        {test.pending ? "Testing…" : "Test connection"}
      </button>
      <button onClick={onSave} disabled={save.pending}
              className="border border-accent text-accent rounded px-3 py-1 text-sm disabled:opacity-50">
        {save.pending ? "Saving…" : "Save"}
      </button>
      <span className="text-xs">
        {test.ok === true && (
          <span className="text-emerald-400" title={test.detail || ""}>
            ✓ connected{test.elapsed_ms != null ? ` · ${test.elapsed_ms.toFixed(0)}ms` : ""}
            {test.detail ? ` · ${test.detail.slice(0, 60)}` : ""}
          </span>
        )}
        {test.ok === false && (
          <span className="text-red-400" title={test.error || ""}>
            ✗ {(test.error || "").slice(0, 100)}
          </span>
        )}
        {save.ok === true && <span className="text-emerald-400 ml-3">{save.detail || "Saved."}</span>}
        {save.ok === false && <span className="text-red-400 ml-3" title={save.error || ""}>✗ {(save.error || "").slice(0, 100)}</span>}
      </span>
    </div>
  );
}

function ProviderList({ title, providers, primary }: {
  title: string; providers: Record<string, unknown>; primary: string;
}) {
  return (
    <div>
      <div className="font-semibold mb-1">{title}</div>
      <div className="border border-border rounded divide-y divide-border bg-panel/60">
        {Object.entries(providers).map(([name, prov]) => (
          <div key={name} className="px-3 py-2 text-xs flex items-center gap-2">
            {name === primary && <span className="text-accent">★</span>}
            <span className="font-mono">{name}</span>
            <span className="text-muted">— kind={String((prov as { kind: string }).kind)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
