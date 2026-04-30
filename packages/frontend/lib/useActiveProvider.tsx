"use client";

import { useEffect, useState } from "react";
import { api, HealthResponse } from "./api";

/**
 * Polls /health every 5s to track which target_db provider is currently
 * active. Pages that depend on the catalog (Tables, Domains, table detail)
 * should `useEffect(..., [health.provider_name])` so a Settings switch
 * triggers an automatic refetch — no manual reload required.
 *
 * Returns null while the first request is in flight.
 */
export function useActiveProvider(): HealthResponse | null {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      try {
        const h = await api.health();
        if (alive) setHealth(h);
      } catch {
        // network blip — keep last known good
      }
      if (alive) timer = setTimeout(tick, 5000);
    };
    tick();

    return () => {
      alive = false;
      if (timer) clearTimeout(timer);
    };
  }, []);

  return health;
}

/** A compact one-line summary like "my-sqlite-demo · sqlite · 18 tables". */
export function ActiveProviderBadge({ health }: { health: HealthResponse | null }) {
  if (!health) return <span className="text-xs text-muted">…</span>;
  const { provider_name, target_dialect, tables } = health;
  if (!provider_name) {
    return (
      <span className="text-xs text-muted">
        legacy catalog · {tables} tables
      </span>
    );
  }
  const palette: Record<string, string> = {
    mssql: "border-blue-500 text-blue-400",
    sqlite: "border-emerald-500 text-emerald-400",
    postgresql: "border-cyan-500 text-cyan-400",
  };
  const klass = palette[target_dialect] ?? "border-border text-muted";
  return (
    <span className="text-xs flex items-center gap-2">
      <span className="text-muted">Active:</span>
      <span className="font-mono">{provider_name}</span>
      <span className={`uppercase tracking-wide border rounded px-1 py-px ${klass}`}>
        {target_dialect || "?"}
      </span>
      <span className="text-muted">· {tables} tables</span>
    </span>
  );
}
