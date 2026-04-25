"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, TableDetail } from "@/lib/api";
import { RowsTable } from "@/components/RowsTable";

export default function TableDetailPage({ params }: { params: { fqn: string } }) {
  const fqn = decodeURIComponent(params.fqn);
  const [t, setT] = useState<TableDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.table(fqn).then(setT).catch((e) => setError(String(e)));
  }, [fqn]);

  if (error) return <div className="text-red-400 text-sm">{error}</div>;
  if (!t) return <div className="text-muted text-sm">Loading…</div>;

  return (
    <div className="space-y-5">
      <div>
        <Link href="/tables" className="text-xs text-muted">← back to tables</Link>
        <h1 className="text-xl font-semibold mt-1">{t.fqn}</h1>
        <p className="text-sm text-muted max-w-3xl mt-1">{t.description}</p>
        <div className="mt-2 flex flex-wrap items-center gap-1">
          {t.domains.map((d) => (
            <span key={d} className="tag">{d}</span>
          ))}
          {t.is_descriptor && <span className="tag">descriptor</span>}
          {t.is_association && <span className="tag">association</span>}
          {t.is_extension && <span className="tag">extension</span>}
        </div>
      </div>

      <div className="border border-border bg-panel rounded p-4">
        <h2 className="text-sm font-semibold text-muted mb-2">
          Columns ({t.columns.length})
          <span className="text-xs ml-2">PK: {t.primary_key.join(", ") || "(none)"}</span>
        </h2>
        <table className="results w-full">
          <thead>
            <tr>
              <th>name</th>
              <th>type</th>
              <th>null</th>
              <th>description</th>
              <th>samples</th>
            </tr>
          </thead>
          <tbody>
            {t.columns.map((c) => (
              <tr key={c.name}>
                <td>
                  {c.name}
                  {c.is_identifying && <span className="tag ml-2">PK</span>}
                </td>
                <td>{c.data_type}</td>
                <td>{c.nullable ? "NULL" : "NOT NULL"}</td>
                <td className="max-w-md text-muted">{c.description}</td>
                <td className="text-muted text-xs">
                  {(c.sample_values || []).slice(0, 4).join(", ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="border border-border bg-panel rounded p-4">
          <h2 className="text-sm font-semibold text-muted mb-2">Parent neighbors</h2>
          <ul className="text-xs space-y-1">
            {t.parent_neighbors.map((p) => (
              <li key={p}>
                <Link href={`/tables/${encodeURIComponent(p)}`} className="hover:underline font-mono">
                  {p}
                </Link>
              </li>
            ))}
            {!t.parent_neighbors.length && <li className="text-muted">(none)</li>}
          </ul>
        </div>
        <div className="border border-border bg-panel rounded p-4">
          <h2 className="text-sm font-semibold text-muted mb-2">Child neighbors</h2>
          <ul className="text-xs space-y-1 max-h-72 overflow-auto">
            {t.child_neighbors.map((c) => (
              <li key={c}>
                <Link href={`/tables/${encodeURIComponent(c)}`} className="hover:underline font-mono">
                  {c}
                </Link>
              </li>
            ))}
            {!t.child_neighbors.length && <li className="text-muted">(none)</li>}
          </ul>
        </div>
      </div>

      {t.sample_rows.length > 0 && (
        <div className="border border-border bg-panel rounded p-4">
          <h2 className="text-sm font-semibold text-muted mb-2">Sample rows</h2>
          <RowsTable rows={t.sample_rows} />
        </div>
      )}
    </div>
  );
}
