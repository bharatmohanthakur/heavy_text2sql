"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, TableSummary, DomainScore } from "@/lib/api";

export default function TablesPage() {
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [domains, setDomains] = useState<DomainScore[]>([]);
  const [filter, setFilter] = useState("");
  const [domainFilter, setDomainFilter] = useState<string>("");
  const [showDescriptors, setShowDescriptors] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.domains().then((d) => setDomains(d.domains)).catch(() => undefined);
  }, []);

  useEffect(() => {
    setLoading(true);
    api
      .tables({
        domain: domainFilter || undefined,
        descriptors: showDescriptors,
        limit: 500,
      })
      .then((r) => setTables(r.tables))
      .finally(() => setLoading(false));
  }, [domainFilter, showDescriptors]);

  const visible = tables.filter((t) =>
    !filter || t.fqn.toLowerCase().includes(filter.toLowerCase()),
  );

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Tables</h1>
      <div className="flex flex-wrap gap-3 items-center">
        <input
          className="bg-panel border border-border rounded px-3 py-1.5 text-sm w-64"
          placeholder="filter by fqn…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        <select
          className="bg-panel border border-border rounded px-3 py-1.5 text-sm"
          value={domainFilter}
          onChange={(e) => setDomainFilter(e.target.value)}
        >
          <option value="">all domains</option>
          {domains.map((d) => (
            <option key={d.name} value={d.name}>
              {d.name} ({d.table_count})
            </option>
          ))}
        </select>
        <label className="text-xs text-muted flex items-center gap-1">
          <input
            type="checkbox"
            checked={showDescriptors}
            onChange={(e) => setShowDescriptors(e.target.checked)}
          />
          include descriptors
        </label>
        <span className="text-xs text-muted ml-auto">
          {loading ? "loading…" : `${visible.length} of ${tables.length}`}
        </span>
      </div>
      <div className="border border-border rounded overflow-hidden">
        <table className="results w-full">
          <thead>
            <tr>
              <th>fqn</th>
              <th>description</th>
              <th>domains</th>
              <th>rows</th>
              <th>cols</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((t) => (
              <tr key={t.fqn}>
                <td>
                  <Link href={`/tables/${encodeURIComponent(t.fqn)}`} className="hover:underline">
                    {t.fqn}
                  </Link>
                  {t.is_descriptor && <span className="tag ml-2">descriptor</span>}
                </td>
                <td className="max-w-md">
                  <span className="text-muted">{t.description.slice(0, 110)}</span>
                </td>
                <td>
                  {t.domains.slice(0, 3).map((d) => (
                    <span key={d} className="tag">
                      {d}
                    </span>
                  ))}
                  {t.domains.length > 3 && <span className="tag">+{t.domains.length - 3}</span>}
                </td>
                <td>{t.row_count ?? "–"}</td>
                <td>{t.column_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
