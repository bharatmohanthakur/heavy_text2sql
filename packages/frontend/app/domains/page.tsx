"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, DomainScore } from "@/lib/api";

export default function DomainsPage() {
  const [domains, setDomains] = useState<DomainScore[]>([]);
  useEffect(() => {
    api.domains().then((d) => setDomains(d.domains));
  }, []);

  const max = Math.max(...domains.map((d) => d.table_count), 1);

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Domains ({domains.length})</h1>
      <div className="grid grid-cols-2 gap-x-6 gap-y-1">
        {domains.map((d) => (
          <Link
            key={d.name}
            href={`/tables?domain=${encodeURIComponent(d.name)}`}
            className="flex items-center justify-between border-b border-border py-1.5 hover:text-accent text-sm"
          >
            <span>{d.name}</span>
            <span className="flex items-center gap-3 text-xs text-muted">
              <span
                className="bg-accent h-1 inline-block"
                style={{ width: `${(d.table_count / max) * 90 + 10}px` }}
              />
              {d.table_count}
            </span>
          </Link>
        ))}
      </div>
    </div>
  );
}
