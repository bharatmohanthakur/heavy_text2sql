"use client";

import Link from "next/link";
import { useActiveProvider } from "@/lib/useActiveProvider";

/**
 * Shown sitewide when the backend reports catalog_loaded=false — i.e.
 * a fresh clone where nobody has run the build pipeline yet. Without
 * this, /tables, /domains, /query all return 503s with no obvious next
 * step. With it, the operator gets a clear "click here, then click
 * Rebuild" path.
 */
export function OnboardingBanner() {
  const health = useActiveProvider();
  if (!health) return null;
  if (health.catalog_loaded && health.pipeline_ready) return null;

  return (
    <div className="border border-amber-700 bg-amber-950/40 text-amber-200 rounded-lg px-4 py-3 mb-4 text-sm">
      <div className="font-semibold mb-1">
        Welcome — the platform isn&apos;t bootstrapped yet.
      </div>
      <div className="text-amber-300/80 mb-2">
        {!health.catalog_loaded && <>No table catalog has been built. </>}
        {!health.pipeline_ready && <>The query pipeline is offline. </>}
        Open Settings and run the Rebuild stages (ingest → classify → graph →
        catalog → index → gold-seed) to get going.
      </div>
      <Link
        href="/settings"
        className="inline-block border border-amber-600 text-amber-200 rounded px-3 py-1 hover:bg-amber-900/40"
      >
        Go to Settings →
      </Link>
    </div>
  );
}
