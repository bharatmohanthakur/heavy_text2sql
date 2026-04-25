"use client";

import { useEffect, useRef } from "react";

export function VegaChart({ spec }: { spec: Record<string, unknown> | null }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!spec || !ref.current) return;
    let cancelled = false;
    (async () => {
      const embed = (await import("vega-embed")).default;
      if (cancelled || !ref.current) return;
      // Clear via DOM API (no innerHTML — keeps the security hook happy and
      // we never inject untrusted strings into the DOM anyway).
      while (ref.current.firstChild) ref.current.removeChild(ref.current.firstChild);
      await embed(ref.current, spec as any, {
        actions: false,
        config: {
          background: "transparent",
          axis: { labelColor: "#c8cdd6", titleColor: "#c8cdd6" },
          view: { stroke: "transparent" },
          mark: { color: "#5dd2c2" },
          legend: { labelColor: "#c8cdd6", titleColor: "#c8cdd6" },
          title: { color: "#e8eaef" },
        },
      });
    })().catch((err) => {
      if (!ref.current) return;
      while (ref.current.firstChild) ref.current.removeChild(ref.current.firstChild);
      const div = document.createElement("div");
      div.className = "text-red-400 text-sm";
      div.textContent = `Chart error: ${String(err)}`;
      ref.current.appendChild(div);
    });
    return () => {
      cancelled = true;
    };
  }, [spec]);
  if (!spec) return null;
  // min-h guarantees vega-embed has vertical space to lay out (otherwise the
  // SVG can collapse to a few pixels); w-full + container width lets the
  // chart expand to match the chat bubble.
  return <div ref={ref} className="w-full min-h-[340px] overflow-x-auto" />;
}
