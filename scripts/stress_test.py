"""Drive the live API with the tough-questions suite, summarize per-query
behavior, and emit a markdown report so we can see exactly where the LLM
struggles or shines.

Usage:
    .venv/bin/python scripts/stress_test.py --base http://127.0.0.1:8011
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import httpx
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def run(base: str, queries: list[dict], out_md: Path, out_json: Path) -> None:
    rows: list[dict] = []
    with httpx.Client(timeout=120.0) as c:
        for i, spec in enumerate(queries, 1):
            q = spec["q"]
            qid = spec.get("id", f"q{i}")
            print(f"[{i}/{len(queries)}] {qid}: {q!r}")
            t0 = time.perf_counter()
            try:
                resp = c.post(f"{base}/query", json={"question": q, "max_rows": 50})
                resp.raise_for_status()
                payload = resp.json()
            except Exception as e:
                rows.append({
                    "id": qid, "q": q, "error": str(e),
                    "elapsed_ms": (time.perf_counter() - t0) * 1000,
                })
                print(f"  ✗ HTTP error: {e}")
                continue
            elapsed = (time.perf_counter() - t0) * 1000
            rows.append({
                "id": qid,
                "q": q,
                "validated": payload["validated"],
                "executed": payload["executed"],
                "row_count": payload["row_count"],
                "domains": (payload["domains"] or {}).get("domains") or [],
                "selected_tables": payload["selected_tables"][:8],
                "join_nodes": (payload.get("join_tree") or {}).get("nodes") or [],
                "join_edges": (payload.get("join_tree") or {}).get("edge_count"),
                "resolved_count": len(payload["resolved_entities"]),
                "few_shot_count": payload["few_shot_count"],
                "repair_attempts": len(payload["repair_attempts"]),
                "sql": payload["sql"],
                "rationale": payload["rationale"],
                "description": payload["description"],
                "viz_kind": (payload.get("viz") or {}).get("kind"),
                "error": payload["error"],
                "timings_ms": payload["timings_ms"],
                "elapsed_ms": elapsed,
            })
            ok = "✓" if (payload["validated"] and payload["executed"]) else "✗"
            print(
                f"  {ok} validated={payload['validated']} "
                f"executed={payload['executed']} rows={payload['row_count']} "
                f"join_nodes={len((payload.get('join_tree') or {}).get('nodes') or [])} "
                f"repairs={len(payload['repair_attempts']) - 1} "
                f"({elapsed:.0f} ms)"
            )

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(rows, indent=2, default=str))
    write_markdown(rows, out_md)
    print(f"\nReports:\n  {out_json}\n  {out_md}")


def write_markdown(rows: list[dict], path: Path) -> None:
    n = len(rows)
    n_validated = sum(1 for r in rows if r.get("validated"))
    n_executed = sum(1 for r in rows if r.get("executed"))
    n_repaired = sum(1 for r in rows if (r.get("repair_attempts") or 0) > 1)
    n_zero_rows = sum(1 for r in rows if r.get("row_count") == 0)
    avg_ms = sum(r.get("elapsed_ms", 0) for r in rows) / max(1, n)

    lines: list[str] = [
        "# Tough-questions stress run",
        "",
        f"- queries: **{n}**",
        f"- SQL valid: **{n_validated}/{n}**",
        f"- executed:  **{n_executed}/{n}**",
        f"- needed repair: **{n_repaired}/{n}**",
        f"- returned 0 rows: **{n_zero_rows}/{n}**",
        f"- avg wall-clock: **{avg_ms:.0f} ms**",
        "",
    ]
    for r in rows:
        ok = "✅" if (r.get("validated") and r.get("executed")) else "❌"
        lines.append(f"## {ok} `{r['id']}`")
        lines.append(f"> _{r['q']}_")
        lines.append("")
        lines.append(
            f"**validated** {r.get('validated')} · "
            f"**executed** {r.get('executed')} · "
            f"**rows** {r.get('row_count')} · "
            f"**join nodes** {len(r.get('join_nodes') or [])} · "
            f"**repairs** {(r.get('repair_attempts') or 0) - 1} · "
            f"**{r.get('elapsed_ms', 0):.0f} ms**"
        )
        if r.get("domains"):
            lines.append(f"- domains: {' · '.join(r['domains'])}")
        if r.get("selected_tables"):
            lines.append(f"- top tables: {' · '.join(r['selected_tables'][:5])}")
        if r.get("error"):
            lines.append(f"- **error**: `{r['error']}`")
        if r.get("rationale"):
            lines.append(f"- rationale: _{r['rationale']}_")
        if r.get("description"):
            lines.append(f"- description: _{r['description']}_")
        if r.get("sql"):
            lines.append("```sql")
            lines.append(r["sql"])
            lines.append("```")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8011")
    ap.add_argument(
        "--questions",
        default=str(REPO_ROOT / "data/eval/tough_questions.yaml"),
    )
    ap.add_argument("--out-json", default=str(REPO_ROOT / "data/eval/runs/tough.json"))
    ap.add_argument("--out-md", default=str(REPO_ROOT / "data/eval/runs/tough.md"))
    args = ap.parse_args()

    spec = yaml.safe_load(Path(args.questions).read_text())
    if not spec.get("queries"):
        print("no queries in YAML", file=sys.stderr)
        sys.exit(1)
    run(args.base, spec["queries"], Path(args.out_md), Path(args.out_json))


if __name__ == "__main__":
    main()
