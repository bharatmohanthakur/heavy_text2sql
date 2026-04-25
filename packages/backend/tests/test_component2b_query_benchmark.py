"""Component 2b query-classifier benchmark.

Runs the 30-query benchmark from data/eval/query_classifier_benchmark.yaml
against real Azure GPT-4o, measures hit-rate / strict-top1 / forbidden-leakage,
and prints a per-query report.

The classifier emits ranked names only — primary, secondary, tertiary —
no confidence numbers. We grade purely on inclusion in that ordered list.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from text2sql.classification import QueryDomainClassifier, load_domain_catalog
from text2sql.config import REPO_ROOT, load_config
from text2sql.ingestion.edfi_fetcher import IngestionConfig, fetch_all

BENCHMARK_PATH = REPO_ROOT / "data/eval/query_classifier_benchmark.yaml"
HIT_RATE_FLOOR = 0.75            # ≥75% queries: every expected_in_top3 element must appear
TOP1_STRICT_FLOOR = 0.55         # ≥55% queries with expected_top1 → primary matches


@pytest.mark.skipif(not os.environ.get("AZURE_OPENAI_API_KEY"), reason="no azure key")
def test_query_classifier_benchmark(tmp_path: Path) -> None:
    cfg = load_config()
    manifest = fetch_all(IngestionConfig.from_app_config(cfg.ed_fi, REPO_ROOT))
    catalog = load_domain_catalog(manifest)
    valid = {d.name for d in catalog.domains}

    from text2sql.providers import build_llm
    llm = build_llm(cfg.llm_for_task("classifier_fallback"))
    qc = QueryDomainClassifier(llm, catalog, cache_path=tmp_path / "qc.json")

    bench = yaml.safe_load(BENCHMARK_PATH.read_text())["queries"]
    rows: list[dict] = []
    n_top3_hit = n_top3_total = 0
    n_top1_hit = n_top1_total = 0
    forbidden_leaks: list[str] = []
    hallucinations: list[str] = []

    for spec in bench:
        out = qc.classify(spec["q"])
        ranked = out.domains
        top3 = set(ranked[:3])

        for n in ranked:
            if n not in valid:
                hallucinations.append(f"{spec['q']!r} → {n!r}")

        for f in spec.get("forbidden") or []:
            if f in top3:
                forbidden_leaks.append(f"{spec['q']!r} leaked {f!r}")

        expected_in_top3 = spec.get("expected_in_top3") or []
        hit_top3 = None
        if expected_in_top3:
            n_top3_total += 1
            hit_top3 = all(e in top3 for e in expected_in_top3)
            if hit_top3:
                n_top3_hit += 1

        expected_top1 = spec.get("expected_top1")
        hit_top1 = None
        if expected_top1:
            n_top1_total += 1
            hit_top1 = (out.primary == expected_top1)
            if hit_top1:
                n_top1_hit += 1

        rows.append({
            "q": spec["q"],
            "tag": spec.get("tag", ""),
            "ranked": ranked,
            "expected_in_top3": expected_in_top3,
            "expected_top1": expected_top1,
            "hit_top3": hit_top3,
            "hit_top1": hit_top1,
            "source": out.source,
        })

    print("\n" + "=" * 110)
    print(f"Query classifier benchmark — {len(bench)} queries")
    print("=" * 110)
    for r in rows:
        marks = "".join([
            "✓" if r["hit_top3"] else ("✗" if r["hit_top3"] is False else " "),
            "T" if r["hit_top1"] else ("t" if r["hit_top1"] is False else " "),
        ])
        ranked_str = " > ".join(r["ranked"]) if r["ranked"] else "(none)"
        print(f"  [{marks}] {r['tag']:14s} {r['q']!r}")
        print(f"          ranked:   {ranked_str}")
        if r["expected_in_top3"]:
            print(f"          want_in3: {r['expected_in_top3']}")
        if r["expected_top1"]:
            print(f"          want_t1:  {r['expected_top1']}")

    top3_rate = n_top3_hit / n_top3_total if n_top3_total else 1.0
    top1_rate = n_top1_hit / n_top1_total if n_top1_total else 1.0
    print()
    print(f"top-3 hit-rate:    {n_top3_hit}/{n_top3_total} = {top3_rate:.0%}  (floor {HIT_RATE_FLOOR:.0%})")
    print(f"strict top-1 rate: {n_top1_hit}/{n_top1_total} = {top1_rate:.0%}  (floor {TOP1_STRICT_FLOOR:.0%})")
    print(f"forbidden leaks:   {len(forbidden_leaks)}")
    print(f"hallucinations:    {len(hallucinations)}")

    assert not hallucinations, "hallucinated domain names: " + "; ".join(hallucinations)
    assert not forbidden_leaks, "forbidden leakage: " + "; ".join(forbidden_leaks)
    assert top3_rate >= HIT_RATE_FLOOR, f"top-3 hit-rate {top3_rate:.0%} < {HIT_RATE_FLOOR:.0%}"
    assert top1_rate >= TOP1_STRICT_FLOOR, f"strict top-1 rate {top1_rate:.0%} < {TOP1_STRICT_FLOOR:.0%}"
