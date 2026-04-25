"""The four resolution tiers.

Each tier returns a list[(record, score)] where score ∈ [0,1], higher is
better. The resolver short-circuits on the first tier that meets a confidence
threshold; otherwise it keeps escalating.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
from rapidfuzz import fuzz, process

from text2sql.entity_resolution.value_index import ValueIndex, ValueRecord
from text2sql.providers.base import EmbeddingProvider, LLMMessage, LLMProvider, VectorStore


# ── Tier 1: exact match ───────────────────────────────────────────────────────


def tier1_exact(query: str, index: ValueIndex) -> list[tuple[ValueRecord, float]]:
    return [(r, 1.0) for r in index.exact(query)]


# ── Tier 2: rapidfuzz trigram + Metaphone ─────────────────────────────────────


def tier2_fuzzy(
    query: str,
    index: ValueIndex,
    *,
    candidates: Iterable[ValueRecord] | None = None,
    limit: int = 12,
    similarity_floor: float = 60.0,
    min_length_ratio: float = 0.4,
) -> list[tuple[ValueRecord, float]]:
    pool = list(candidates) if candidates is not None else index.records
    if not pool:
        return []
    # Length-ratio guard: drop candidates that are dramatically shorter than
    # the query — they cause false positives via partial-string matching
    # ("ber" against "FlibbertyJibbet"). The floor is max(3 chars, 40% of query).
    q_len = len(query)
    min_cand_len = max(3, int(q_len * min_length_ratio))
    pool = [r for r in pool if len(r.value) >= min_cand_len]
    if not pool:
        return []
    choices = [r.value for r in pool]
    raw = process.extract(
        query, choices, scorer=fuzz.WRatio, limit=limit, score_cutoff=similarity_floor
    )
    seen: dict[tuple[str, str, str], tuple[ValueRecord, float]] = {}
    for choice, score, idx in raw:
        rec = pool[idx]
        key = (rec.fqn, rec.column, rec.value)
        new_s = float(score) / 100.0
        if key not in seen or seen[key][1] < new_s:
            seen[key] = (rec, new_s)
    # Phonetic boost: if Metaphone codes match, lift the score by 0.05.
    try:
        from metaphone import doublemetaphone
        q_meta = doublemetaphone(query)
        out: list[tuple[ValueRecord, float]] = []
        for rec, s in seen.values():
            v_meta = doublemetaphone(rec.value)
            if any(q in v_meta for q in q_meta if q) or any(v in q_meta for v in v_meta if v):
                s = min(1.0, s + 0.05)
            out.append((rec, s))
        out.sort(key=lambda kv: kv[1], reverse=True)
        return out
    except Exception:
        return sorted(seen.values(), key=lambda kv: kv[1], reverse=True)


# ── Tier 3: Vector ANN over column_values collection ──────────────────────────


def tier3_vector(
    query: str,
    embedder: EmbeddingProvider,
    store: VectorStore,
    *,
    column_scope: list[tuple[str, str]] | None = None,
    domain_scope: list[str] | None = None,
    k: int = 8,
) -> list[tuple[ValueRecord, float]]:
    vec = embedder.embed([query], kind="query")[0]
    filters: dict | None = None
    if column_scope:
        filters = {"fqn": [f for f, _ in column_scope]}
    elif domain_scope:
        filters = {"domains": list(domain_scope)}
    hits = store.search("column_values", vec, k=k, filters=filters)
    out: list[tuple[ValueRecord, float]] = []
    for h in hits:
        rec = ValueRecord(
            fqn=h.payload.get("fqn", ""),
            column=h.payload.get("column", ""),
            value=h.payload.get("value", ""),
            domains=tuple(h.payload.get("domains", [])),
            is_descriptor=False,
        )
        # If column_scope was set, drop hits whose column doesn't match.
        if column_scope and (rec.fqn, rec.column) not in set(column_scope):
            continue
        out.append((rec, float(h.score)))
    return out


# ── Tier 4: LLM disambiguation ────────────────────────────────────────────────


_LLM_SYSTEM = (
    "Pick the single best (table, column, value) that the user phrase refers to "
    "from the provided candidates. Reply with JSON only: "
    '{"index": <int>} where index is the chosen candidate position, or '
    '{"index": -1} if none of them fit.'
)


def tier4_llm(
    query: str,
    candidates: list[tuple[ValueRecord, float]],
    llm: LLMProvider,
) -> tuple[ValueRecord, float] | None:
    if not candidates:
        return None
    catalog_block = "\n".join(
        f"  [{i}] {rec.fqn}.{rec.column} = {rec.value!r}"
        for i, (rec, _) in enumerate(candidates)
    )
    user = f"Phrase: {query!r}\n\nCandidates:\n{catalog_block}\n"
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"index": {"type": "integer"}},
        "required": ["index"],
    }
    try:
        raw = llm.complete(
            [
                LLMMessage(role="system", content=_LLM_SYSTEM),
                LLMMessage(role="user", content=user),
            ],
            schema=schema,
            temperature=0.0,
            max_tokens=40,
        )
    except Exception:
        return None
    import json
    idx = int(json.loads(raw)["index"])
    if idx < 0 or idx >= len(candidates):
        return None
    rec, _ = candidates[idx]
    return (rec, 0.95)
