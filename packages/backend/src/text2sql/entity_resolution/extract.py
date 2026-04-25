"""Extract candidate phrases from a NL query.

We don't try fancy NER. We pull noun-like fragments: capitalized phrases,
quoted strings, and tokens that look like values (descriptor codes, hyphenated
abbreviations, alphanumeric IDs). Anything we can't classify we still hand to
the resolver — the funnel will discard noise.
"""

from __future__ import annotations

import re

# Patterns we treat as candidates:
#  - quoted: "..." or '...'
#  - Capitalized words (Hispanic, Algebra), incl. multi-word "Title I", "Pre-K"
#  - Mixed-case codes (M-DCPS, ELA, IEP)
_QUOTED = re.compile(r'"([^"]+)"|\'([^\']+)\'')
_CAP_WORD = re.compile(
    r"\b(?:[A-Z][a-zA-Z\-]+(?:\s+[A-Z][a-zA-Z\-]+)*(?:\s+I{1,3}|\s+IV|\s+V)?)\b"
)
# Hyphenated codes / acronyms: Pre-K, ELA, IEP, M-DCPS
_CODE = re.compile(r"\b[A-Z][A-Z0-9\-]{1,12}\b")

_LOWER_STOPS = frozenset({
    "what", "list", "show", "find", "get", "how", "many", "all", "the", "and",
    "for", "with", "from", "in", "on", "of", "by", "to", "at", "is", "are",
    "was", "were", "an", "a",
})


def extract_phrases(query: str) -> list[str]:
    """Return distinct candidate phrases from the NL query, ordered by
    appearance. Heuristic — never empty unless the query is empty."""
    if not query:
        return []
    seen: list[str] = []
    seen_set: set[str] = set()

    def add(p: str) -> None:
        p = p.strip().strip(",.;:!?")
        if not p:
            return
        if p.lower() in _LOWER_STOPS:
            return
        if p.lower() in seen_set:
            return
        seen.append(p)
        seen_set.add(p.lower())

    for m in _QUOTED.finditer(query):
        add(m.group(1) or m.group(2))
    for m in _CAP_WORD.finditer(query):
        add(m.group(0))
    for m in _CODE.finditer(query):
        add(m.group(0))
    return seen
