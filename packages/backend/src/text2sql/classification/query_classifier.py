"""Sub-component 2b: query domain classifier (runtime, LLM-driven).

Maps a user's natural-language question to a ranked list of domains:
  primary (required) > secondary (optional) > tertiary (optional)

Drives the cluster-routing step of the §9 pipeline so schema linking only sees
tables in the relevant domains.

Latency budget: ~150-400 ms (one LLM call). Cached by hash(question + catalog).
On LLM error or empty / invalid output, we fall back to the catalog's first
`fallback_top_k` domains so downstream layers always get *something* routable.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from text2sql.classification.catalog import DomainCatalog
from text2sql.providers.base import LLMMessage, LLMProvider

log = logging.getLogger(__name__)


@dataclass
class QueryClassification:
    query: str
    domains: list[str]            # ranked, primary first; 1-3 entries
    reasoning: str
    source: str                   # "llm" | "cache" | "fallback"
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def primary(self) -> str | None:
        return self.domains[0] if self.domains else None

    @property
    def secondary(self) -> str | None:
        return self.domains[1] if len(self.domains) > 1 else None

    @property
    def tertiary(self) -> str | None:
        return self.domains[2] if len(self.domains) > 2 else None

    def top(self, k: int = 3) -> list[str]:
        return list(self.domains[:k])


_QUERY_SYSTEM = """\
You map an end-user's natural-language question against a database to the
domain(s) of that database it concerns.

Return:
  - primary_domain (REQUIRED): the single most-relevant domain.
  - secondary_domain (OPTIONAL, may be null): a clearly second-relevant domain.
  - tertiary_domain (OPTIONAL, may be null): a third domain, only if the
    question genuinely spans three areas.

Use ONLY the provided domain names — never invent. Order matters: primary is
the most relevant, then secondary, then tertiary. Set secondary/tertiary to
null when the question only concerns one or two domains.
"""


_RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "primary_domain": {"type": "string"},
        "secondary_domain": {"type": ["string", "null"]},
        "tertiary_domain": {"type": ["string", "null"]},
        "reasoning": {"type": "string"},
    },
    "required": ["primary_domain", "secondary_domain", "tertiary_domain", "reasoning"],
}


def _format_catalog(catalog: DomainCatalog) -> str:
    return "\n".join(f"- {d.name}: {d.description}" for d in catalog.domains)


def _cache_key(query: str, catalog_names: list[str]) -> str:
    payload = {"q": query.strip().lower(), "c": sorted(catalog_names)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class _Cache:
    """Tiny disk-backed cache; cap at 5000 entries."""
    _MAX = 5000

    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._data: dict[str, dict[str, Any]] = {}
        if path and path.exists():
            try:
                self._data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}

    def get(self, key: str) -> dict[str, Any] | None:
        return self._data.get(key)

    def put(self, key: str, value: dict[str, Any]) -> None:
        self._data[key] = value
        if len(self._data) > self._MAX:
            for k in list(self._data.keys())[: self._MAX // 10]:
                self._data.pop(k, None)
        self._flush()

    def _flush(self) -> None:
        if not self._path:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, sort_keys=True), encoding="utf-8")


class QueryDomainClassifier:
    def __init__(
        self,
        llm: LLMProvider,
        catalog: DomainCatalog,
        *,
        cache_path: Path | None = None,
        fallback_top_k: int = 3,
    ) -> None:
        self._llm = llm
        self._catalog = catalog
        self._cache = _Cache(cache_path)
        self._fallback_top_k = fallback_top_k
        self._catalog_block = _format_catalog(catalog)
        self._valid_names = {d.name for d in catalog.domains}

    def classify(self, query: str) -> QueryClassification:
        query = query.strip()
        if not query:
            return QueryClassification(query=query, domains=[], reasoning="", source="fallback")

        key = _cache_key(query, self._catalog.names())
        cached = self._cache.get(key)
        if cached:
            return self._from_payload(query, cached, source="cache")

        try:
            raw = self._llm.complete(
                [
                    LLMMessage(role="system", content=_QUERY_SYSTEM),
                    LLMMessage(
                        role="user",
                        content=f"DOMAINS:\n{self._catalog_block}\n\nQUESTION: {query}",
                    ),
                ],
                schema=_RESPONSE_SCHEMA,
                temperature=0.0,
                max_tokens=200,
            )
        except Exception as e:
            log.warning("query classification failed: %s", e)
            return self._fallback(query, reasoning=f"llm error: {e!r}")
        # strict json_schema → trust the parse
        payload = json.loads(raw)

        domains = self._validate(payload)
        if not domains:
            return self._fallback(query, reasoning=payload.get("reasoning", ""))

        result_payload = {
            "primary_domain": domains[0],
            "secondary_domain": domains[1] if len(domains) > 1 else None,
            "tertiary_domain": domains[2] if len(domains) > 2 else None,
            "reasoning": payload.get("reasoning", ""),
        }
        self._cache.put(key, result_payload)
        return QueryClassification(
            query=query,
            domains=domains,
            reasoning=payload.get("reasoning", ""),
            source="llm",
            raw=payload,
        )

    def _validate(self, payload: dict[str, Any]) -> list[str]:
        out: list[str] = []
        for key in ("primary_domain", "secondary_domain", "tertiary_domain"):
            d = payload.get(key)
            if isinstance(d, str) and d in self._valid_names and d not in out:
                out.append(d)
        return out

    def _from_payload(
        self, query: str, payload: dict[str, Any], *, source: str
    ) -> QueryClassification:
        domains = self._validate(payload)
        return QueryClassification(
            query=query,
            domains=domains,
            reasoning=payload.get("reasoning", ""),
            source=source,
            raw=payload,
        )

    def _fallback(self, query: str, *, reasoning: str) -> QueryClassification:
        names = [d.name for d in self._catalog.domains[: self._fallback_top_k]]
        return QueryClassification(
            query=query,
            domains=names,
            reasoning=reasoning or "low-confidence; fell back to catalog defaults",
            source="fallback",
        )
