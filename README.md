# Ed-Fi Text-to-SQL Platform

Pluggable, production-grade NL→SQL for Ed-Fi ODS. Default schema target: **DS 6.1.0 core (829 tables, 1,663 FKs, 35 domains)**. The full design is in [`edfi_text2sql_plan.md`](./edfi_text2sql_plan.md); this README is the operator quickstart.

> **Note on TPDM:** the public Ed-Fi Extensions repo does not yet publish DS 6.1.0-targeted TPDM artifacts (only DS 4.0.0 / 5.2.0). Run on DS 6.1.0 core today; TPDM lights up automatically once Ed-Fi ships a 6.1.0 artifact set.

## Architecture

```
NL → cluster route → entity resolve → schema link → Steiner join tree
   → context → LLM SQL → validate → execute → viz + describe
```

- **Domain classification:** parsed deterministically from `ApiModel.json` (`entityDefinitions[].domains[]`). 90% of tables resolve directly, ~9% inherit from aggregate roots, ~1% from descriptor-referrer voting. LLM only on residuals.
- **FK graph:** in-process with memory-mapped APSP — sub-millisecond Steiner join paths.
- **Provider abstraction:** LLM (Azure OpenAI / Anthropic / OpenAI / Bedrock / vLLM), embeddings (Azure / OpenAI / BGE-M3 / Bedrock Titan), vector store (FAISS / Qdrant / OpenSearch / Azure AI Search), target SQL engine (Postgres / MSSQL / Snowflake) — all swappable via `configs/default.yaml`.
- **Gold SQL flywheel:** every approved query becomes a few-shot + an eval ground truth.

## Components & Build Status

| # | Component | Status |
|---|---|---|
| 1 | Data ingestion (ApiModel.json + FKs from Ed-Fi GitHub) | ✅ done |
| 2 | Domain classifier (read ApiModel + inherit + LLM-on-residual) | 🚧 in progress |
| 3 | FK graph + APSP + Steiner solver | pending |
| 4 | Sub-clustering inside oversize domains | pending |
| 5 | Embedding + vector index | pending |
| 6 | Entity resolver (4-tier funnel) | pending |
| 7 | Gold SQL store (CRUD + retrieval + AST embed) | pending |
| 8 | NL→SQL pipeline orchestrator | pending |
| 9 | Validation + repair loop | pending |
| 10 | Visualization + description | pending |
| 11 | API + auth + RBAC | pending |
| 12 | Frontend (Query / Gold / Schema / Cluster / Eval / Settings) | pending |
| 13 | Eval harness (continuous, CI-gated) | pending |
| 14 | Hardening (more providers, k8s, observability, docs) | pending |

## Quickstart

```bash
# 1. Tooling
brew install colima docker docker-compose sevenzip
colima start --cpu 4 --memory 8 --disk 60

# 2. Python 3.12+ venv
python3.13 -m venv .venv
.venv/bin/pip install -e packages/backend

# 3. Local DB (Ed-Fi populated Postgres container — has all 767 tables + sample data)
docker run -d --name edfi-pg \
  -e POSTGRES_USER=edfi -e POSTGRES_PASSWORD=edfi -e TPDM_ENABLED=true \
  -p 5432:5432 edfialliance/ods-api-db-ods-populated:7.0

# 4. Secrets
cp .env.example .env
# fill in AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, etc. (.env is gitignored)

# 5. Source env + run Component 1 (data ingestion)
set -a && . .env && TARGET_DB_PASSWORD=edfi && set +a
.venv/bin/text2sql ingest

# 6. Verify everything wired correctly
.venv/bin/pytest packages/backend/tests/ -v
```

## Configuration

Provider routing lives in [`configs/default.yaml`](./configs/default.yaml). Secrets come from `.env` only (gitignored). YAML references env vars via `${VAR}` or `${VAR:-default}` — the loader interpolates.

**Default stack:**
- **Target SQL engine:** Postgres 13 — `edfialliance/ods-api-db-ods-populated:7.0` (767 tables, 1,270 FKs, real sample data). Switch to Azure SQL by setting `target_db.primary: mssql-azure` in YAML.
- **Metadata DB:** Postgres (will be a separate container; gold SQL, users, audit, eval runs).
- **LLM:** Azure OpenAI `gpt-4o` (your deployment, via `.env`).
- **Embeddings:** Azure OpenAI `text-embedding-3-large` (3072-dim).
- **Vector store:** FAISS file-backed under `data/artifacts/vector/`.

Other providers ship — Bedrock, Anthropic direct, OpenAI direct, BGE-M3-local, Qdrant, OpenSearch, Azure AI Search — but aren't wired by default. Switch by editing the `primary:` field of the relevant section.

## Layout

```
configs/                YAML configs (provider routing + domain overrides)
data/edfi/              cached ApiModel.json + ForeignKeys.sql (gitignored)
data/artifacts/         table_classification.json, graph.pkl, vector index, etc.
packages/backend/       Python — FastAPI + CLI + components
  src/text2sql/
    config.py           layered YAML+env config
    ingestion/          Component 1
    classification/     Component 2 (next)
    providers/          LLM / Embedding / Vector / SqlEngine factories
  tests/                integration tests against real services
packages/frontend/      Next.js (later)
```

## Verification

After each component lands, integration tests prove it works against real systems:

```bash
.venv/bin/pytest packages/backend/tests/ -v
```

Tests skip cleanly if a backing service (Azure OpenAI, Postgres, …) isn't reachable — they don't lie about coverage.

See [`edfi_text2sql_plan.md`](./edfi_text2sql_plan.md) for the full architectural spec.
