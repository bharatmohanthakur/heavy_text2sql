# Ed-Fi Text-to-SQL Platform

A production-grade NL→SQL platform for Ed-Fi ODS databases. Asks questions in
plain English; returns validated SQL, executed rows, a chart, and a one-line
description — backed by an authoritative FK graph, a domain-classified table
catalog, a vector + BM25 retrieval index, an approved gold-SQL flywheel, and
a streaming agent loop.

The system runs end-to-end against the published Ed-Fi DS 6.1.0 metadata + the
Northridge populated template (15 GB MSSQL backup, 829 tables, 21 628 students,
1.1 M attendance events).

The full design is in [`edfi_text2sql_plan.md`](./edfi_text2sql_plan.md);
this README is the operator quickstart.

> **Note on TPDM:** the public Ed-Fi Extensions repo does not yet publish DS
> 6.1.0-targeted TPDM artifacts (only DS 4.0.0 / 5.2.0). Run on DS 6.1.0 core
> today; TPDM lights up automatically once Ed-Fi ships a 6.1.0 artifact set.

---

## What this gives you

- **`/query`** — single-shot pipeline: classify domains → resolve entities →
  retrieve tables → Steiner-tree FK joins → few-shot retrieval → LLM SQL →
  validate + repair → execute → Vega-Lite + NL summary.
- **`/chat`** — agentic, multi-turn, streaming. The LLM drives the same
  components as tools, with token-by-token SSE streaming, conversation
  persistence, and automatic post-process viz/description.
- **Eval harness** — 6 metrics (schema-linking recall, join-path exactness,
  SQL syntactic validity, execution accuracy, descriptor leakage, latency
  p50/p95/p99) with markdown + JSON reports and a regression gate.
- **Provider abstraction** — swap LLM / embedder / vector store / target DB
  via YAML, no code changes.

---

## Architecture

```
                          ┌──────────────────────────────────────────┐
                          │  Next.js frontend                        │
                          │  /, /chat, /tables, /domains, /gold      │
                          └─────────────┬────────────────────────────┘
                                        │ /api/* rewrite
                                        ▼
                          ┌──────────────────────────────────────────┐
                          │  FastAPI backend  (port 8011)            │
                          │  /query  /chat  /chat/stream             │
                          │  /tables /domains /gold /conversations   │
                          └─────────────┬────────────────────────────┘
                                        │
   ┌────────────────────────────────────┼─────────────────────────────────┐
   │                                    │                                 │
   ▼                                    ▼                                 ▼
Text2SqlPipeline                   AgentRunner                      Component layer
(canonical, sync)                  (streaming, multi-turn)          (shared by both)
                                                                    ┌─────────────┐
                                                                    │  classify   │
                                                                    │  resolve    │
                                                                    │  retrieve   │
                                                                    │  steiner    │
                                                                    │  gold       │
                                                                    │  validate   │
                                                                    │  execute    │
                                                                    │  viz+desc   │
                                                                    └─────────────┘
                                                                          │
                                                                          ▼
                                                              ┌─────────────────────┐
                                                              │  Live MSSQL ODS     │
                                                              │  (Northridge)       │
                                                              │  Postgres metadata  │
                                                              │  FAISS vector index │
                                                              └─────────────────────┘
```

The 14 components are described in `edfi_text2sql_plan.md` (build-ready spec,
source of truth).

---

## Prerequisites

- **macOS / Linux**, Python 3.12, Node 20+
- **`uv`** for Python packaging (`brew install uv` or
  `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- **Docker** (Colima on Apple Silicon: see "Apple Silicon" below) for
  the live MSSQL ODS
- **Postgres** running locally (or Docker) for metadata + conversations
- An LLM key (Azure OpenAI is the default; OpenAI / OpenRouter / Anthropic
  / vLLM also supported via config)

---

## One-shot setup

```bash
# 1. Clone & install Python deps into a uv-managed venv
git clone https://github.com/bharatmohanthakur/heavy_text2sql.git
cd heavy_text2sql
uv sync                                # creates .venv, installs everything

# 2. Install frontend deps
cd packages/frontend && npm install && cd -

# 3. Provide credentials
cp .env.example .env
$EDITOR .env                           # fill in LLM + DB credentials

# 4. Bring up MSSQL Northridge + Postgres metadata DB
make docker-up                         # docker-compose up -d
make restore-northridge                # download .bak (~15 GB) and RESTORE

# 5. Build the static artifacts (one-time, ~5 min)
make build                             # ingest → graph → catalog → embed

# 6. Seed the gold few-shot store
make gold-seed                         # loads data/eval/gold_queries_bootstrap.yaml

# 7. Run it
make api                               # FastAPI on :8011
make frontend                          # Next.js on :3000
```

Open `http://localhost:3000/chat` and ask a question.

---

## Configuration — providers via YAML

All routing lives in `configs/default.yaml`. **Secrets never live in YAML** —
YAML references env vars via `${VAR}` and the loader interpolates from `.env`
+ process env (process env wins).

### LLM providers

| name | kind | required env |
|---|---|---|
| `azure-gpt-4o` (default) | `azure_openai` | `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_CHAT_DEPLOYMENT` |
| `openai-gpt-4o` | `openai` | `OPENAI_API_KEY` |
| `anthropic-sonnet` | `anthropic` | `ANTHROPIC_API_KEY` |
| `openrouter-glm-5.1` | `openrouter` | `OPENROUTER_API_KEY` |
| `openrouter-deepseek-v3.2` | `openrouter` | `OPENROUTER_API_KEY` |
| `openrouter-claude-sonnet-4.6` | `openrouter` | `OPENROUTER_API_KEY` |
| `openrouter-qwen3-coder-next` | `openrouter` | `OPENROUTER_API_KEY` |

To switch the primary LLM:
```yaml
llm:
  primary: openai-gpt-4o      # was azure-gpt-4o
```

To route specific tasks to specific models (e.g. cheap LLM for descriptions,
strong one for SQL):
```yaml
llm:
  task_routing:
    sql_generation:        azure-gpt-4o
    repair_loop:           azure-gpt-4o
    visualization:         azure-gpt-4o          # or anything cheaper
    description:           azure-gpt-4o
    classifier_fallback:   azure-gpt-4o
```

> **Note**: the agentic chat (`/chat`) currently uses the OpenAI tool-calling
> shape, so its `llm_for_task("sql_generation")` must resolve to an
> `azure_openai` or `openai` provider. The pipeline (`/query`) accepts any
> provider that implements the `LLMProvider` protocol.

### Embedding providers

| name | kind | dim | runs locally? |
|---|---|---|---|
| `azure-text-embedding-3-large` (default) | `azure_openai` | 3072 | no |
| `openai-text-embedding-3-large` | `openai` | 3072 | no |
| `openai-text-embedding-3-small` | `openai` | 1536 | no |
| `bge-m3-local` | `sentence_transformers` (BAAI/bge-m3) | 1024 | **yes — CPU/GPU** |
| `bge-large-en-local` | `sentence_transformers` (BAAI/bge-large-en-v1.5) | 1024 | **yes — CPU** |
| `e5-mistral-7b-local` | `sentence_transformers` (intfloat/e5-mistral-7b-instruct) | 4096 | **yes — GPU** |
| `multilingual-e5-large-local` | `sentence_transformers` (intfloat/multilingual-e5-large) | 1024 | **yes — CPU** |
| `minilm-l6-local` | `sentence_transformers` (all-MiniLM-L6-v2) | 384 | **yes — CPU, tiny** |

#### Use a local Hugging Face model (offline, no per-call cost)

```bash
# 1. Install the optional extra (one-time, ~3 GB for torch + sentence-transformers)
uv sync --extra local-embeddings

# 2. Pick a local provider in .env
echo "EMBEDDING_PROVIDER=bge-m3-local" >> .env
# (or: bge-large-en-local | e5-mistral-7b-local | multilingual-e5-large-local | minilm-l6-local)

# 3. Re-build the FAISS index with the new model (must rebuild — different
#    embedding dimension means the old index is incompatible)
uv run text2sql index-catalog
```

The first `index-catalog` after switching downloads the model weights to
`~/.cache/huggingface/` (1–8 GB depending on the model). Subsequent runs use
the local cache.

To add a model not listed above (e.g. `nomic-embed-text-v1.5`,
`Snowflake/arctic-embed-l`, your own fine-tune), append it under
`embeddings.providers` in `configs/default.yaml`:

```yaml
embeddings:
  providers:
    my-custom-encoder:
      kind: sentence_transformers
      model: nomic-ai/nomic-embed-text-v1.5
      device: cpu                   # cpu | cuda | mps
      dim: 768
      batch_size: 32
      normalize: true
      trust_remote_code: true       # required by some HF repos
      query_prefix: "search_query: "    # E5/Nomic-style prompts (optional)
      doc_prefix: "search_document: "
```

Then `EMBEDDING_PROVIDER=my-custom-encoder` in `.env`.

#### When to pick local vs cloud

| | local (BGE-M3, E5) | cloud (Azure / OpenAI) |
|---|---|---|
| First-run cost | model download (1–8 GB) | API key only |
| Per-call cost | $0 | $0.13 / 1M tokens (3-large) |
| Privacy | data never leaves the box | data sent to vendor |
| Quality on Ed-Fi text | within 1–3 pts of 3-large | best-in-class |
| GPU helpful for | E5-Mistral-7B, big rebuilds | n/a |
| Index size | smaller (1024-dim < 3072-dim) | larger |

For dev / personal use, **`bge-large-en-local`** on CPU is the sweet spot:
fast enough for the 829-table catalog (~30s rebuild), no key required,
quality close to OpenAI.

### Vector stores

| name | kind |
|---|---|
| `faiss-local` (default) | local FAISS index under `data/artifacts/vector/` |
| `qdrant-local` | Qdrant on `http://127.0.0.1:6333` |

### Target DB (the populated ODS we run SQL against)

```yaml
target_db:
  primary: ${TARGET_DB:-postgresql-local}
```

Pick via the `TARGET_DB` env var:
- `postgresql-local` — local Postgres ODS
- `mssql-azure` — remote Azure SQL
- `mssql-northridge` — local Docker MSSQL with Northridge restored

### Metadata DB (gold store + conversation history)

Always Postgres. Configured under `metadata_db:` in YAML.

---

## Stage-by-stage build

The platform is built component-by-component. Each command is idempotent and
emits an artifact under `data/artifacts/`. You only need to re-run a stage if
the upstream input changed.

### Stage 1 — ingest Ed-Fi metadata (Component 1)

Fetches `ApiModel.json` + `0030-ForeignKeys.sql` for DS 6.1.0 (and any
configured extensions) from the Ed-Fi GitHub. Cached in `data/edfi/`.

```bash
uv run text2sql ingest                 # default: data_standard_version=6.1.0
uv run text2sql ingest --force         # re-fetch from GitHub
```

Output: `data/edfi/manifest.json` + per-version subdirs.

### Stage 2 — classify tables into domains (Component 2)

Reads `ApiModel.json` and emits `data/artifacts/table_classification.json` with
domain tags per table. **Domains come from the Ed-Fi metadata, not the LLM.**

```bash
uv run text2sql map-tables-cmd
```

Output: `data/artifacts/table_classification.json` (829 tables, 35 domains).

### Stage 3 — build the FK graph + APSP + Steiner solver (Component 3)

Parses `0030-ForeignKeys.sql` with sqlglot, groups composite FKs by constraint
name, builds a rustworkx graph, runs all-pairs shortest path.

```bash
uv run text2sql build-fk-graph
```

Output: `data/artifacts/graph.pkl`, `data/artifacts/dist.npy`,
`data/artifacts/next_hop.npy`. Steiner solver is a pure-Python KMB on top.

### Stage 4 — build the table catalog (Component 4)

For every table: pull a sample, generate descriptions for tables and
columns via LLM, write the consolidated catalog. **One-time, ~5 min.**

```bash
uv run text2sql build-table-catalog-cmd
```

Output: `data/artifacts/table_catalog.json` (descriptions, sample values,
PK/FK metadata, row counts).

### Stage 5 — embed + index (Component 5)

Builds the per-table semantic blob (table description + key columns +
column semantics + neighbors), embeds them, writes a FAISS index plus a
BM25 sidecar for hybrid search.

```bash
uv run text2sql index-catalog
uv run text2sql search-tables "students absent in math"   # smoke-test
```

Output: `data/artifacts/vector/`.

### Stage 6 — seed the gold SQL store (Component 7)

```bash
uv run text2sql gold-init               # creates the Postgres tables
uv run text2sql gold-seed               # loads gold_queries_bootstrap.yaml,
                                        # exec-validates each against live DB
uv run text2sql gold-search "top schools by enrollment"
```

Output: rows in `text2sql_meta.gold_sql` + AST-embedded vectors.

### Stage 7 — eval (Component 13)

```bash
uv run text2sql evaluate \
    --suite data/eval/gold_queries_bootstrap.yaml \
    --out-json  data/eval/runs/last.json \
    --out-md    data/eval/runs/last.md \
    --fail-on-regression 0.70           # CI gate at 70 % execution accuracy
```

### Stage 8 — serve

```bash
uv run text2sql serve --port 8011       # FastAPI + agent + WebSocket
cd packages/frontend && npm run dev     # Next.js on :3000
```

---

## Bring your own database

The repo defaults to the Northridge sample, but you can point it at any
Ed-Fi-shaped ODS — Postgres, MSSQL Server, or Azure SQL — without changing
any code. The platform is schema-aware (built off Ed-Fi DS 6.1.0 metadata)
so your DB must follow Ed-Fi conventions: an `edfi.*` schema, the standard
`edfi.Descriptor` lookup table, FK constraints in place, and primary keys
on `Student`, `School`, `EducationOrganization`, etc.

### 1. Add a provider entry to `configs/default.yaml`

Pick the dialect that matches your DB and append it under `target_db.providers`:

```yaml
target_db:
  primary: ${TARGET_DB:-my-ods}        # ← your new entry's name

  providers:
    # Postgres
    my-ods:
      kind: postgresql
      host: ${MY_DB_HOST:-127.0.0.1}
      port: ${MY_DB_PORT:-5432}
      database: ${MY_DB_NAME:-EdFi_Ods}
      user: ${MY_DB_USER:-edfi}
      password_env: MY_DB_PASSWORD     # name of the env var (NOT the value)
      schema_search_path: ["edfi", "tpdm"]

    # OR — MSSQL Server / Azure SQL
    my-ods-mssql:
      kind: mssql
      host: ${MY_DB_HOST}
      port: ${MY_DB_PORT:-1433}
      database: ${MY_DB_NAME}
      user: ${MY_DB_USER}
      password_env: MY_DB_PASSWORD
      trust_server_certificate: true
      encrypt: false                   # set true for production / Azure SQL
      driver: "ODBC Driver 18 for SQL Server"   # mssql only
```

The two existing kinds — `postgresql` and `mssql` — are already implemented
in `packages/backend/src/text2sql/providers/db/`. You don't write code, you
just add the YAML entry.

### 2. Put credentials in `.env`

```bash
# .env
MY_DB_HOST=db.internal.company
MY_DB_NAME=EdFi_Ods_Production
MY_DB_USER=text2sql_readonly
MY_DB_PASSWORD=••••••••••••
TARGET_DB=my-ods                       # tells the loader which provider to use
```

The user only needs `SELECT` rights on `edfi.*` (and `tpdm.*` if you have
TPDM extensions). `EXPLAIN` and read-only metadata catalogs are required by
the validator.

### 3. Verify connectivity

```bash
TARGET_DB=my-ods uv run text2sql show-config        # see resolved config
TARGET_DB=my-ods uv run text2sql ask "SELECT 1"     # smoke-test
```

If `Login failed` or `connection refused`, fix that before going further.

### 4. Build artifacts against your DB

The catalog and embedding step pull **sample rows from your DB**, which is
where descriptions and `[examples: …]` come from. So these need to run
against your real DB:

```bash
TARGET_DB=my-ods uv run text2sql ingest                  # one-time, schema-only
TARGET_DB=my-ods uv run text2sql map-tables-cmd          # classify by Ed-Fi domains
TARGET_DB=my-ods uv run text2sql build-fk-graph          # FK graph
TARGET_DB=my-ods uv run text2sql build-table-catalog-cmd # samples + LLM descriptions
TARGET_DB=my-ods uv run text2sql index-catalog           # embed + FAISS index
```

### 5. (Optional) Seed gold few-shots from your team's known-good queries

Drop NL/SQL pairs into a YAML file in your DB's dialect, then:

```bash
TARGET_DB=my-ods uv run text2sql gold-seed --yaml-path data/eval/my_gold.yaml
```

Each pair is exec-validated against your live DB before being stored — bad
ones are reported and skipped.

### 6. Serve

```bash
TARGET_DB=my-ods uv run text2sql serve --port 8011
cd packages/frontend && npm run dev
```

### Notes when adopting your own DB

- **Dialect detection is automatic.** The validator, repair loop, and prompt
  rules all read `sql_engine.dialect` ("postgresql" / "mssql"). Identifier
  quoting (`"name"` vs `[name]`) and row caps (`LIMIT 50` vs `TOP 50`) are
  picked accordingly.
- **Gold few-shots are dialect-locked.** A gold pair written in Postgres
  syntax will steer the LLM to write Postgres syntax. Re-seed when switching
  dialects (the bootstrap YAML is MSSQL T-SQL).
- **Live-DB filter.** At startup the platform filters the catalog down to
  the tables/columns that actually exist in your live DB. Anything in the
  Ed-Fi metadata schema but missing from your install is hidden from the
  LLM, eliminating "Invalid object name" hallucinations.
- **TPDM / extensions.** If your ODS has TPDM (or a custom extension)
  schemas, add them to `configs/default.yaml` under `ed_fi.extensions` and
  to `target_db.providers.<name>.schema_search_path`. The ingest stage
  fetches matching extension artifacts from the Ed-Fi GitHub repo
  automatically.
- **Non-Ed-Fi databases.** Out of scope today. The classifier, graph
  builder, and descriptor logic are all Ed-Fi-specific. You'd need to swap
  Components 1-2 (ingestion + classification) and re-author the gold store
  for a non-Ed-Fi schema.

---

## Apple Silicon — running MSSQL Server natively

Default Colima uses QEMU which crashes SQL Server 2022 with an mmap error.
Use **vz + Rosetta 2**:

```bash
softwareupdate --install-rosetta --agree-to-license
colima delete -f
colima start --vm-type=vz --vz-rosetta --cpu 4 --memory 12 --disk 80
docker-compose up -d mssql
```

Then restore Northridge:

```bash
make restore-northridge        # downloads ~15 GB .bak, RESTOREs into mssql container
```

The container env is `MSSQL_SA_PASSWORD=Text2Sql!Strong1` — the `.env.example`
has the matching client-side env.

---

## CLI cheatsheet

| command | purpose |
|---|---|
| `text2sql ingest` | Stage 1 — fetch Ed-Fi metadata |
| `text2sql show-config` | print resolved config (secrets redacted) |
| `text2sql map-tables-cmd` | Stage 2 — classify tables into domains |
| `text2sql build-fk-graph` | Stage 3 — FK graph + APSP |
| `text2sql build-table-catalog-cmd` | Stage 4 — descriptions + samples |
| `text2sql index-catalog` | Stage 5 — embed + index |
| `text2sql search-tables "..."` | smoke-test the retriever |
| `text2sql resolve-entities "..."` | smoke-test the 4-tier resolver |
| `text2sql classify-query "..."` | smoke-test the domain classifier |
| `text2sql gold-init` | create gold-SQL Postgres tables |
| `text2sql gold-seed` | load bootstrap gold SQL + exec-validate |
| `text2sql gold-search "..."` | top-k few-shot retrieval |
| `text2sql ask "..."` | run the canonical pipeline once |
| `text2sql chat "..."` | run the agentic loop once (multi-turn via `--conversation-id`) |
| `text2sql evaluate` | run the eval harness, write reports |
| `text2sql serve` | start the FastAPI server |

---

## API surface (port 8011)

| route | what |
|---|---|
| `GET /health` | catalog count + gold-store status |
| `POST /query` | one-shot pipeline (sync) |
| `WS /query/stream` | streamed pipeline stages |
| `POST /chat` | agentic single response (sync) |
| `POST /chat/stream` | SSE: token deltas → tool calls → tool results → post-process viz → final result |
| `GET /tables` | catalog list (filter by domain) |
| `GET /tables/{fqn}` | one table's full schema |
| `GET /domains` | domain → table count |
| `GET /gold` / `POST /gold` / approve / reject | gold-SQL flywheel |
| `GET /conversations` | list past chats |
| `GET /conversations/{id}` | full message history |
| `DELETE /conversations/{id}` | delete |

The frontend uses `/api/*` which Next.js rewrites to `127.0.0.1:8011`.

---

## SSE event protocol on `/chat/stream`

A single chat turn emits, in order:

| event | when |
|---|---|
| `conversation_id` | once, immediately |
| `text_delta` | per assistant text token |
| `tool_call_delta` | per tool-argument JSON token (you'll see args build up live) |
| `step` (tool_call) | once a tool call is fully assembled and about to fire |
| `step` (tool_result) | once the tool returns |
| ... repeat per LLM step ... | |
| `post_process_started` | the agent terminated; viz + description are running |
| `viz` | rows + Vega-Lite spec + NL description |
| `result` | final summary, conversation_id, total_ms |

The frontend renders `text_delta` and `tool_call_delta` for the typing
effect, then promotes drafts to finalized steps when `step` arrives.

---

## Where state lives

| | path / DB |
|---|---|
| Ed-Fi cached metadata | `data/edfi/` (gitignored, ~50 MB per DS version) |
| Build artifacts | `data/artifacts/` (gitignored, ~75 MB total) |
| Gold queries (source) | `data/eval/gold_queries_bootstrap.yaml` |
| Eval reports | `data/eval/runs/` |
| Live ODS | MSSQL Server in Docker, DB `EdFi_Ods_Northridge` |
| Gold-SQL store | Postgres `text2sql_meta` schema |
| Conversation history | Postgres `text2sql_meta.conversation` + `conversation_message` |
| Vector index | local FAISS in `data/artifacts/vector/` |

Wipe everything safe to rebuild: `rm -rf data/edfi data/artifacts && make build`.

---

## Tests

```bash
# Unit tests (fast, no live deps)
uv run pytest -q

# Live-stack tests (require Azure key + MSSQL + Postgres)
TARGET_DB=mssql-northridge \
  MSSQL_SA_PASSWORD='Text2Sql!Strong1' \
  METADATA_DB_PASSWORD=edfi \
  uv run pytest packages/backend/tests/test_agent_loop.py
```

Test coverage: 25 agent-stack tests (5 conversation store + 15 tool wrappers
+ 5 loop integration), plus per-component tests for ingestion, classifier,
graph, catalog, retrieval, resolver, gold store, pipeline, repair loop, viz,
API, eval harness.

---

## Troubleshooting

- **`required secret env var 'X' is not set`** — the YAML references an env
  var that's missing from `.env` or process env. `text2sql show-config`
  shows what got interpolated.
- **`Login failed for user 'SA'`** — set `MSSQL_SA_PASSWORD=Text2Sql!Strong1`
  to match `docker-compose.yml`.
- **MSSQL "Invalid mapping of address" on Apple Silicon** — Colima is on
  QEMU instead of vz/Rosetta. See "Apple Silicon" above.
- **`/chat` returns sql=null** — the agent gave up. Check the conversation
  via `GET /conversations/{id}`; usually means `find_similar_queries`
  returned no relevant gold examples for that question shape — add one to
  the gold store and re-seed.
- **"agent loop currently supports azure_openai or openai LLMs"** — the
  primary LLM under task `sql_generation` must be one of those for
  tool-calling. Anthropic + OpenRouter are supported by `/query` but not
  `/chat`.
- **Cluster IDs change across rebuilds** — Hungarian assignment in
  `classification/subcluster.py` keeps them stable when overlap > 70%.

---

## Spec / further reading

- **`edfi_text2sql_plan.md`** — the 900-line build-ready spec; the source
  of truth this implementation realizes.
- Component-level READMEs and design notes live in the source files
  (top-of-module docstrings).

---

## License

Internal. See repository owner.
