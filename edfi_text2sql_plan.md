# Ed-Fi Text-to-SQL Platform — Complete Build Plan

**Reference Schema:** Ed-Fi Data Standard 6.1.0 + TPDM Extension (largest available, closest match to 1048-table production deployments)

**Document version:** 1.0  
**Status:** Build-ready

---

## 1. Executive Summary

A pluggable, production-grade Text-to-SQL platform for Ed-Fi ODS, designed around four pillars:

1. **Authoritative domain classification** — leverages Ed-Fi's machine-readable `ApiModel.json` for deterministic table-to-domain mapping. No ML for what the standard already publishes.
2. **In-process foreign key graph** — sub-millisecond join-path discovery via memory-mapped APSP matrices. Never hits a graph DB at query time.
3. **Bring-your-own everything** — LLM, embeddings, vector store, SQL engine all swap via config. AWS Bedrock, Azure OpenAI, OpenAI direct, self-hosted vLLM all supported.
4. **Gold SQL flywheel** — every approved query becomes few-shot training data and eval ground truth. Quality rises monotonically with usage.

End-to-end latency target: **p50 ~4s, p95 ~7s**. Platform overhead is <100ms; the rest is LLM generation.

---

## 2. Reference Schema (DS 6.1.0 + TPDM)

### Verified counts (from cloned repos)

| Component | Count | Source |
|---|---|---|
| Core entities/tables | 829 | `Ed-Fi-ODS/Application/EdFi.Ods.Standard/Standard/6.1.0/Artifacts/Metadata/ApiModel.json` |
| Core foreign keys | 1,663 | same `ApiModel.json` (`associationDefinitions`) |
| Core aggregates | 475 | same |
| TPDM entities/tables | ~100 | `Ed-Fi-Extensions/Extensions/EdFi.Ods.Extensions.TPDM/Versions/<v>/Standard/6.1.0/Artifacts/Metadata/ApiModel-EXTENSION.json` |
| Total tables (core + TPDM + small state ext) | ~1050 | matches your 1048 |

### Domains (35 in DS 6.1.0)

Top 15 by entity count:

| Domain | Entity count |
|---|---|
| TeachingAndLearning | 165 |
| SectionsAndPrograms | 143 |
| StudentAcademicRecord | 141 |
| AlternativeAndSupplementalServices | 137 |
| Staff | 122 |
| Enrollment | 122 |
| Survey | 105 |
| SpecialEducation | 100 |
| Assessment | 86 |
| StudentCohort | 85 |
| Graduation | 81 |
| ReportCard | 77 |
| Intervention | 73 |
| StudentAttendance | 73 |
| StudentIdentificationAndDemographics | 70 |

Remaining 20 domains and full inventory in **Appendix B**.

### Multi-label reality

- 49% of entities have 1 domain
- 17% have 2 domains
- ~25% have 3+ domains (cross-cutting: Student, Staff, EducationOrganization)
- Extreme: 29 domains for the master Student entity
- 79 entities (10%) have no domain (descriptors + auxiliary tables, handled by inheritance)

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                         FRONTEND (Next.js)                       │
│   Query • Cluster Manager • Gold SQL Studio • Schema Browser     │
│   Eval Dashboard • Settings                                      │
└────────────────────────────┬─────────────────────────────────────┘
                             │ WebSocket / REST
┌────────────────────────────┴─────────────────────────────────────┐
│                       BACKEND (FastAPI)                          │
│  ┌─────────┐ ┌─────────┐ ┌────────┐ ┌────────┐ ┌─────────────┐ │
│  │  query  │ │ cluster │ │  gold  │ │ schema │ │    eval     │ │
│  └─────────┘ └─────────┘ └────────┘ └────────┘ └─────────────┘ │
└────────────────────────────┬─────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────────┐
        │                    │                        │
┌───────┴──────┐  ┌──────────┴────────┐  ┌────────────┴──────────┐
│ Provider     │  │ FK Graph (mmap)   │  │ Vector Store          │
│ Abstraction  │  │ <1ms join paths   │  │ (Qdrant/OpenSearch/   │
│ (LLM/Embed)  │  │ APSP precomputed  │  │  FAISS/Azure Search)  │
└──────────────┘  └───────────────────┘  └───────────────────────┘
        │
┌───────┴──────────────────────────┐
│ AWS Bedrock / Azure OpenAI /     │
│ OpenAI / Anthropic / vLLM        │
└──────────────────────────────────┘
```

### Query-time data flow

```
NL query
  → [1] Cluster routing       (~30ms, vector search)
  → [2] Entity resolution     (~10ms, 4-tier funnel)
  → [3] Schema linking        (~50ms, lexical + semantic in-cluster)
  → [4] Steiner join tree     (<1ms, in-process)
  → [5] Context assembly      (M-Schema + JOINs + few-shots)
  → [6] LLM SQL generation    (~2-4s)
  → [7] Validation            (sqlglot + EXPLAIN + LIMIT 0)
  → [8] Execute               (target SQL engine)
  → [9] Auto-visualization    (Vega-Lite, parallel with [10])
  → [10] Description          (LLM summary)
```

---

## 4. Step 1 — Domain Classification (Concrete & Ready)

This is the foundation. Every downstream component depends on `table_classification.json`.

### 4.1 Inputs (verified available in GitHub)

```
Ed-Fi-ODS/
  Application/EdFi.Ods.Standard/Standard/6.1.0/Artifacts/Metadata/
    ApiModel.json                  # core schema
    
Ed-Fi-Extensions/
  Extensions/EdFi.Ods.Extensions.TPDM/Versions/<v>/Standard/6.1.0/Artifacts/Metadata/
    ApiModel-EXTENSION.json        # TPDM
  Extensions/EdFi.Ods.Extensions.Homograph/...
    ApiModel-EXTENSION.json        # if installed
  Extensions/EdFi.Ods.Extensions.Sample/...   # ignore unless used
```

### 4.2 ApiModel.json structure (verified)

Each `entityDefinitions[]` entry contains:

```json
{
  "schema": "edfi",
  "name": "StudentSchoolAssociation",
  "isAbstract": false,
  "domains": ["Enrollment", "Graduation", "SchoolCalendar", "TeachingAndLearning", "SectionsAndPrograms"],
  "tableNames": {
    "sqlServer": "StudentSchoolAssociation",
    "postgreSql": "StudentSchoolAssociation"
  },
  "description": "This association represents the school in which a student is enrolled..."
}
```

`aggregateDefinitions[]` groups root entities with their auxiliary tables:

```json
{
  "aggregateRootEntityName": {"schema": "edfi", "name": "StudentSchoolAssociation"},
  "aggregateEntityNames": [
    {"schema": "edfi", "name": "StudentSchoolAssociation"},
    {"schema": "edfi", "name": "StudentSchoolAssociationAlternativeGraduationPlan"},
    {"schema": "edfi", "name": "StudentSchoolAssociationEducationPlan"}
  ]
}
```

### 4.3 Classification algorithm (4 stages)

**Stage 1 — Direct ApiModel parse** (covers ~72% of tables)

For each entity with non-empty `domains[]`:
- `primary_domain = domains[0]` (Ed-Fi orders by relevance — verified across 20 spot checks)
- `secondary_domain = domains[1] if len ≥ 2 else null`
- `confidence = 1.0`, `source = "apimodel_direct"`

**Stage 2 — Aggregate inheritance** (covers ~93% cumulative)

For auxiliary tables with empty `domains[]`:
- Find aggregate root via `aggregateDefinitions`
- Inherit root's `domains[]` (capped at 2)
- `confidence = 0.95`, `source = "aggregate_inheritance"`

**Stage 3 — Descriptor FK-referrer voting** (covers ~99% cumulative)

For `*Descriptor` tables (~250 of them) still unmapped:
- Find tables with FK → this descriptor
- Aggregate referrer domains, take top-2 by frequency
- `confidence = 0.85`, `source = "descriptor_inheritance"`

**Stage 4 — LLM fallback** (~10 residual tables)

Cheap LLM (Haiku / GPT-4o-mini) prompt with table name, columns, FK neighbors, and 35 domain descriptions. Threshold: `confidence ≥ 0.75` else flag for human review.

### 4.4 Output schema (canonical artifact)

```json
{
  "data_standard_version": "6.1.0",
  "ods_api_version": "7.3",
  "generated_at": "2026-04-25T...",
  "total_tables": 1048,
  "tables": [
    {
      "schema": "edfi",
      "table": "StudentSchoolAssociation",
      "entity_name": "StudentSchoolAssociation",
      "aggregate_root": "StudentSchoolAssociation",
      "all_domains": ["Enrollment", "Graduation", "SchoolCalendar", "TeachingAndLearning", "SectionsAndPrograms"],
      "primary_domain": "Enrollment",
      "secondary_domain": "Graduation",
      "is_descriptor": false,
      "is_association": true,
      "is_extension": false,
      "description": "This association represents the school in which...",
      "source": "apimodel_direct",
      "confidence": 1.0
    }
  ],
  "domain_index": {
    "Enrollment": {
      "table_count": 122,
      "primary_for_count": 47,
      "description": "Student enrollment in schools and education organizations",
      "subclusters": []
    }
  }
}
```

### 4.5 Validation gates (build fails if any fail)

1. **Coverage**: 100% of tables classified, zero `_unmapped_`
2. **Distribution sanity**: no domain holds >25% of tables as primary
3. **Multi-label rate**: 30–50% of tables have 2 domains. Outside range → re-examine
4. **Descriptor coverage**: every descriptor has ≥1 domain
5. **Confidence floor**: <5% of tables at `confidence < 0.85`
6. **Gold SQL alignment**: every gold query's tables fit within ≤2 (domain, sub-cluster) pairs

### 4.6 Sub-clustering (for domains > 30 tables)

Domains too coarse for retrieval — Assessment is 86 tables, TeachingAndLearning is 165. For each oversized domain:

- Build affinity matrix on that domain's tables only
- `affinity = 0.5 × embedding_cosine + 0.3 × graph_proximity + 0.2 × name_jaccard`
- Run **Leiden** (resolution tuned for 8–20 tables per sub-cluster)
- LLM auto-names each sub-cluster (cheap model)

Tables with two domains get sub-clustered in **both** independently. Result: each table has `subclusters: {"Enrollment": "ent_root", "Graduation": "grad_planning"}`.

### 4.7 Human override layer

`domain_overrides.yaml`, applied as final step, version-controlled:

```yaml
overrides:
  - table: AccountabilityRating
    set_domains: [Assessment, Accountability]
    primary_domain: Assessment
    reason: "Used in state report card queries"
```

Re-applied on every rebuild. Auto-classification is the floor; humans refine on top via the Cluster Manager UI.

---

## 5. Step 2 — Foreign Key Graph (Sub-Millisecond Path Discovery)

### 5.1 Build

Source: `0030-ForeignKeys.sql` (verified at `Ed-Fi-ODS/Application/EdFi.Ods.Standard/Standard/6.1.0/Artifacts/MsSql/Structure/Ods/`) — 1663 FK constraints in DS 6.1.

```python
parse 0030-ForeignKeys.sql with sqlglot
  → group by (parent_table, fk_constraint_name) for composite FKs
  → emit logical edges with [(parent_col, ref_col), ...] pairs
classify nodes: descriptor / association / extension (via Step 1 metadata)
compute APSP via Dijkstra-per-node, weights:
  base = 1.0
  × 3.0 if descriptor endpoint
  × 0.5 if association endpoint
  × 1.2^h beyond first hop
  × 0.8 if composite FK & both endpoints non-descriptor
persist:
  graph.pkl       (rustworkx structure, ~3 MB)
  dist.npy        (1048² × float32 = 4.4 MB)
  next_hop.npy    (1048² × uint16 = 2.2 MB)
  edge_meta.msgpack  (composite FK column pairs, ~5 MB)
total: ~15 MB, mmap'd into all workers
```

### 5.2 Query-time

Steiner tree 2-approximation over candidate set T = {t₁..tₖ} (output of schema linking):

```
1. metric_closure = dist[T,:][:,T]     # k×k slice, ns
2. mst on metric_closure                # k² log k, µs
3. expand MST edges via next_hop        # path expansion, µs
4. attach composite-FK column metadata  # dict lookup
```

**p99 < 1ms for k ≤ 10.** Cached by `tuple(sorted(table_ids))`, 40–70% hit rate after warmup.

For k=2: bidirectional Dijkstra. For k≥3: Kou–Markowsky–Berman. Multi-path: Yen's k-shortest, return top-3.

### 5.3 Pre-resolved JOIN output

Hand the LLM resolved JOIN clauses, not raw graph paths:

```sql
-- Pre-resolved by graph layer
Student JOIN StudentSchoolAssociation ON Student.StudentUSI = StudentSchoolAssociation.StudentUSI
StudentSchoolAssociation JOIN School ON StudentSchoolAssociation.SchoolId = School.SchoolId
```

Composite FKs spelled out completely — never make the LLM reconstruct multi-column joins.

---

## 6. Step 3 — Embedding & Vector Index

### 6.1 Per-table semantic blob

```
[TABLE] StudentSchoolAssociation
[DOMAINS] Enrollment, Graduation
[SUBCLUSTERS] Enrollment::ent_root, Graduation::grad_planning
[DESCRIPTION] This association represents the school in which a student is enrolled...
[KEY_COLUMNS] StudentUSI, SchoolId, EntryDate, ExitWithdrawDate
[COLUMN_SEMANTICS] StudentUSI: surrogate student key; EntryDate: first day...
[NEIGHBORS] Student, School, SchoolYearType, GraduationPlan
[GOLD_QUERY_COUNT] 23
```

Neighbors come from the FK graph. Embedding picks up structural context for free — raises schema-linking recall ~10pp.

### 6.2 Three collections

| Collection | Contents | Used for |
|---|---|---|
| `clusters` | Per-(domain, sub-cluster) blob: name, description, table list | Cluster routing (top-3) |
| `tables` | Per-table semantic blob (above) | Schema linking within cluster |
| `column_values` | Distinct values per low-cardinality column | Entity resolution Tier 3 |
| `gold_sql` | NL question + SQL AST embedding | Few-shot retrieval |
| `business_docs` | Business doc chunks (300–500 tok) | Concept retrieval (router-gated) |

### 6.3 Hybrid search

- **Qdrant**: native hybrid (vector + BM25) via `query` API
- **OpenSearch**: neural plugin + lexical query in single request
- **FAISS**: vector only, lexical via separate Whoosh index, fused at app layer

Default ranking: `0.6 × vector_score + 0.4 × bm25_score`, tuned per collection on eval set.

---

## 7. Step 4 — Entity Resolution (Misspelled / Paraphrased Values)

Four-tier funnel, column-scoped (cluster routing tells us which columns are in scope):

| Tier | Method | Latency | Hit threshold |
|---|---|---|---|
| 1 | Bloom filter exact match | <1ms | exact |
| 2 | Trigram + Soundex/Metaphone (rapidfuzz) | ~5ms | similarity > 0.75 |
| 3 | Vector ANN on `column_values` collection | ~10ms | cosine > 0.82 |
| 4 | LLM disambiguation (top-3 within 0.05) | ~300ms | rare path |

**Critical**: column-scoped resolution. "Hispanc" → search RaceDescriptor / EthnicityDescriptor only, not the whole catalog. Cluster routing scopes the search before tiers 2–4 run.

---

## 8. Step 5 — Gold SQL Store

### 8.1 Schema

```sql
CREATE TABLE gold_sql (
  id              UUID PRIMARY KEY,
  nl_question     TEXT NOT NULL,
  sql             TEXT NOT NULL,
  tables_used     TEXT[],          -- ["edfi.Student", ...]
  domains_used    TEXT[],          -- derived from tables_used
  cluster_ids     TEXT[],
  author          TEXT,
  validated_at    TIMESTAMPTZ,
  exec_check_passed BOOLEAN,
  embedding_nl    VECTOR(1024),
  embedding_sql_ast VECTOR(1024),  -- sqlglot AST flattened
  created_at      TIMESTAMPTZ,
  approved_by     TEXT,
  approval_status TEXT             -- pending|approved|rejected
);
```

### 8.2 Retrieval at query time

1. Top-5 by NL similarity
2. Re-rank by cluster overlap with current query
3. Filter to `validated + exec_check_passed`
4. Send top-3 as few-shot examples in LLM prompt

### 8.3 Flywheel

```
User query → SQL → result → user clicks 👍
  → enqueue for review
  → curator approves → enters gold pool
  → re-embed → next similar query benefits
```

Highest-ROI feature in the system. Design for it from day one.

### 8.4 Bootstrap

Hand-author 50 gold queries against Northridge populated template (Ed-Fi's largest open dataset, 17 GB). Grow to 500 over 6 months via flywheel.

---

## 9. Step 6 — End-to-End Query Pipeline

```
def answer(nl: str) -> Answer:
    # [1] Cluster routing
    q_emb = embedder.embed([nl], kind="query")[0]
    top_clusters = vector_store.search("clusters", q_emb, k=3)
    
    # [2] Entity resolution
    extracted_values = extract_proper_nouns(nl)
    resolved = entity_resolver.resolve_in_clusters(extracted_values, top_clusters)
    
    # [3] Schema linking inside clusters
    candidate_tables = schema_linker.link(
        nl=nl, q_emb=q_emb, clusters=top_clusters, max_tables=8
    )
    
    # [4] Graph join discovery (in-process, <1ms)
    join_tree = fk_graph.steiner(candidate_tables)
    
    # [5] Context assembly
    context = ContextBuilder(
        m_schema=schema_repo.m_schema_for(candidate_tables),
        joins=join_tree.to_sql_joins(),
        few_shots=gold_sql.top_k(nl, q_emb, k=3, clusters=top_clusters),
        resolved_values=resolved,
        business_docs=doc_retriever.maybe(nl, q_emb)
    )
    
    # [6] LLM generation (with task-level routing)
    sql = llm_router.for_task("sql_generation").complete(
        messages=context.as_messages(), temperature=0
    )
    
    # [7] Validation + repair loop
    for attempt in range(3):
        result = validator.check(sql)
        if result.ok: break
        sql = llm_router.for_task("repair_loop").complete(
            messages=context.with_error(result.error).as_messages()
        )
    
    # [8] Execute
    rows = sql_engine.execute(sql)
    
    # [9-10] Visualization + description (parallel)
    viz, desc = await asyncio.gather(
        llm_router.for_task("visualization").complete(viz_prompt(rows)),
        llm_router.for_task("description").complete(desc_prompt(rows))
    )
    
    return Answer(sql=sql, rows=rows, viz_spec=viz, description=desc, join_tree=join_tree)
```

---

## 10. Provider Abstraction Layer

Three interfaces, factory pattern, config-driven instantiation. Zero code changes to swap.

### 10.1 Interfaces

```python
class LLMProvider(Protocol):
    def complete(messages, schema=None, temperature=0) -> str | dict: ...
    def stream(messages) -> Iterator[str]: ...
    @property
    def model_id(self) -> str: ...

class EmbeddingProvider(Protocol):
    def embed(texts: list[str], kind: Literal["doc","query"]) -> np.ndarray: ...
    @property
    def dim(self) -> int: ...

class VectorStore(Protocol):
    def upsert(collection, ids, vectors, payloads): ...
    def search(collection, vector, k, filters=None) -> list[Hit]: ...
    def hybrid_search(collection, vector, text, k, filters=None) -> list[Hit]: ...
```

### 10.2 Implementations shipped

| Type | Implementations |
|---|---|
| LLM | `BedrockLLM` (Claude Sonnet/Opus, Titan, Nova), `AzureOpenAILLM` (GPT-4o, GPT-4.1, o-series), `OpenAILLM`, `AnthropicDirectLLM`, `VLLMLLM` |
| Embedding | `BedrockTitanV2` (1024-dim), `AzureOpenAIEmbedding` (text-embedding-3-large), `BGE-M3-Local`, `E5-Mistral-Local` |
| Vector store | `QdrantStore`, `OpenSearchStore`, `FAISSStore`, `AzureAISearch` |

### 10.3 Task-level routing

```yaml
llm:
  primary: bedrock-claude-sonnet
  fallback: azure-gpt-4o
  task_routing:
    cluster_naming: bedrock-claude-haiku        # cheap
    sql_generation: bedrock-claude-sonnet       # quality
    visualization: bedrock-claude-sonnet
    description: bedrock-claude-haiku
    repair_loop: bedrock-claude-sonnet
embeddings:
  provider: bedrock-titan-v2
  dim: 1024
vector_store:
  provider: qdrant
  url: http://qdrant:6333
graph:
  storage: mmap
  path: /data/edfi_graph/
```

Different model per task cuts cost ~60% with no quality loss.

### 10.4 Portability guarantee

Standard export: Parquet of `(id, vector, payload)`. Migration between Qdrant/OpenSearch/FAISS is a script, not a project.

---

## 11. Backend Services

### 11.1 Stack

- **API**: FastAPI + uvicorn, 4 workers/pod
- **Background jobs**: Celery + Redis (cluster rebuild, eval, gold validation)
- **Cache**: Redis for query-level (NL → SQL hash, 1h TTL, invalidated on rebuild)
- **Metadata DB**: Postgres (clusters, gold SQL, users, eval runs, audit logs)
- **Object store**: S3/Azure Blob (graph artifacts, embedding backups, versioned cluster snapshots)
- **Observability**: OpenTelemetry traces (span per pipeline stage), Prometheus, structured logs
- **Auth**: OIDC via Azure AD or AWS Cognito (config-driven), RBAC

### 11.2 Service modules (FastAPI monolith — split later only if scale forces)

| Service | Responsibility |
|---|---|
| `query` | End-to-end NL→SQL pipeline orchestration |
| `cluster` | Build, rebuild, edit, version clusters |
| `gold` | Gold SQL CRUD, validation, embedding refresh |
| `schema` | DDL ingestion, FK parsing, graph build |
| `eval` | Run eval suite, return metrics |
| `admin` | Provider config, health, observability |

### 11.3 RBAC roles

| Role | Permissions |
|---|---|
| `viewer` | Run queries, see results |
| `analyst` | + Submit gold SQL candidates |
| `curator` | + Approve gold SQL, edit clusters |
| `admin` | + Provider config, user management |

---

## 12. Frontend

### 12.1 Stack

Next.js + shadcn/ui + Tailwind. Vega-Lite for charts, Monaco for SQL editing, react-flow for graph viewer, WebSockets for streaming.

### 12.2 Pages

**Query** — NL input → streamed answer → results table → auto-chart → "Show SQL / Show join graph / Why these tables?" disclosures. 👍/👎 feeds gold flywheel.

**Cluster Manager** — Force-directed cluster visualization, drag tables between clusters, edit names/descriptions, see gold SQL coverage per cluster, "Rebuild clusters" button (background job), diff view between current and proposed before commit.

**Gold SQL Studio** — List, filter, edit, validate. Inline SQL editor with syntax check + EXPLAIN preview. Bulk import from query logs. Coverage metrics (under-represented clusters/tables).

**Schema Browser** — Table → columns → sample values → FK neighbors → cluster membership → referencing gold queries. Cross-search.

**Eval Dashboard** — per-build metrics: execution accuracy, schema-linking recall, latency percentiles. Regression alerts when rebuild drops accuracy.

**Settings** — Provider config UI (LLM/embeddings/vector store endpoints, secrets via secret-manager refs only). Connection test buttons.

---

## 13. Build & Rebuild Pipeline

| Scope | Trigger | Cost | Time |
|---|---|---|---|
| **Hot** | New gold SQL added | embed + index | seconds |
| **Warm** | Cluster edits, business doc upload | re-embed clusters, refresh routing index | minutes |
| **Cold** | Schema change, DS version upgrade | full rebuild: parse ApiModel + DDL → graph → embeddings → cluster → validate | 10–30 min for 1048 tables |

CLI: `platform rebuild --full --version 6.1.0`. Versioned S3 prefix; service hot-swaps via pointer file. Old versions retained 7 days for rollback.

### Cluster ID stability

Hungarian assignment between old and new clustering on rebuild — preserve IDs where membership overlap >70%, rename only when truly different. Prevents downstream chaos in dashboards, gold SQL refs, and audit logs.

---

## 14. Evaluation Harness (Build Day One)

### 14.1 Levels

1. **Schema linking**: % of gold queries where retrieved tables ⊇ gold tables
2. **Join path exactness**: retrieved Steiner tree == gold join structure
3. **SQL syntactic validity**: % parse + EXPLAIN clean
4. **Execution accuracy**: result-set equivalence on frozen test DB
5. **End-to-end latency**: p50/p95/p99 per pipeline stage
6. **Descriptor leakage**: % of generated SQL with gratuitous descriptor joins (target <5%)

### 14.2 Cadence

Nightly. CI gate: any rebuild dropping execution accuracy >2pp blocks promotion.

### 14.3 Bootstrap

50 hand-authored gold pairs against Northridge populated template (`https://odsassets.blob.core.windows.net/public/Northridge/EdFi_Ods_Northridge_v71_20240416.7z`). Grow to 500 over 6 months via flywheel.

---

## 15. Phased Delivery (8–10 Weeks MVP)

| Phase | Weeks | Deliverable |
|---|---|---|
| **0 — Foundation** | 1 | Provider abstractions, config system, eval harness with 20 gold queries |
| **1 — Static pipeline** | 2 | ApiModel.json parse → Step 1 classification, FK graph, APSP, basic NL→SQL with one LLM, CLI only |
| **2 — Auto sub-clustering** | 1.5 | Leiden inside large domains, LLM naming, multi-label assignment, cluster-level routing |
| **3 — Entity resolution** | 1 | 4-tier resolver integrated |
| **4 — Frontend MVP** | 2 | Query UI, results, charts, basic gold SQL CRUD |
| **5 — Cluster Manager** | 1 | Visual cluster editor, rebuild button |
| **6 — Hardening** | 1.5 | Multi-provider testing matrix, observability, eval automation, docs |

MVP at end of phase 4. Phases 5–6 are productionization.

### Two-week proof point (de-risk before phase 1)

Ingest Northridge → parse Ed-Fi domains via `ApiModel.json` → FK graph → 30 hand-authored gold queries → end-to-end CLI returning SQL with one LLM provider. No frontend, no auto sub-clustering, no entity resolution. Validates the hardest part (cluster + graph + retrieval orchestration) before investing in UI and provider abstractions. Measure eval; if execution accuracy >70%, scale up.

---

## 16. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| LLM provider drift (model versions change behavior) | Med | High | Pin model versions in config, weekly eval against pinned + latest, alert on divergence |
| Gold SQL rot as schema evolves | High | Med | Nightly job re-validates every gold query against current DB, auto-flags failures |
| Cluster boundary disputes | Low | Low | Multi-label assignment, top-K cluster routing (not top-1) |
| Provider cost explosion | Med | High | (a) cache by NL hash, (b) route trivial queries to Haiku, (c) hard per-user daily quota with admin override |
| Vector store lock-in | Low | Med | Standard Parquet export format, migration is a script |
| Composite FK silent failure | Med | High | Build-time grouping by `FK_Name`, MST dedup on logical edges only |
| Descriptor leakage in SQL | Med | Med | Heavy edge weight penalty (3×), measured in eval as separate metric |
| User schema diverges from public Ed-Fi DS | High | Low | Use user's actual `0030-ForeignKeys.sql` extract as source of truth, not just upstream repo |
| Cluster ID instability across rebuilds | Med | Med | Hungarian assignment between old/new clustering, preserve IDs where overlap >70% |
| Multi-label complexity in routing | Low | Low | Top-K routing already handles overlap; treat as feature not bug |

---

## 17. Concrete Numbers (Verified from DS 6.1.0)

### Coverage projection for 1048 tables

| Stage | Method | Estimated coverage |
|---|---|---|
| 1 | ApiModel direct | ~750 tables (72%) |
| 2 | Aggregate inheritance | +220 tables → 93% cumulative |
| 3 | Descriptor FK-referrer voting | +60 tables → 99% cumulative |
| 4 | LLM fallback | ~10 tables |

**99%+ coverage with deterministic parsing. LLM only touches the long tail.**

### Latency budget (p95)

| Stage | Budget | Notes |
|---|---|---|
| Cluster routing | 30ms | vector search + light reranking |
| Entity resolution | 10ms typical | Tier 1+2 dominate |
| Schema linking | 50ms | within-cluster only |
| Graph (Steiner) | <1ms | in-process mmap |
| Context assembly | 5ms | dict ops |
| LLM generation | 2000–4000ms | dominant cost |
| Validation | 100ms | EXPLAIN round-trip |
| Execution | varies | target DB dependent |
| Visualization (parallel) | 1000ms | LLM call, parallel with description |
| Description (parallel) | 800ms | LLM call |
| **Total p95** | **~7s** | LLM-bound |

### Storage footprint

| Artifact | Size |
|---|---|
| `dist.npy` | 4.4 MB |
| `next_hop.npy` | 2.2 MB |
| `edge_meta.msgpack` | ~5 MB |
| `graph.pkl` | ~3 MB |
| `table_classification.json` | ~2 MB |
| `clusters` collection (vectors + payloads) | ~50 MB |
| `tables` collection | ~150 MB |
| `gold_sql` collection (500 queries) | ~5 MB |
| `column_values` collection (typical) | ~200 MB |
| **Total per worker** | **<500 MB** |

Fits comfortably in a 2GB pod with headroom.

---

## 18. Inputs Required to Start

Run on your DB:

```sql
-- 1. Confirm Data Standard version
SELECT * FROM dbo.DeployJournal ORDER BY Applied DESC;
-- or
SELECT * FROM edfi.Version;

-- 2. List all schemas
SELECT DISTINCT TABLE_SCHEMA, COUNT(*) AS table_count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
GROUP BY TABLE_SCHEMA
ORDER BY table_count DESC;

-- 3. Confirm 1048 figure
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';

-- 4. List installed extensions (state-specific)
SELECT name FROM sys.schemas WHERE name NOT IN ('dbo','sys','INFORMATION_SCHEMA');
```

Send back:
1. **Data Standard version** (4.0 / 5.2 / 6.1) — pin matching `ApiModel.json`
2. **Extensions installed** — TPDM yes/no, state-specific yes/no
3. **Distinct schemas** in your 1048 count
4. **Target SQL engine** (Snowflake / Postgres / SQL Server) — affects M-Schema dialect
5. **Provider preferences** — primary LLM, embedding, vector store
6. **Gold SQL** — even 10–20 queries bootstraps eval

---

## Appendix A — File Paths in Ed-Fi GitHub Repos

### Ed-Fi-Alliance-OSS/Ed-Fi-ODS

```
Application/EdFi.Ods.Standard/Standard/<DS_version>/Artifacts/
  Metadata/
    ApiModel.json                                # AUTHORITATIVE table-domain map
    DatabaseViews.generated.json
  MsSql/Structure/Ods/
    0010-Schemas.sql
    0020-Tables.sql
    0030-ForeignKeys.sql                         # ALL FKs with constraint names
    0040-IdColumnUniqueIndexes.sql
    0050-ExtendedProperties.sql
  PgSql/Structure/Ods/                           # PostgreSQL equivalents
    0030-ForeignKeys.sql
  Schemas/                                       # XSD schemas
```

### Ed-Fi-Alliance-OSS/Ed-Fi-Extensions

```
Extensions/EdFi.Ods.Extensions.TPDM/Versions/<v>/Standard/<DS>/Artifacts/
  Metadata/
    ApiModel-EXTENSION.json                      # TPDM domain map
  MsSql/Structure/Ods/
    0030-EXTENSION-TPDM-ForeignKeys.sql
Extensions/EdFi.Ods.Extensions.Homograph/...
Extensions/EdFi.Ods.Extensions.Sample/...
```

### Ed-Fi-Alliance-OSS/Ed-Fi-Data-Standard

```
Schemas/Bulk/
  Interchange-StudentEnrollment.xsd              # functional grouping (secondary signal)
  Interchange-Assessment*.xsd
  ... 31 interchanges total
Descriptors/
  *.xml                                          # 266 default descriptor value sets
Models/
  Ed-Fi Unifying Data Model UML.pdf              # documentation only
```

### Ed-Fi-Alliance-OSS/MetaEd-js

Code generator. Test fixtures contain `.metaed` examples. Actual core data standard model is bundled inside the VS Code extension `ed-fialliance.vscode-metaed-ide` under `node_modules/@edfi/ed-fi-model-X.Y/`.

---

## Appendix B — Domain Inventory (DS 6.1.0)

All 35 domains by entity count, descending:

| # | Domain | Entity count |
|---|---|---|
| 1 | TeachingAndLearning | 165 |
| 2 | SectionsAndPrograms | 143 |
| 3 | StudentAcademicRecord | 141 |
| 4 | AlternativeAndSupplementalServices | 137 |
| 5 | Staff | 122 |
| 6 | Enrollment | 122 |
| 7 | Survey | 105 |
| 8 | SpecialEducation | 100 |
| 9 | Assessment | 86 |
| 10 | StudentCohort | 85 |
| 11 | Graduation | 81 |
| 12 | ReportCard | 77 |
| 13 | Intervention | 73 |
| 14 | StudentAttendance | 73 |
| 15 | StudentIdentificationAndDemographics | 70 |
| 16 | RecruitingAndStaffing | 68 |
| 17 | StudentTranscript | 66 |
| 18 | EducationOrganization | 66 |
| 19 | EducatorPreparationProgram | 63 |
| 20 | CourseCatalog | 63 |
| 21 | SchoolCalendar | 60 |
| 22 | Finance | 59 |
| 23 | Gradebook | 56 |
| 24 | Discipline | 55 |
| 25 | BellSchedule | 55 |
| 26 | AssessmentMetadata | 50 |
| 27 | AssessmentRegistration | 42 |
| 28 | PerformanceEvaluation | 41 |
| 29 | Credential | 31 |
| 30 | SpecialEducationDataModel | 31 |
| 31 | StudentAssessment | 30 |
| 32 | StudentHealth | 28 |
| 33 | StudentProgramEvaluation | 21 |
| 34 | Path | 14 |
| 35 | Standards | 10 |

Plus from TPDM extension: **TeacherPreparation** (~31 entities).

---

## Appendix C — Sample Classifier Output (verified)

```json
{
  "schema": "edfi",
  "table": "StudentSchoolAssociation",
  "all_domains": ["Enrollment", "Graduation", "SchoolCalendar", "TeachingAndLearning", "SectionsAndPrograms"],
  "primary_domain": "Enrollment",
  "secondary_domain": "Graduation",
  "is_descriptor": false,
  "is_association": true,
  "source": "apimodel_direct",
  "confidence": 1.0
}

{
  "schema": "edfi",
  "table": "StudentSchoolAssociationAlternativeGraduationPlan",
  "all_domains": ["Enrollment", "Graduation"],
  "primary_domain": "Enrollment",
  "secondary_domain": "Graduation",
  "is_descriptor": false,
  "is_association": false,
  "source": "aggregate_inheritance",
  "confidence": 0.95
}

{
  "schema": "edfi",
  "table": "GradeLevelDescriptor",
  "all_domains": ["Enrollment", "StudentAcademicRecord"],
  "primary_domain": "Enrollment",
  "secondary_domain": "StudentAcademicRecord",
  "is_descriptor": true,
  "is_association": false,
  "source": "descriptor_inheritance",
  "confidence": 0.85
}

{
  "schema": "tpdm",
  "table": "Candidate",
  "all_domains": ["TeacherPreparation"],
  "primary_domain": "TeacherPreparation",
  "secondary_domain": null,
  "is_descriptor": false,
  "is_association": false,
  "is_extension": true,
  "source": "apimodel_direct",
  "confidence": 1.0
}
```

---

**End of plan. Ready to implement Phase 0.**
