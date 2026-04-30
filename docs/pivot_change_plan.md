# Pivot — Self-Contained Ingest from Target DB + Operator CSVs

**Goal:** drop the Ed-Fi GitHub dependency entirely. Catalog inputs come from
the operator's environment: two operator-supplied CSVs plus the live target DB.

## Inputs (replacing ApiModel.json + 0030-ForeignKeys.sql)

### 1. Schema CSV — table & domain mapping
Columns from the operator screenshots:
```
Ranking | Domain | TABLE_SCHEMA | TABLE_NAME | COLUMN_NAME | Populated
```
- One row per (schema, table, column).
- `Domain` carries the routing bucket (e.g. `Descriptor`, `Student`).
- `Populated` (Yes/No) gates whether we sample values for that column.

### 2. Relationships CSV — FK graph
```
FK_Name | Parent_Table | Parent_Column | Referenced_Table | Referenced_Column |
Parent_Schema | Referenced_Schema
```
- Composite FKs grouped by `FK_Name`.
- Replaces every consumer of `0030-ForeignKeys.sql`.

### 3. Target DB itself — enrichment
LLM + sampling generates what ApiModel used to ship:
- Table descriptions (sample N rows + name + domain → LLM)
- Per-column unique values (top-N distinct, only when `Populated=Yes`)
- Column descriptions (column name + samples + populated flag → LLM)

---

## Code changes — concrete file map

### A. Delete / deprecate

| File | Action |
|---|---|
| `packages/backend/src/text2sql/ingestion/edfi_fetcher.py` | **Delete.** GitHub raw fetcher no longer needed. |
| `packages/backend/src/text2sql/ingestion/__init__.py` | **Rename module → `text2sql.catalog_inputs`** (re-export the new CSV loader). |
| `scripts/pull_edfi_dataset.py` | Delete or repurpose for sample CSVs. |
| `data/edfi/core/`, `data/edfi/manifest.json` | Stop being build inputs (path stays for the demo SQLite only). |
| `text2sql ingest` CLI command | Replace with `text2sql ingest-csvs --schema <path> --relationships <path>`. |

### B. New modules

| File | Purpose |
|---|---|
| `packages/backend/src/text2sql/catalog_inputs/schema_csv.py` | Parse the Schema CSV → `list[ColumnRow]` (schema/table/column/domain/populated). |
| `packages/backend/src/text2sql/catalog_inputs/relationships_csv.py` | Parse the Relationships CSV → `list[FKEdge]` (the same dataclass `graph/fk_parser.py` already exports). Composite FKs grouped by `FK_Name`. |
| `packages/backend/src/text2sql/catalog_inputs/loader.py` | `CatalogInputs` aggregator that yields `(domains, tables, columns, fks)` — the sole upstream contract for the catalog builder. |
| `packages/backend/src/text2sql/table_catalog/dbsample_enricher.py` | Sample N rows + top-K distinct values per column; cache per provider. |
| `packages/backend/src/text2sql/table_catalog/llm_descriptions.py` | Table-level + column-level description prompts; takes (name, domain, samples) → text. Replaces the ApiModel-prose path inside `description_generator.py`. |

### C. Refactor in place

| File | Change |
|---|---|
| `packages/backend/src/text2sql/classification/metadata.py` | Remove `IngestionManifest` import. Build `TableMetadata` from `CatalogInputs` instead. `apimodel_domain_hints` → `csv_domain` (single value, not a list). |
| `packages/backend/src/text2sql/classification/table_mapping.py` | Stages 1–3 collapse: the operator CSV already carries domain — Stage 1 is now a direct lookup. Stage 4 (LLM) survives as the fallback for rows the CSV left blank. |
| `packages/backend/src/text2sql/classification/catalog.py` | `DomainCatalog.build()` consumes `CatalogInputs.domains` (distinct values from the CSV) instead of the hard-coded 35-list. Domain ranking comes from the `Ranking` column. |
| `packages/backend/src/text2sql/graph/fk_parser.py` | Keep `FKEdge`. Add a new constructor `from_relationships_csv(rows)` next to the existing `from_reflected()`. Drop the regex-based 0030-ForeignKeys.sql parser. |
| `packages/backend/src/text2sql/graph/builder.py` | Already provider-isolated (N3). Just change the input source — no behavior change. |
| `packages/backend/src/text2sql/table_catalog/catalog_builder.py` | Top of file: replace `IngestionManifest` argument with `CatalogInputs`. Remove the ApiModel-fallback branch. Wire `dbsample_enricher` + `llm_descriptions` into `_build_table_entry`. The `reflect_unknown_tables` path stays — it now handles the Δ between the CSV and the live DB. |
| `packages/backend/src/text2sql/table_catalog/description_generator.py` | Drop the "ApiModel already has descriptions for 100%" assumption. Every description goes through the LLM path now (with a cache). |
| `packages/backend/src/text2sql/cli.py` | Replace `ingest` with `ingest-csvs`. `rebuild` orchestrator stages: `ingest-csvs` → `sample-target-db` → `classify` → `build-fk-graph` → `build-table-catalog-cmd` → `index-catalog` → `gold-seed`. |
| `packages/backend/src/text2sql/api/admin.py` | `/admin/jobs/rebuild` `_STAGE_COMMANDS` updated to match the new stage list. New endpoints: `POST /admin/catalog_inputs/upload` (multipart for the two CSVs) + `GET /admin/catalog_inputs` (return what's currently registered). |
| `configs/default.yaml` | Drop the `ed_fi:` section (`data_standard_version`, `extensions`, `github`). Add a new `catalog_inputs:` section pointing at the two CSV paths (or metadata-DB tables). |
| `configs/domains.yaml` | Delete — domain list is now data, not config (Q8). |

### D. Frontend changes

| File | Change |
|---|---|
| `packages/frontend/app/settings/page.tsx` | New card: "Catalog inputs" — two file-upload widgets (Schema CSV, Relationships CSV) + a "use metadata-DB tables instead" switch. Calls the new admin endpoints. |
| `packages/frontend/components/OnboardingBanner.tsx` | Banner copy updated: "Connect your DB and upload your two CSVs to begin" instead of "run ingest". |
| `packages/frontend/lib/api.ts` | Type definitions for the new endpoints (`CatalogInputsStatus`, `UploadCsvRequest`). |
| `packages/frontend/app/page.tsx`, `chat/page.tsx`, etc. | No code changes — they consume `/health` + `/tables`, which are already provider-aware. |

### E. Test changes

| File | Change |
|---|---|
| `packages/backend/tests/test_component1_ingestion.py` | Replace with `test_csv_inputs.py` — covers schema CSV parser, relationships CSV parser, composite-FK grouping, validation errors. |
| `packages/backend/tests/test_component2_classification.py` | Update fixtures: instead of a fake ApiModel.json, build `CatalogInputs` from inline CSV rows. Stage-1 test becomes a direct-lookup test. |
| `packages/backend/tests/test_component3_graph.py` | Swap `parse_fks(0030...sql)` fixtures for `from_relationships_csv(rows)`. FKEdge shape unchanged so downstream Steiner tests stay green. |
| `packages/backend/tests/test_component4_table_catalog.py` | Swap inputs from `IngestionManifest` to `CatalogInputs`. Add new tests for `dbsample_enricher` (mock target DB) and `llm_descriptions` (mock LLM). |
| `packages/backend/tests/test_catalog_reflect_unknown.py` | Already target-DB-driven; no change. Keep as the safety net for tables the operator CSV missed. |

---

## Sequencing (six-week plan, integrates with the existing v0.9 schedule)

| Week | Focus |
|---|---|
| **Week 1** (Apr 30 + May 4–8) | Q1, Q2 — write the two CSV parsers + tests; freeze the on-disk format. |
| **Week 2** (May 11–15) | Q3 — refactor catalog_builder + classification + graph to consume `CatalogInputs`. Delete the GitHub fetcher. |
| **Week 3** (May 18–22) | Q4, Q5, Q6 — target-DB-driven enrichment (samples → table desc, top-N distinct values, LLM column descriptions). Per-provider caching. |
| **Week 4** (May 25–29) | Q7 — Settings UI for CSV upload + admin endpoints; rebuild orchestrator stages renamed. |
| **Week 5** (Jun 1–5) | Q8 — sweep every Ed-Fi-named string out of code (domain list, prompts, system messages); migrate eval suite to pivot-shape inputs. |
| **Week 6** (Jun 8–12) | End-to-end smoke on the operator's actual DB; recorded demo against the new flow; release prep. |
| **Fri Jun 13** | v0.9 release tag. |

---

## Open questions (need operator answer)

1. CSV transport: file upload via Settings UI, or are these two tables the operator already maintains in the metadata DB? (Affects Q7.)
2. Domain ranking: does `Ranking=0` mean primary, or highest priority? (Affects classification ordering.)
3. `Populated` flag granularity: is it per-column or per-(table, column)? Sample shows per-column rows, so we treat it as authoritative per row.
4. Composite FK detection: the screenshot's `FK_Name` looks unique per relationship. Confirm grouping by `FK_Name` is sufficient (vs `Parent_Table + FK_Name`).
5. Refresh cadence: when the operator's DB schema evolves, do they re-export both CSVs, or is there a delta-update flow?
