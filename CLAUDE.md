# CLAUDE.md

Guidance for Claude Code when working in this repository. Keep this file lean — layer-specific detail lives in `mock/CLAUDE.md`, `pipeline/CLAUDE.md`, and `docs/`.

## What this project is

NAB **"TAC@NABVNSC22"** Core Data Engineer assignment. Build a **batch data pipeline** that turns mock banking data into trusted, governed, **AI-ready context** for an internal AI assistant (Banker.AI / Customer.AI). The pipeline must enforce **Zero Trust AI** at the data layer — *before* any AI consumer is allowed to read the context.

**Chosen scenario → Transaction investigation context:** mock transactions, disputes/chargebacks, merchant categories, fraud alerts, and investigation notes.

**Source of truth:**
- `Requirements.md` (official brief, EN) + `Requirements-notes.md` (Vietnamese summary of the recorded briefing).
- `docs/data-model.md` — full table/field/PII/DQ map (contracts, enums, masking matrix, quarantine schema, gold grain). **Keep `mock/config.py` in sync with it.**
- `docs/bronze-layer.md` — Bronze contracts, sample rows, Databricks ingestion.
- `docs/schema-adjustment-brief.md` — the 10 schema CTAs.

## Where things live (read the matching file before editing that layer)

- **`mock/CLAUDE.md`** — Faker generator: run commands, architecture, rules for adding/changing tables & defects.
- **`pipeline/CLAUDE.md`** — bronze notebook gotchas + the DQ convention (rule IDs, quarantine flow).
- **`G3_Workflow.md`** — team process (branching, PRs, Databricks Repos, catalog dropdown).

## Implementation status

- ✅ **Mock data generator** (`mock/`, Python + Faker) — working. 25 Bronze CSVs + `defects_manifest.csv` → `data/raw/`.
- ✅ **Bronze ingestion** (`pipeline/bronze/bronze_*.py`) — 26 PySpark notebooks.
- 🟡 **Silver** — partial (`pipeline/silver/silver_*.py` + SQL). **Gold, DQ engine, quarantine, masking/access enforcement, lineage, automated tests, Makefile** — planned/partial.

Today only `python -m mock.generate` runs end-to-end; the `make …` targets are intended but not built.

## Hard requirements (non-negotiable, graded in Acceptance Criteria)

- ≥ **3 input datasets**; ≥ **20–30 mock tables** with intentional DQ defects (the 25 mocked tables satisfy this).
- Layers: **raw (bronze) → silver (cleaned) → gold (curated, AI-ready)**.
- ≥ **8 executable DQ rules** (mock injects far more).
- ≥ **1 quarantine output** (failure rule + reason + disposition); ≥ **1 PII masking step** before AI output; ≥ **1 metadata/lineage output**; ≥ **1 automated test**; a way to **rerun from a clean state**.
- Batch ETL, mock data only — **no real customer/NAB data, no personal AWS accounts** (Databricks Free Edition, mentor-endorsed).

Defect coverage must include: missing fields, invalid values, duplicates, stale records, inconsistent statuses, RI breaks, sensitive fields needing masking, and records that must never reach an AI consumer (full checklist: `docs/data-model.md` §9).

## Architecture — medallion

| Layer | Zone | Contract | Who can read |
|---|---|---|---|
| **Raw** | bronze | ingested **as-is**, append-only, immutable; schema validation only, nothing dropped | pipeline only |
| **Silver** | cleaned | conformed, deduped, typed, RI-repaired; failed rows → quarantine; PII masked | pipeline + internal |
| **Gold** | curated | denormalised **context docs** (`investigation_context`, one row per `case_id`, `legal_hold` excluded); zero unmasked PII | AI consumer + role-filtered |

Same physical row appears three times (raw→silver→gold) with a stable lineage chain so any AI answer traces back to source.

## Platform & stack (Databricks Free Edition)

- **Databricks + Unity Catalog** (`catalog.schema.table`); source CSVs in UC **Volumes**. No AWS / personal cloud.
- Bronze→Silver→Gold Delta tables; **batch ETL**. Ingest via `COPY INTO` (idempotent) or Auto Loader; DLT optional for lineage/retries/`EXPECT … ON VIOLATION` quarantine.
- **Mock:** Python + Faker (seeded → reproducible defects). **DQ:** DLT expectations / custom checks (GE where needed). **Access/PII:** UC column masks + dynamic views enforce `internal_only` vs `ai_allowed`.

## Environments & team workflow

Full process in **`G3_Workflow.md`**. Three Unity Catalogs map to stages:

| Stage | Branch | Catalog | Purpose |
|---|---|---|---|
| DEV | personal (`melvin`, …) | `g3_dev` | notebook syntax + basic logic |
| TEST | personal | `g3_test` | business validation w/ team/QA |
| PROD | `main` (post-merge) | `g3_catalog` | official load |

⚠️ **Catalog switching isn't wired up yet** — every notebook hardcodes `CATALOG = "g3_dev"` (no widget); the catalog-widget refactor is **planned, deferred**. `g3_dev` is the correct DEV default meanwhile. Per the workflow, new work goes on a personal branch, not straight to `main`.

## Zero Trust AI / privacy

- **Masking** (`docs/data-model.md` §6): per sensitive field → `mask` / `hash` / `tokenize` / `drop` / `generalize`. Bronze raw → Silver masked → Gold redacted/excluded.
- **Access:** Gold includes **only `ai_allowed`** fields (`internal_only` / `customer_facing` / `ai_allowed`).
- **Never reach AI:** `legal_hold`, SAR flags, notes failing leakage scans, anything below the quality threshold.
- Every gold row carries `source_references`, `run_id`, `last_refreshed_at`, `quality_status`, `masking_status`, `context_version`, and `warning_flags` (e.g. `stale_case`, `partial_data`, `redacted_notes`).

## Data model

25 mocked domain tables (customers/accounts/cards; reference dims; facts `transactions`/`auth_attempts`/`transaction_devices`; investigation cases/notes/bridges) + 7 governance tables **emitted by the pipeline, not mocked** (`dq_rule`, `dq_result`, `quarantine_record`, `pipeline_run`, `metadata_lineage`, `masking_policy`, `access_policy`). **Full field-level contracts: `docs/data-model.md`.**

## Conventions

- **Naming:** source tables unprefixed & **plural** (`customers`). One convention across all layers (schema-adjustment-brief §1). Pipeline models: `raw__`/`silver__`/`gold__`.
- **Identifiers:** stable string keys (`customer_id`, …) via `helpers.seq_id` from `config.PFX`; surrogate hash keys only in silver/gold.
- **Determinism:** all generators seeded; same seed ⇒ same defects (essential for tests).
- **Timestamps:** ISO-8601 UTC (`helpers.iso` → `%Y-%m-%dT%H:%M:%SZ`); never local time.
- Raw PII is synthetic, git-ignored (`data/raw`); masked samples belong under `data/sample/`.
