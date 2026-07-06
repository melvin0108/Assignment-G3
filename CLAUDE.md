# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

NAB **"TAC@NABVNSC22"** Core Data Engineer assignment. Build a **batch data pipeline** that turns mock banking data into trusted, governed, **AI-ready context** for an internal AI assistant (Banker.AI / Customer.AI). The pipeline must enforce **Zero Trust AI** at the data layer — *before* any AI consumer is allowed to read the context.

**Chosen scenario → Transaction investigation context:** mock transactions, disputes/chargebacks, merchant categories, fraud alerts, and investigation notes.

The requirements live in:
- `Requirements.md` — the official brief (English).
- `Requirements-notes.md` — Vietnamese summary of the recorded briefing (`G3 - Data engineer.m4a`).

Treat those two files as the source of truth. The two pipeline-side doc files are also authoritative for their layer:
- `docs/data-model.md` — full table/field/PII/DQ map (contracts, enums, masking matrix, quarantine schema, gold grain). **Keep `mock/config.py` in sync with this doc** — it is the single source of truth for column order, enums, ID prefixes, generation order, and volumes.
- `docs/bronze-layer.md` — Bronze contracts, sample rows, and the Databricks ingestion picture.
- `docs/schema-adjustment-brief.md` — the 10 CTAs (naming, contracts, metadata columns, enums, masking, quarantine schema, gold grain) the schema was tightened against.

## Implementation status

The repo is **partially built**:
- ✅ **Mock data generator** (`mock/`, Python + Faker) — implemented and working. Generates the 25 Bronze source CSVs + `_defects_manifest.csv` into `data/raw/`.
- ⬜ **Silver / Gold pipeline** (Databricks + dbt or DLT), **DQ rule engine**, **quarantine output**, **masking/access policy enforcement**, **metadata/lineage output**, **automated tests**, and a **Makefile** — *planned, not yet present*. No `data/silver`, `data/gold`, `data/quarantine`, `dbt/` or `tests/` directories exist yet. The `make ...` targets below are the intended interface once those layers land.

## Hard requirements (non-negotiable, graded in Acceptance Criteria)

The finished pipeline MUST include all of:
- ≥ **3 input datasets**; ≥ **20–30 mock tables** total, each containing intentional data-quality defects (the 25 mocked tables already satisfy this).
- Layers: **raw (bronze) → silver (cleaned/conformed) → gold (curated, AI-ready)**.
- ≥ **8 data-quality rules**, executable (not just described). The mock layer already injects far more than 8 (see DQ convention below).
- ≥ **1 quarantine output** for failed records, with failure rule + reason + disposition.
- ≥ **1 PII masking/redaction/tokenisation step** before the AI-ready output.
- ≥ **1 metadata/lineage output** (source → field traceability).
- ≥ **1 automated test / executable validation command**.
- A way to **rerun from a clean state**.
- Batch ETL (it's mock data). **No real customer data, no confidential NAB data, no personal AWS accounts** — everything runs on a personal machine (Databricks Free Edition is the only cloud-ish option the mentor endorsed).

Injected DQ defects must cover: missing required fields, invalid values, duplicates, stale/outdated records, inconsistent status values, referential-integrity breaks, sensitive fields needing masking, and records that must never reach an AI consumer. The full coverage checklist is `docs/data-model.md` §9.

## Architecture — medallion, with explicit layer contracts

| Layer | Zone | Contract | Who can read |
|---|---|---|---|
| **Raw** | bronze | Source files ingested **as-is**, append-only, immutable. Schema validation only; nothing is dropped. Every record keeps `_source_file`, `_ingest_ts`, `_run_id` (full metadata column set in `bronze-layer.md` §4). | pipeline only |
| **Silver** | cleaned | Conformed, deduplicated, typed, RI-repaired-where-safe. Failed rows diverted to quarantine. PII masked per the matrix. | pipeline + internal roles |
| **Gold** | curated (AI-ready) | Denormalised **context documents** (`investigation_context`, one row per `case_id`, `legal_hold` excluded) + masked facts. Zero unmasked PII. Each row carries quality status, masking status, source refs, context version, last-refreshed. | AI consumer + role-filtered |

The same physical row appears three times (raw → silver → gold) with a stable lineage chain so an AI answer can always be traced back to its source.

## Platform & stack (decision: Databricks)

- **Platform:** **Databricks (Free Edition)** — chosen by the team. **No AWS / no personal cloud accounts.** Unified catalog via **Unity Catalog** (`catalog.schema.table`, e.g. `tx_inv.bronze.transactions`); source files land in UC **Volumes** (`/Volumes/tx_inv/landing/<table>/`).
- **Medallion on Databricks:** Bronze → Silver → Gold Delta tables. **Batch ETL** (per briefing). Ingestion via `COPY INTO` (idempotent) or Auto Loader `trigger(availableNow=True)`; managed option via **Delta Live Tables (DLT)** for free lineage + retries + `EXPECT ... ON VIOLATION` quarantine.
- **Mock data generation:** **Python + Faker** (fixed seed → reproducible defects), output as CSV into the landing Volumes.
- **DQ:** DLT expectations / custom checks; optionally Great Expectations where a rule can't be expressed inline.
- **Access/PII:** Unity Catalog **column masks** and **dynamic views** enforce `internal_only` vs `ai_allowed` field visibility.

## Mock data generator (the part that exists)

The only executable component today. Generates the Bronze source CSVs defined in `docs/data-model.md` with **intentional DQ defects** baked in for the downstream pipeline to catch. Deterministic (seeded) and streaming-safe so `transactions` can scale to ~2M rows.

### Run

```bash
pip install -r requirements.txt            # only dep: Faker

python -m mock.generate                    # default: transactions=50,000 -> data/raw/
python -m mock.generate --transactions 2000 --customers 150   # fast demo
python -m mock.generate --scale 10         # 10x all volumes
python -m mock.generate --stress           # ~2,000,000 transactions (streaming)
python -m mock.generate --tables customers,accounts,transactions   # subset only
python -m mock.generate --defect-rate 0.15 --seed 42 --out data/raw
```

Key args: `--seed` (default 42, same seed ⇒ identical data + defects), `--out` (default `data/raw`), `--customers`, `--transactions`, `--scale`, `--defect-rate` (0–1, default 0.05), `--stress`, `--tables` (comma subset), `--no-manifest`, `--quiet`.

Output: one `<table>.csv` per table (column order matches the contracts) plus `_defects_manifest.csv` — **every intentionally-injected bad record** with `source_table, record_key, rule_id, rule_name, failure_reason, severity`. This manifest is the **ground truth** for validating the Silver `quarantine_records` output (expected vs actual failures).

### Generator architecture (read multiple files to change correctly)

```
mock/
  config.py     # enums, ID prefixes, reference data, TABLE_SCHEMAS (column order),
                #   GENERATION_ORDER, BASE_VOLUMES, RUN_DATE  — keep in sync with data-model.md
  helpers.py    # Faker/RNG setup + value helpers (masked_pan, full_pan, tax_id, iso, ...)
  defects.py    # DefectManifest (collects + writes _defects_manifest.csv)
  generators.py # one gen_<table>() per table + the GENERATORS registry
  generate.py   # CLI: argparse, count derivation, CSV streaming, summary
```

- **`Ctx` dataclass** (`generators.py`) threads everything through every generator: `f` (Faker), `rng` (random.Random), `manifest` (DefectManifest), `defect_rate`, `counts`, `ids` (table → list of ids), `pools` (cross-table lookups). Helpers `ctx.defect_count(n, weight, min_count)` and `ctx.sample_indices(n, k)` make defect placement deterministic.
- **Parents before children:** `GENERATION_ORDER` (in `config.py`) generates reference dims → customers/employees → accounts/cards → merchants → transactions → facts → disputes/cases → bridges/notes. Parent generators stash their IDs in `ctx.ids`/`ctx.pools` so children emit **referentially-clean FKs by default**; defects then deliberately break some.
- **Two generator shapes:** small/lookup tables `return` a list; large facts (`transactions`, `auth_attempts`, `transaction_devices`, `fraud_alerts`) are **generator functions that `yield`** so millions of rows stream to disk without being held in memory. Every generator's signature is `gen_<table>(ctx, n)` and emits dict rows whose keys match `config.TABLE_SCHEMAS[<table>]`. `generate.py` writes via a streaming `csv.DictWriter`.
- **Volume derivation:** `build_counts()` (`generate.py`) derives the full per-table volume map from two knobs (`--customers`, `--transactions`) plus `--scale` — accounts ≈ 1.5×customers, cards ≈ 1.2×accounts, auth_attempts ≈ 1.2×transactions, disputes ≈ 2% of transactions, cases ≈ 0.1%, etc.
- **Determinism:** `make_faker(seed)` calls `Faker.seed(seed)`; a **separate** `random.Random(seed+1)` drives numeric helpers. `RUN_DATE = 2026-07-06` is pinned (`config.py`) so time-based defects (future timestamps, stale cases) are identical regardless of when generation runs.
- **Sentinel orphan IDs** (`config.py`): `CUST-9999`, `ACC-9999`, `CARD-9999`, `TXN-999999` are guaranteed-not-to-exist IDs used to inject referential-integrity breaks. Reuse these when adding new RI defects; do not generate real rows with them.

## Mock data model

Full inventory and field-level contracts in **`docs/data-model.md`**. Summary:

- **25 mocked domain tables:** customers/accounts/cards; reference dims (`merchant_categories`, `channels`, `case_status_types`, `dispute_reason_codes`, `fraud_types`, `countries`, `currencies`, `branches`, `date_dim`, `employees`, `merchants`); facts/events (`transactions` — stress target, ~2M/hr, `auth_attempts`, `transaction_devices`); investigation (`disputes`, `chargebacks`, `fraud_alerts`, `investigation_cases`, `investigation_notes`, `case_transactions`, `case_parties`, `customer_contact_logs`).
- **7 governance tables (emitted by the pipeline, NOT mocked):** `dq_rule`, `dq_result`, `quarantine_record`, `pipeline_run`, `metadata_lineage`, `masking_policy`, `access_policy`.

## Zero Trust AI / privacy conventions

- **Masking policy** (`masking_policy`, generated from the matrix in `docs/data-model.md` §6): per sensitive field → `mask` / `hash` / `tokenize` / `drop` / `generalize`. Bronze keeps raw; Silver masks; Gold redacts/excludes.
- **Access policy** (`access_policy`): per field → `internal_only` / `customer_facing` / `ai_allowed`. Gold output includes **only `ai_allowed`** fields.
- **Never reach AI:** records under `legal_hold`, Suspicious Activity Report (SAR) flags, free-text notes that failed sensitive-data leakage scans, and anything below the quality threshold.
- Every gold row carries: `source_references`, `run_id`, `last_refreshed_at`, `quality_status`, `masking_status`, `context_version`, and `warning_flags` (e.g. `stale_case`, `partial_data`, `redacted_notes`) when relevant.
- Document, alongside the gold output: example prompts the AI **can** answer from this context, and prompts it **must refuse** (missing / restricted / unsafe context).

## DQ convention

Each rule is a row in `dq_rule` (`rule_id`, `layer`, `target_table`, `severity ∈ {reject, quarantine, warn}`, expression/sql). Outcome → `dq_result` (pass/fail counts, sample failed keys); individual failed rows → `quarantine_record` with `(record_key, rule_id, failure_reason, disposition)`. Disposition ∈ `rejected | quarantined | masked | allowed_with_warning`. Quarantine schema is fixed in `docs/data-model.md` §7.

**Rule ID naming** (already used by the mock layer, keep consistent when adding Silver rules): `DQ-<TABLE>-<CHECK>`, e.g. `DQ-TXN-AMT-POS`, `DQ-CUST-EMAIL-FMT`, `DQ-CARD-PAN-LEAK`, `DQ-ACC-CUST-FK`. Every `man.add(...)` call in `generators.py` is paired with one of these — the set of distinct rule IDs injected there is the minimum the Silver engine must implement.

## Intended command interface (dbt/pipeline layers — not yet implemented)

```bash
make mock       # python -m mock.generate            → seeded mock files -> data/raw (+ _defects_manifest.csv)
make ingest     # dbt run --select tag:raw           -> raw layer        ⬜ planned
make build      # dbt build                          -> silver + gold + tests   ⬜ planned
make test       # dbt test && pytest tests/          -> DQ rules + unit tests   ⬜ planned
make dq-report  # render dq_result/quarantine summary -> DQ evidence      ⬜ planned
make clean      # drop dbt targets + clear data/silver data/gold data/quarantine  ⬜ planned
make inspect-quarantine   # quick view of quarantined records                  ⬜ planned
```

Today only `make mock`'s underlying command works (run `python -m mock.generate` directly — there is no `Makefile` yet).

## Intended repository layout

```
mock/                 # ✅ Faker-based generator + config (source of mock data)
data/
  raw/                # ✅ layered bronze outputs (gitignore the big files, keep samples)
  silver/ gold/ quarantine/   # ⬜ planned layered outputs
dbt/  or  pipeline/   # ⬜ dbt models, schema.yml (contracts), tests
tests/                # ⬜ pytest unit tests (schema-drift, business-rule violations)
docs/
  data-model.md       # ✅ AI-ready context: full table/field/PII/DQ map
  bronze-layer.md     # ✅ bronze contracts + Databricks ingestion
  schema-adjustment-brief.md  # ✅ the 10 schema CTAs
  data-contracts/     # ⬜ one contract per source dataset
  runbook.md          # ⬜ how to run + verify (required deliverable)
```

## Conventions

- **Table/layer prefixes:** source tables are unprefixed and **plural** (`customers`, not `customer`) — the schema-adjustment-brief §1 mandates a single convention across source/bronze/silver/gold/DQ/lineage. Pipeline models use `raw__`, `silver__`, `gold__` (or dbt folders `raw/`, `silver/`, `gold/`).
- **Identifiers:** stable string keys (`customer_id`, `transaction_id`, `case_id`), built from `config.PFX` prefixes via `helpers.seq_id`. Surrogate hash keys only in silver/gold.
- **Determinism:** all generators take a seed; reruns from the same seed reproduce the same defects (essential for tests).
- **Timestamps:** store as ISO-8601 UTC (`helpers.iso` → `%Y-%m-%dT%H:%M:%SZ`); never rely on local time. Dates via `helpers.iso_date`.
- **PII is never committed** unmasked; `data/raw` is git-ignored except for a small masked sample kept under `data/sample/`.
