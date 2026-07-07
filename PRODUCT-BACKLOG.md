# Product Backlog — NAB Transaction-Investigation Pipeline

| | |
|---|---|
| **Project** | NAB Core Data Engineer assignment — "TAC@NABVNSC22" |
| **Scenario** | Transaction investigation (transactions, disputes/chargebacks, merchants, fraud alerts, investigation notes) |
| **Goal** | Turn mock banking data into trusted, governed, **Silver-layer AI-ready context**, enforcing **Zero Trust AI at the data layer** before any AI consumer may read it |
| **Platform** | Databricks Free Edition + Unity Catalog (catalog `tx_inv`; schemas `bronze` / `silver` / `gov`; landing Volume `/Volumes/tx_inv/landing/<table>/`) |
| **Status** | Mock data layer ✅ complete · Pipeline ⬜ to build |
| **Last updated** | 2026-07-07 |

### Team & status key
**Rule: each task is assigned to exactly one member at a time** (pairing is allowed, but every row has one accountable owner).

**Status values:** `To Do` · `In Progress` · `Blocked` · `Review` · `Done`  *(all tasks start at `To Do`)*

**Members (placeholders — replace with real names):**

| ID | Overall pipeline ownership | Specific responsibilities | Main handoff / deliverable |
|---|---|---|---|
| **M1** | Owns the platform foundation and the main pipeline flow from raw files to final AI-ready Silver context. | Create the Databricks catalog, schemas, landing volume, upload helper, Bronze ingestion, pipeline run tracking, orchestration job, and final `silver.investigation_context_ai` view. | A runnable Databricks job that can ingest source CSVs, build Bronze, track each run, and produce the final AI-safe context surface. |
| **M2** | Owns data-quality rules, quarantine, governance evidence, and access policy metadata. | Build the 36-rule registry, run each rule as an executable failure query, write failed records to quarantine, populate DQ results, and seed field-level access policy tables. | Governance tables that explain what failed, why it failed, what users are allowed to see, and which records are blocked from AI use. |
| **M3** | Owns Silver cleansing and privacy controls for people and account data. | Transform customers, employees, accounts, and cards into typed Silver tables; tokenize or mask personal data; install column masks; emit lineage for masked and AI-bound fields. | Silver tables for customer/account entities that are clean, typed, masked, and safe for non-PII users. |
| **M4** | Owns Silver cleansing for transaction activity plus referential-integrity and scale checks. | Transform transactions, authorization attempts, and device data; mark orphan foreign-key records; keep unsafe/unjoinable rows out of AI context; run the 2M-transaction stress test. | Silver activity tables that preserve transaction evidence, identify broken joins, and prove the pipeline can handle the required scale. |
| **M5** | Owns investigation-domain Silver tables, automated tests, evidence, runbook, and demo packaging. | Transform reference data, disputes, chargebacks, fraud alerts, cases, notes, bridges, and contact logs; redact free-text PII; build tests; prepare samples, runbook, and demo script. | Verifiable evidence that the full pipeline works, protects sensitive data, and can be reproduced by another engineer. |

> Swap `M1`–`M5` for your real team members. Each task has one accountable owner; pairing is still allowed.

---

## 1. Context

We have a working **mock data generator** (`mock/`, Python + Faker) that produces 25 Bronze source CSVs plus a `_defects_manifest.csv` (the ground-truth list of every intentionally-injected bad record) into `data/raw/`. **Everything downstream — Bronze ingestion, Silver cleansing, DQ engine, quarantine, PII masking, lineage, Silver AI-ready context, and tests — still needs to be built.**

This backlog is the dependency-ordered path to **all 11 grading criteria green**, sequenced so a thin end-to-end slice is demoable before fanning out to the full scope.

### In scope (full comprehensive build)
- 25 Bronze tables → governed Silver pipeline on Databricks; no separate Gold layer
- Silver AI-ready context output/table/view for the internal AI consumer
- All **36 injected DQ rules** executable (not just described)
- All **7 governance tables** populated each run
- Role-based **PII masking** (UC column masks + redaction)
- End-to-end **lineage** and **metadata**
- **Quarantine** of failed records with reasons
- Automated **tests** producing evidence + a reproducible **runbook**
- ~2M-transaction stress target

### Out of scope
- Real customer / confidential NAB data · personal AWS accounts · production AI integration · Docker (optional) · Figma / user research / SDVF

---

## 2. Architecture decisions

| Area | Decision | Rationale |
|---|---|---|
| **Transformation engine** | **SQL notebooks** orchestrated as one Databricks Job; DQ as a **rule-registry → failure-query** pattern | Many of the 36 rules are **cross-record** (FK anti-joins, `DQ-TXN-CARD-ACTIVE`, `DQ-AUTH-TS-ORDER`, near-duplicate windows, conditional `DQ-CASEPARTY-RESOLVE`, free-text PII regex). DLT `EXPECT … ON VIOLATION` evaluates **single-row predicates only** and silently cannot express these. A uniform "SELECT of failing rows" covers all 36 and emits the exact `failure_reason` / `disposition` / `raw_record` the quarantine contract needs. |
| **Bronze ingest** | **`COPY INTO`** (default); DLT streaming tables optional | Idempotent, simple, no serverless cost/quirks on Free Edition. |
| **PII enforcement** | **UC column masks** on Silver (+ Bronze defense-in-depth) + redaction inside transforms | Storage-level: even `SELECT *` by a non-privileged role cannot read raw PII. |
| **AI-safe surface** | **UC dynamic view `silver.investigation_context_ai`** — only `ai_allowed` columns, `legal_hold` rows excluded — for the `ai_consumer` role | The physical Zero-Trust surface (load-bearing for AC7/AC9) while keeping Silver as the terminal layer. |
| **DQ oracle** | `_defects_manifest.csv` → `gov._defects_manifest_staging` | Ground truth; the manifest-vs-quarantine reconciliation test is the master DQ evidence. |

> ⚠️ **Verify first (Epic 1):** confirm the Free Edition tenant supports UC **column masks + dynamic views**. This is load-bearing for the Zero-Trust surface; if unsupported, fall back to view-level masking and document it.

---

## 3. Grading rubric → epic coverage

All 11 acceptance criteria must pass:

| # | Acceptance criterion | Primary epic(s) |
|---|---|---|
| AC1 | Repo understandable by another engineer | E1, E8 |
| AC2 | Pipeline runs source → Silver AI-ready output end-to-end | E2, E4, E7, E8 |
| AC3 | Mock data has valid + invalid records | (mock — done) |
| AC4 | Data contracts match implementation | E1, E2, E5, E8 |
| AC5 | DQ checks are **executable**, not just described | E3, E8 |
| AC6 | Failed records quarantined with useful reasons | E3, E8 |
| AC7 | Sensitive fields masked/redacted/tokenised/removed before AI output | E4, E5, E7 |
| AC8 | Final Silver output has metadata + source traceability | E2, E6, E7 |
| AC9 | AI output exposes no unsafe/unsupported data | E7, E8 |
| AC10 | Tests/validation runnable + produce evidence | E8 (starts in E4) |
| AC11 | Runbook clear enough to reproduce the demo | E8 |

---

## 4. Readiness, sizing, and success

### Story point scale

| Story points | Brief description |
|---|---|
| 1 | Very simple task with minimal effort and no meaningful dependency. |
| 2 | Simple task with slightly more work than 1; low complexity and low uncertainty. |
| 3 | Moderate task with some complexity, small dependencies, or light validation work. |
| 5 | Medium-complexity task requiring multiple steps, dependencies, or some unknowns. |
| 8 | Complex task; do not assign. Break into smaller stories of 5 points or less. |
| 13 | Very complex task; do not assign. Break into smaller stories of 5 points or less. |

### Definition of Ready

- Backlog item has one accountable owner, clear acceptance criteria, and a known source-of-truth document or contract.
- Dependencies are named, including upstream tables, DQ rules, UC permissions, or pipeline stages.
- Verification is defined as an executable check, query, test, or documented demo step.
- Any uncertainty large enough to make the story exceed 5 points has been split or moved to a spike.

### Definition of Done

- **Backlog item:** SQL/notebook merged; runs clean on seed 42; any DQ rule it touches reconciles to the manifest; relevant test green.
- **Epic:** all stories done; the rubric AC(s) it owns are demonstrable via an executable command.
- **Release:** all 11 ACs green; `make mock → upload → ingest → build → test` reproduces identical quarantine counts from a clean state; final output is produced in Silver; 2M-row stress run recorded in the runbook; Confluence page links repo + outputs + runbook.

### Definition of Success

- The team can run the pipeline from mock data to final Silver AI-ready context and explain each control in the walkthrough.
- DQ, quarantine, masking, access, lineage, metadata, tests, and runbook evidence satisfy all 11 acceptance criteria.
- The final Silver AI view exposes only approved fields, excludes unsafe records, and contains enough source references for answer verification.

**Effort key:** S = small, M = medium, L = large. Story points estimate individual backlog items and are capped at 5.

---

## 5. The backlog — 8 epics (dependency-ordered)

> Sequencing principle: prove the **entire pattern on one table** in **Epic 4 (thin slice)** — touching all 11 rubric items minimally — then fan out to all 25 tables / 36 rules.

### Epic 1 — Platform & Unity Catalog scaffolding
**Goal:** reproducible, script-provisioned workspace + repo entrypoints. · **Effort M · ~2 days · Track A · Depends on: — · Covers AC1, AC11**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E1-S1 | Create the main Databricks catalog `tx_inv` and the three schemas `bronze`, `silver`, and `gov`. | Give the project a clear place to store raw ingested data, cleaned data, and governance evidence. | Catalog + 3 schemas exist; setup SQL is idempotent (re-run = no-op). | 2 | M1 | To Do |
| E1-S2 | Create landing folders in the Volume path `/Volumes/tx_inv/landing/<table>/` for all 25 source tables. | Give every generated CSV table a predictable upload location before ingestion starts. | One directory per table; generated by looping `config.TABLE_SCHEMAS`. | 3 | M1 | To Do |
| E1-S3 | Create Unity Catalog roles/groups for PII users, masked users, AI consumers, the pipeline runner, and auditors, then grant only the permissions each role needs. | Prevent accidental access to raw sensitive data and make the Zero Trust design enforceable. | Each role has exactly the grants `access_policies` requires. | 5 | M1 | To Do |
| E1-S4 | Create the `pipeline_runs` governance table and a `run_id` generator using the format `RUN-YYYYMMDD-<seq>`. | Let every pipeline execution be tracked from start to finish with counts and status. | One row opened at Job start, closed at end. | 3 | M1 | To Do |
| E1-S5 | Add project entrypoints: `Makefile` targets (`mock`, `upload`, `ingest`, `build`, `test`, `clean`), Databricks Job JSON, `pipelines/`, `tests/`, `docs/runbook.md` skeleton, and `.gitignore` rules. | Make the project easy for another engineer to run without guessing the commands or folder layout. | `make` targets exist and are documented; large generated data is git-ignored. | 5 | M5 | To Do |
| E1-S6 | Test whether this Databricks tenant supports Unity Catalog column masks and dynamic views. | Confirm the privacy design can be implemented as planned, or document the fallback before building on it. | Result (supported / fallback) recorded in runbook. | 2 | M1 | To Do |
| E1-S7 | Build an upload helper that copies `data/raw/*.csv` files into the matching landing Volume folders. | Move locally generated mock data into Databricks in a repeatable way. | Idempotent upload; re-run does not duplicate. | 3 | M1 | To Do |

### Epic 2 — Bronze layer: ingest 25 tables (raw + metadata)
**Goal:** all 25 source CSVs as Delta in `bronze.<table>`, all-STRING, append-only, with the 8 mandatory metadata columns; manifest ingested as the test oracle. · **Effort M · ~2–3 days · Track A · Depends on: E1 · Covers AC2, AC4, AC8, AC10**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E2-S1 | Generate Bronze table definitions for all 25 CSV sources, storing every business column as `STRING` plus the 8 required metadata columns. | Keep Bronze as a faithful raw landing layer while adding audit fields needed for tracing and reruns. | 25 tables; each has the 8 metadata columns. | 5 | M1 | To Do |
| E2-S2 | Load each CSV into its matching Bronze table using `COPY INTO`, including source file metadata, source record ID, row hash, and rescued-data capture. | Make ingestion repeatable and trace every Bronze row back to its original file and record. | Idempotent; re-ingest of same files adds 0 rows. | 5 | M1 | To Do |
| E2-S3 | Confirm the ingestion approach: use `COPY INTO` by default and document when DLT would be used instead. | Avoid later confusion about why the project chose a simpler batch ingestion pattern. | Decision + alternative documented in runbook. | 2 | M1 | To Do |
| E2-S4 | Add a Bronze row-count test for every source table. | Prove that all CSV rows landed in Bronze and no rows were silently lost. | `count(bronze.<table>)` == non-header CSV lines, all 25; `_rescued_data` null on clean tables. | 3 | M5 | To Do |
| E2-S5 | Load `_defects_manifest.csv` into `gov._defects_manifest_staging` as raw strings. | Bring the known list of injected bad records into Databricks so tests can compare expected and actual failures. | Table populated with raw STRING copy of the oracle. | 2 | M2 | To Do |

### Epic 3 — DQ rule registry + failure-engine + quarantine  🔴 critical-path bottleneck
**Goal:** the core engine — a `dq_rules` catalog (all 36 rules currently emitted by the generator/manifest) + a generic failure-extractor that writes one row per failing record to `quarantine_records`, aggregated into `dq_results`. · **Effort L · ~3–4 days · Track B · Depends on: E2 · Covers AC5, AC6, AC10. Start the moment E2 lands.**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E3-S1 | Create `gov.dq_rules` and seed all 36 data-quality rules with rule ID, layer, target table, severity, SQL expression, rule name, and category. | Store every data-quality rule in one governed table so rules are executable and reviewable. | Exactly the 36 rule IDs from `mock/generators.py` / `_defects_manifest.csv`; columns match the contract. | 3 | M2 | To Do |
| E3-S2 | Assign each rule a severity and category, including special flags for rules that block records from AI use. | Make downstream behavior clear: reject, warn, allow with warning, or exclude from AI context. | AI-exclusion rules (`DQ-CASE-LEGALHOLD`, `DQ-NOTE-LEGALHOLD`) flagged; warn/allowed-with-warning set where the brief rewards it. | 2 | M2 | To Do |
| E3-S3 | Build a failure-extractor notebook that accepts a `rule_id`, runs that rule, and writes each failed record to quarantine. | Turn the rule registry into real executable checks instead of only documentation. | Emits one `quarantine_records` row per (record × rule) with all contract columns including `raw_record=to_json(bronze row)`. | 5 | M2 | To Do |
| E3-S4 | Create and populate `gov.dq_results` with pass/fail totals for each rule in each run. | Give the demo and tests a quick summary of data-quality results without reading every quarantined row. | Per (`run_id × rule_id`): total / passed / failed / sample_failed_keys[]. | 3 | M2 | To Do |
| E3-S5 | Label each of the 36 rules as single-row, cross-record, or AI-exclusion (see §6). | Help engineers choose the correct SQL pattern for each rule and avoid using row-only checks for join/window rules. | Each rule tagged single-row / cross-record / ai-exclusion. | 3 | M2 | To Do |
| E3-S6 | Add disposition logic that maps rule severity to the action taken, and allows one record to fail multiple rules. | Preserve full evidence when the same bad record has more than one problem. | One record carrying multiple failures produces multiple quarantine rows. | 3 | M2 | To Do |

### Epic 4 — Thin vertical slice: customers raw → Silver AI context  🟢 MVP demo
**Goal:** prove the entire pattern on **one table** so all 11 rubric items are demoable before fanning out. Pick `customers` (has PII → masking; has 3 DQ rules → `DQ-CUST-EMAIL-FMT`/`ID-DUP`/`NEAR-DUP`; feeds final Silver context via `case_parties`). · **Effort M · ~2 days · Track C · Depends on: E2, E3 · Covers all 11 (each minimally)**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E4-S1 | Build the first Silver transform for `customers`: cast columns to proper types, tokenize customer names, mask email/phone, hash address and tax ID, and convert date of birth to age band. | Prove the raw-to-clean pattern on a table that contains sensitive customer information. | Silver columns + types match contract; no raw PII in non-privileged view. | 5 | M3 | To Do |
| E4-S2 | Add Unity Catalog column masks to the sensitive customer fields, with raw values visible only to `fraud_ops_pii_clear`. | Make sure privacy protection works at the storage/query layer, not only inside one transform. | Mask registered in `masking_policies`; behaves per role. | 3 | M3 | To Do |
| E4-S3 | Run the three customer data-quality rules through the E3 failure engine. | Prove the DQ engine can find known customer defects and quarantine them correctly. | `quarantine_records` matches manifest (EMAIL-FMT=11, ID-DUP=7, NEAR-DUP=7 on seed 42). | 3 | M3 | To Do |
| E4-S4 | Write customer lineage rows to `metadata_lineage`, mapping each important Bronze field to its Silver field. | Show how the final data can be traced back to the raw source. | Bronze field → silver field mapping present. | 2 | M3 | To Do |
| E4-S5 | Build a minimal `silver.investigation_context` table and `silver.investigation_context_ai` view for 1-2 sample cases. | Demonstrate the full pipeline pattern from raw customer data to AI-safe context before scaling to all tables. | AI-ready context grain present; a `legal_hold` case is excluded. | 5 | M1 | To Do |
| E4-S6 | Add the first automated manifest-vs-quarantine test for customer records only. | Prove the project can compare expected bad records with actual quarantined records. | Test passes; proves the test strategy. | 3 | M5 | To Do |

> **Demo gate:** this epic is the MVP walkthrough. Do not fan out until it demos clean.

### Epic 5 — Silver fan-out: all 25 tables + PII masking + column masks
**Goal:** replicate the E4 Silver pattern across the remaining 24 tables; full PII matrix; column masks everywhere PII exists. · **Effort L · ~4–5 days · Track C/D (parallel by table group) · Depends on: E4 · Covers AC4, AC6, AC7, AC9**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E5-S1a | Build Silver transforms for reference tables: `merchant_categories`, `channels`, `case_status_types`, `dispute_reason_codes`, `fraud_types`, `countries`, `currencies`, `branches`, `date_dim`, and `merchants`. | Clean and type the lookup data that other Silver tables use for labels, categories, and joins. | Types conformed; matches contracts. | 5 | M5 | To Do |
| E5-S1b | Build Silver transforms for `employees`, `accounts`, and `cards`, including hashed employee names, masked/hashed employee emails, and card PAN masking that keeps only the last 4 digits. | Protect staff and cardholder information while keeping useful investigation fields. | No full PAN persists past Silver. | 5 | M3 | To Do |
| E5-S1c | Build Silver transforms for `transactions`, `auth_attempts`, and `transaction_devices`, including typed amounts/timestamps and hashed or truncated device identifiers. | Clean the activity data that drives investigations while reducing device-related privacy risk. | Types conformed; device PII hashed. | 5 | M4 | To Do |
| E5-S1d | Build Silver transforms for disputes, chargebacks, fraud alerts, investigation cases, notes, case-to-transaction links, case parties, and customer contact logs. | Clean the investigation records that explain why a transaction is being reviewed. | Types conformed; matches contracts. | 5 | M5 | To Do |
| E5-S2 | Standardize allowed text values, such as lowercase `risk_rating` and `disputes.status`, and mapping `on_hold` to `suspended` while still recording the original defect. | Make Silver values consistent for users while keeping evidence that the raw record was invalid. | Conformance applied; original defect still flagged. | 3 | M5 | To Do |
| E5-S3 | Redact personal data from free-text fields in `investigation_notes.note_text` and `customer_contact_logs.note`, and store `_pii_flags` showing what was found. | Stop emails, phone numbers, and card numbers from leaking through notes written by humans. | No raw email/phone/PAN in Silver notes; still-leaking rows quarantined. | 5 | M5 | To Do |
| E5-S4 | Install column masks for every PII field listed in the data model and register each masked field in `masking_policies`. | Apply privacy controls consistently across all Silver tables, not only the customer slice. | Every §6-matrix field masked at storage layer. | 5 | M3 | To Do |
| E5-S5 | Handle near-duplicate customer and employee records by quarantining duplicates and keeping the earliest trusted record. | Prevent duplicate people from confusing investigations while preserving evidence of the duplicate rows. | Duplicate quarantined; earliest by `_ingest_ts`/PK kept. | 3 | M3 | To Do |
| E5-S6 | Mark orphan foreign-key rows in Silver, keep them for audit, and make sure the final AI context does not join them as if they were valid. | Preserve raw evidence without letting broken relationships create misleading AI context. | Orphan row present + marked; quarantine row carries `failure_reason`. | 5 | M4 | To Do |

### Epic 6 — Governance tables population
**Goal:** all 7 governance tables reliably populated each run and queryable for the DQ-evidence / lineage / privacy deliverables. · **Effort M · ~2 days · Track B · Depends on: E3, E5 · Covers AC6, AC8, AC9**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E6-S1 | Finalize and version the seeded `dq_rules` table so the 36 rules do not change unexpectedly between runs. | Keep the rule list stable for evidence, grading, and repeatable tests. | 36 rows; stable across runs. | 2 | M2 | To Do |
| E6-S2 | Populate `dq_results` every time the pipeline runs. | Provide a run-level summary of how many records passed or failed each rule. | Counts + sample keys per rule. | 3 | M2 | To Do |
| E6-S3 | Populate `quarantine_records` every time the pipeline runs, partitioned by `run_id`. | Keep detailed evidence of every failed record and make it easy to inspect a single run. | Appended; partitioned by `run_id`. | 3 | M2 | To Do |
| E6-S4 | Update `pipeline_runs` with one complete row for every Databricks Job run. | Show when the run started, when it ended, whether it succeeded, and how many rows moved through each stage. | start/end, status, per-stage counts (bronze_in, silver_out, quarantine, ai_context_out). | 3 | M1 | To Do |
| E6-S5 | Emit `metadata_lineage` rows from each Silver transform and the final AI-context build. | Let reviewers trace important final fields back to their original Bronze source columns. | source→target field mapping; covers all AI-context-bound + masked fields. | 5 | M3 | To Do |
| E6-S6 | Seed `masking_policies` with one row for each masked field. | Document which fields are sensitive and what masking rule protects them. | Matches §6 matrix. | 3 | M3 | To Do |
| E6-S7 | Seed `access_policies` with one row per field showing whether it is internal-only, customer-facing, or AI-allowed. | Drive the AI-safe view and grants from explicit policy metadata instead of hidden assumptions. | Drives Silver AI-view `ai_allowed` filter + UC grants. | 3 | M2 | To Do |

### Epic 7 — Silver AI-ready context + Zero-Trust enforcement
**Goal:** build `silver.investigation_context` (one doc per case, `legal_hold` excluded) from all Silver tables; final AI-ready contract; enforce Zero-Trust via the UC dynamic view. · **Effort L · ~3 days · Track A/D · Depends on: E5, E6 · Covers AC7, AC8, AC9**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E7-S1 | Build the Silver AI context aggregator, one record per investigation case, excluding legal-hold cases and legal-hold notes. | Combine the cleaned Silver tables into the single case-level context an internal AI consumer would read. | Arrays populated; legal_hold cases excluded; legal_hold notes excluded. | 5 | M1 | To Do |
| E7-S2 | Add final context metadata fields: `quality_status`, `masking_status`, `warning_flags`, `source_references`, `context_version`, `last_refreshed_at`, and `usage_restrictions`. | Help AI users and auditors understand data quality, masking, sources, freshness, and usage limits for each case. | Final Silver context columns present and correct; records below quality threshold are excluded or flagged by a documented rule. | 3 | M1 | To Do |
| E7-S3 | Create the dynamic view `silver.investigation_context_ai` for `ai_consumer`, exposing only AI-allowed columns and excluding legal-hold records. | Enforce the final Zero Trust boundary before any AI tool can read the data. | `ai_consumer` cannot select forbidden columns or legal_hold rows. | 3 | M1 | To Do |
| E7-S4 | Add an AI-safety assertion that fails the run if unsafe values appear in the AI view. | Catch privacy or legal-hold leaks even if an upstream transform or policy is misconfigured. | Run fails if any full PAN / raw email / raw phone / legal_hold case_id appears in the AI view. | 3 | M5 | To Do |
| E7-S5 | Write `docs/ai-prompts.md` with examples of prompts the AI can answer and prompts it must refuse. | Show reviewers how the AI-safe data surface should and should not be used. | Lists prompts AI can answer vs must refuse. | 2 | M5 | To Do |
| E7-S6 | Check the data-contract documentation for completeness across every source table. | Make sure another engineer can understand table purpose, fields, relationships, sensitive data, and DQ rules. | `docs/data-model.md` or generated contract docs cover purpose, keys, fields, required/optional status, accepted values, examples, DQ rules, sensitive classification, and relationships for every source table. | 3 | M5 | To Do |

### Epic 8 — Tests, stress, runbook, demo polish
**Goal:** every rubric item provable by an executable command; prove the 2M stress target; deliver runbook + demo. · **Effort M · ~3 days · Track D · Depends on: E7 (tests start in E4) · Covers AC2, AC4, AC6, AC7, AC9, AC10, AC11**

| ID | Task | Objective | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|------|-----------|-------------------------|--------------|----------|--------|
| E8-S1 | Build the master manifest-vs-quarantine reconciliation test for all rules. | Prove that every intentionally bad record is caught and no unexpected records are incorrectly quarantined. | Per rule: precision = recall = 1.0 (modulo intentional ai_exclusion). | 5 | M5 | To Do |
| E8-S2 | Build schema-contract tests for Bronze, Silver, and final Silver context. | Prove that implemented tables match the documented contracts. | Silver types match contracts; Bronze has 8 metadata cols; final Silver context has the required context columns. | 3 | M5 | To Do |
| E8-S3 | Build masking-effectiveness tests for unprivileged users and the AI view. | Prove that sensitive values cannot be read by the wrong role and that tokenization is consistent. | No raw PII in unprivileged/Silver AI-view; tokenize deterministic; mask behaves per role. | 5 | M5 | To Do |
| E8-S4 | Build AI-safety and legal-hold tests. | Prove the AI surface excludes legally restricted records and raw card data. | Zero legal_hold case_id in AI-view; legal_hold notes absent; raw PAN absent past Bronze. | 3 | M5 | To Do |
| E8-S5 | Build a lineage completeness test for the final Silver AI context. | Prove every AI-facing field can be traced back to source data for audit and explanation. | Every final Silver AI-context field traces to a Bronze field. | 3 | M5 | To Do |
| E8-S6 | Build reconciliation/count tests across Bronze, Silver, quarantine, and AI context. | Prove row movement through the pipeline is explainable and no stage silently drops records. | bronze = CSV lines; conformed Silver = bronze - quarantined - rejected (+warned); Silver AI context = non-legal_hold cases above/within quality threshold. | 5 | M5 | To Do |
| E8-S7 | Run the stress dataset with about 2M transactions and record performance details. | Prove the solution works beyond the small demo dataset and document its runtime limits. | `--stress` → full pipeline completes; correctness holds; wall-clock + cluster size recorded. | 5 | M4 | To Do |
| E8-S8 | Complete `docs/runbook.md` with prerequisites, setup, commands, output locations, validation steps, quarantine inspection, limitations, and troubleshooting. | Let another team member reproduce the whole demo without needing private knowledge. | Covers prerequisites, setup, run commands, validation commands, output locations, quarantine inspection, known limitations, troubleshooting; another team can reproduce the demo cold. | 3 | M5 | To Do |
| E8-S9 | Prepare the demo script, committed masked sample outputs in `data/sample/`, and a DQ-evidence summary. | Package the final proof for grading and Confluence submission. | Deliverables attached/linked from Confluence. | 3 | M5 | To Do |

---

## 6. The 36 DQ rules — by failure-query shape (drives Epic 3)

| Shape | Rules | Implementation |
|---|---|---|
| **(a) Single-row predicate** | `DQ-TXN-AMT-POS`, `DQ-TXN-MERCH-REQ`, `DQ-TXN-TS-FUTURE`, `DQ-ACC-OPENDATE-FUTURE`, `DQ-CUST-EMAIL-FMT`, `DQ-CARD-EXPIRED-ACTIVE`, `DQ-MERCH-RISK-CASING`, `DQ-DISP-STATUS-ENUM`, `DQ-DISP-REASON-REQ`, `DQ-CASE-STATUS-ENUM`, `DQ-ALT-SCORE-RANGE`, `DQ-DEV-TYPE-REQ`, `DQ-NOTE-PII-LEAK`, `DQ-CTL-NOTE-PII`, uniqueness `DQ-CUST-ID-DUP`/`DQ-TXN-ID-DUP`/`DQ-CARD-DUP`/`DQ-EMP-EMAIL-UNIQ` | `WHERE NOT <predicate>` (also expressible as DLT `EXPECT` if DLT Bronze chosen) |
| **(b) Cross-record / join / window** | FK anti-joins `DQ-ACC-CUST-FK`, `DQ-TXN-ACCT-FK`, `DQ-AUTH-TXN-FK`, `DQ-DISP-TXN-FK`, `DQ-CBK-DISP-FK`, `DQ-DEV-TXN-FK`, `DQ-CASETXN-TXN-FK`; `DQ-TXN-CARD-ACTIVE`, `DQ-AUTH-TS-ORDER`, `DQ-CASE-STALE`, `DQ-CUST-NEAR-DUP`, `DQ-EMP-NAME-NEAR-DUP`, `DQ-CASEPARTY-RESOLVE`, `DQ-CASEPARTY-TYPE-ENUM`, `DQ-CTL-DNC-VIOLATION` | failure query with JOIN / anti-join / window — **NOT** EXPECT |
| **(c) AI-exclusion** | `DQ-CASE-LEGALHOLD`, `DQ-NOTE-LEGALHOLD` | writes an evidence row; **primary effect** = Silver AI-context / AI-view exclusion |

> Manifest expected counts (default seed 42, top): `DQ-TXN-AMT-POS`=150, `DQ-TXN-TS-FUTURE`=120, `DQ-TXN-MERCH-REQ`=120, `DQ-TXN-ID-DUP`=120, `DQ-TXN-ACCT-FK`=120; customers `EMAIL-FMT`=11, `ID-DUP`=7, `NEAR-DUP`=7. **Derive expected sets from the manifest table at runtime — never hardcode counts** (they drift if the seed changes).

---

## 7. Governance tables — populated by which stage

| Table | Populated by | When |
|---|---|---|
| `dq_rules` | E3 seed | once (immutable) |
| `dq_results` | E3 failure-extractor | each run |
| `quarantine_records` | E3 failure-extractor | each run (append, partitioned by `run_id`) |
| `pipeline_runs` | orchestrator | open at Job start, close at end |
| `metadata_lineage` | each Silver transform and final Silver AI-context build | each run (append) |
| `masking_policies` | E5 (seed from §6 matrix) | once + on change |
| `access_policies` | E6 (drives Silver AI-view `ai_allowed` + UC grants) | once + on change |

---

## 8. Test strategy (test → rubric)

| Executable test | ACs |
|---|---|
| Bronze row-count == CSV line count (all 25) | AC2, AC4, AC10 |
| Schema-contract match (Bronze metadata, Silver types, final Silver context columns) | AC4, AC10 |
| Manifest-vs-quarantine reconciliation (precision/recall per rule) | AC5, AC6, AC10 |
| `dq_results` non-empty, counts consistent with quarantine | AC5, AC10 |
| Masking-effectiveness (no raw PII in unprivileged/Silver AI-view; tokenize determinism; mask-per-role) | AC7, AC9 |
| `legal_hold` exclusion (no legal_hold case_id in AI-view; legal_hold notes absent) | AC9 |
| Raw PAN absent from Silver AI output | AC7, AC9 |
| Lineage completeness (every final Silver AI-context field traces to Bronze) | AC8 |
| Final Silver context metadata fields present | AC8 |
| Reconciliation silver = bronze − quarantined − rejected (+warned) | AC6, AC10 |
| Stress correctness + wall-clock evidence | AC2, AC10 |
| Clean-state rerun reproduces identical quarantine | AC11 |

---

## 9. Risks & dependencies

1. **UC column-mask / dynamic-view support** in the tenant — load-bearing for AC7/AC9. **Verify first (E1-S6)**; fall back to view-level masking + document if unsupported.
2. **Compute quotas / concurrency** for 5 engineers — parallel dev runs collide. Mitigate: serverless SQL warehouses for governance queries; one shared `pipeline_runner` principal; per-engineer dev catalog/schema clone (`tx_inv_<user>`).
3. **2M-row cross-record DQ** (self-join near-dup, FK anti-joins) on a small cluster. Mitigate: Z-ORDER/cluster on `transaction_id`/`account_id`/`card_id`; broadcast small dims; failure-extractor is per-rule so each is independently optimizable.
4. **Column-mask bugs** silently expose or over-mask. Mitigate: E8-S3 masking-effectiveness tests are a mandatory gate; review mask SQL.
5. **EXPECT misuse** — never rely on EXPECT for cross-record rules (silently passes). Failure-query registry is the source of truth.
6. **Oracle drift** — regenerating with a different seed changes expected counts. Pin seed 42 in the runbook; tests read the manifest table at runtime.
7. **Pinned date 2026-07-06** — staleness/future defects are measured against it; re-baseline tests if generation changes.
8. **E3 is the critical-path bottleneck** — start it the instant E2 lands; don't wait for the slice.
9. **Bronze-as-truth vs Silver-cleans** — conformed Silver keeps RI-orphan rows (marked) for completeness; the final Silver AI context excludes unsafe/unjoinable rows.

---

## 10. Sequencing & parallelization (5 people)

**Critical path:** E1 → E2 → E3 → E4 → E7 → E8.

| Track | Owns | Members | Sequence |
|---|---|---|---|
| **A — Foundation / final Silver context** | E1, E2, E7 | M1 | E1 → E2 → E7 Silver AI context aggregator |
| **B — DQ / governance** | E3, E6 | M2 | E3 (starts when E2 lands) → E6 |
| **C — Silver core entities** | E4, E5 | M3, M4 | E4 slice → E5 split by table group (M3 customers/employees/accounts/cards · M4 transactions/devices/auth + RI/stress) |
| **D — Investigation Silver + verification** | E5, E8 | M5 | E5 dims/disputes/cases/notes/bridges; tests drafted alongside E4, finalised after E7 |

Effort totals: E1 **M**, E2 **M**, E3 **L**, E4 **M**, E5 **L**, E6 **M**, E7 **M/L**, E8 **M**. Max ~4 parallel streams after E2 for a 5-person team.

---

## 11. Demo / acceptance checklist (release gate)

From a clean state:
- [ ] `make mock` (seed 42) → `make upload` → `make ingest` → `make build` → `make test` — all green
- [ ] `make test` produces a DQ-evidence report; manifest-vs-quarantine shows precision = recall = 1.0 per rule
- [ ] `SELECT * FROM silver.investigation_context_ai` as `ai_consumer` → only `ai_allowed` columns, zero `legal_hold` cases; AI-safety assertion passes
- [ ] All 7 governance tables populated; `metadata_lineage` traces every final Silver AI-context field to Bronze; `pipeline_runs` shows one clean run with counts
- [ ] `python -m mock.generate --stress` → full pipeline on ~2M txns completes; correctness holds; wall-clock recorded
- [ ] A second engineer follows `docs/runbook.md` cold and reproduces identical quarantine counts
- [ ] Confluence page links repo + outputs + runbook

---

## 12. Source-of-truth files (spec the build is generated from)

- `docs/data-model.md` — contracts, enums, PII matrix §6, quarantine schema §7, final AI context grain §8
- `docs/bronze-layer.md` — Bronze DDL + metadata columns + COPY INTO / DLT templates §5
- `mock/config.py` — `TABLE_SCHEMAS`, enums, `GENERATION_ORDER`, `RUN_DATE`, `ORPHAN_CUSTOMER_ID`
- `mock/generators.py` — the 36 `man.add(...)` rule IDs (authoritative rule list + `rule_name` / `failure_reason` / `severity`)
- `data/raw/_defects_manifest.csv` — validation oracle (expected quarantine set per `rule_id`)
