# Product Backlog — NAB Transaction-Investigation Pipeline

| | |
|---|---|
| **Project** | NAB Core Data Engineer assignment — "TAC@NABVNSC22" |
| **Scenario** | Transaction investigation (transactions, disputes/chargebacks, merchants, fraud alerts, investigation notes) |
| **Goal** | Turn mock banking data into trusted, governed, **Silver-layer AI-ready context**, enforcing **Zero Trust AI at the data layer** before any AI consumer may read it |
| **Platform** | Databricks Free Edition + Unity Catalog (catalog `tx_inv`; schemas `bronze` / `silver` / `gov`; landing Volume `/Volumes/tx_inv/landing/<table>/`) |
| **Status** | Mock data layer ✅ complete · Pipeline ⬜ to build |
| **Last updated** | 2026-07-06 |

### Team & status key
**Rule: each task is assigned to exactly one member at a time** (pairing is allowed, but every row has one accountable owner).

**Status values:** `To Do` · `In Progress` · `Blocked` · `Review` · `Done`  *(all tasks start at `To Do`)*

**Members (placeholders — replace with real names):**

| ID | Focus | Track |
|---|---|---|
| **M1** | Platform, Bronze, orchestration, final Silver AI context | A |
| **M2** | DQ engine + governance, access policy | B |
| **M3** | Silver: customers/employees/accounts/cards · PII masking framework | C |
| **M4** | Silver: transactions/devices/auth · RI handling | C |
| **M5** | Silver: reference dims/disputes/cases/notes/bridges · tests/evidence/runbook | D |

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

- Story has one accountable owner, clear acceptance criteria, and a known source-of-truth document or contract.
- Dependencies are named, including upstream tables, DQ rules, UC permissions, or pipeline stages.
- Verification is defined as an executable check, query, test, or documented demo step.
- Any uncertainty large enough to make the story exceed 5 points has been split or moved to a spike.

### Definition of Done

- **Story:** SQL/notebook merged; runs clean on seed 42; any DQ rule it touches reconciles to the manifest; relevant test green.
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

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E1-S1 | Provision `tx_inv` catalog + `bronze/silver/gov` schemas | Catalog + 3 schemas exist; setup SQL is idempotent (re-run = no-op) | 2 | M1 | To Do |
| E1-S2 | Create landing Volume `/Volumes/tx_inv/landing/<table>/` for all 25 tables | One dir per table; generated by looping `config.TABLE_SCHEMAS` | 3 | M1 | To Do |
| E1-S3 | Define UC roles/groups (`fraud_ops_pii_clear`, `fraud_ops_masked`, `ai_consumer`, `pipeline_runner`, `auditor`) with least-privilege grants | Each role has exactly the grants `access_policies` requires | 5 | M1 | To Do |
| E1-S4 | `pipeline_runs` DDL + `run_id` generator (`RUN-YYYYMMDD-<seq>`) | One row opened at Job start, closed at end | 3 | M1 | To Do |
| E1-S5 | Repo scaffolding: `Makefile` (`mock/upload/ingest/build/test/clean`), Databricks Job JSON, `pipelines/`, `tests/`, `docs/runbook.md` skeleton, `.gitignore` | `make` targets exist and are documented; large data is git-ignored | 5 | M5 | To Do |
| E1-S6 | **Verify UC column-mask + dynamic-view support** in the tenant | Result (supported / fallback) recorded in runbook | 2 | M1 | To Do |
| E1-S7 | Upload helper: copy `data/raw/*.csv` → landing Volume | Idempotent upload; re-run does not duplicate | 3 | M1 | To Do |

### Epic 2 — Bronze layer: ingest 25 tables (raw + metadata)
**Goal:** all 25 source CSVs as Delta in `bronze.<table>`, all-STRING, append-only, with the 8 mandatory metadata columns; manifest ingested as the test oracle. · **Effort M · ~2–3 days · Track A · Depends on: E1 · Covers AC2, AC4, AC8, AC10**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E2-S1 | Templated Bronze DDL per table (all `STRING` + 8 metadata cols) from `config.TABLE_SCHEMAS` | 25 tables; each has the 8 metadata columns | 5 | M1 | To Do |
| E2-S2 | `COPY INTO` ingest per table (metadata from `_metadata`; `_source_record_id`=PK; `_record_hash=sha2(concat_ws(delimiter,...))`; `_rescued_data`) | Idempotent; re-ingest of same files adds 0 rows | 5 | M1 | To Do |
| E2-S3 | Decide COPY-into-vs-DLT (default COPY INTO) | Decision + alternative documented in runbook | 2 | M1 | To Do |
| E2-S4 | Bronze row-count test | `count(bronze.<table>)` == non-header CSV lines, all 25; `_rescued_data` null on clean tables | 3 | M5 | To Do |
| E2-S5 | Ingest manifest → `gov._defects_manifest_staging` | Table populated with raw STRING copy of the oracle | 2 | M2 | To Do |

### Epic 3 — DQ rule registry + failure-engine + quarantine  🔴 critical-path bottleneck
**Goal:** the core engine — a `dq_rules` catalog (all 36 rules currently emitted by the generator/manifest) + a generic failure-extractor that writes one row per failing record to `quarantine_records`, aggregated into `dq_results`. · **Effort L · ~3–4 days · Track B · Depends on: E2 · Covers AC5, AC6, AC10. Start the moment E2 lands.**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E3-S1 | `gov.dq_rules` DDL + seed (36 rows): `rule_id, layer, target_table, severity, expression, rule_name, category` | Exactly the 36 rule IDs from `mock/generators.py` / `_defects_manifest.csv`; columns match the contract | 3 | M2 | To Do |
| E3-S2 | Per-rule severity + category | AI-exclusion rules (`DQ-CASE-LEGALHOLD`, `DQ-NOTE-LEGALHOLD`) flagged; warn/allowed-with-warning set where the brief rewards it | 2 | M2 | To Do |
| E3-S3 | Failure-extractor notebook (parametrised by `rule_id`) | Emits one `quarantine_records` row per (record × rule) with all contract columns incl. `raw_record=to_json(bronze row)` | 5 | M2 | To Do |
| E3-S4 | `gov.dq_results` DDL + populate | Per (`run_id × rule_id`): total / passed / failed / sample_failed_keys[] | 3 | M2 | To Do |
| E3-S5 | Classify the 36 rules into the 3 failure-query shapes (see §6) | Each rule tagged single-row / cross-record / ai-exclusion | 3 | M2 | To Do |
| E3-S6 | Disposition logic (severity → disposition; n × rule per record) | One record carrying multiple failures produces multiple quarantine rows | 3 | M2 | To Do |

### Epic 4 — Thin vertical slice: customers raw → Silver AI context  🟢 MVP demo
**Goal:** prove the entire pattern on **one table** so all 11 rubric items are demoable before fanning out. Pick `customers` (has PII → masking; has 3 DQ rules → `DQ-CUST-EMAIL-FMT`/`ID-DUP`/`NEAR-DUP`; feeds final Silver context via `case_parties`). · **Effort M · ~2 days · Track C · Depends on: E2, E3 · Covers all 11 (each minimally)**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E4-S1 | Silver transform for customers (typed; tokenize name; mask email/phone; hash address; dob→age_band; hash tax_id) | Silver columns + types match contract; no raw PII in non-privileged view | 5 | M3 | To Do |
| E4-S2 | UC column mask on masked customers fields (raw only to `fraud_ops_pii_clear`) | Mask registered in `masking_policies`; behaves per role | 3 | M3 | To Do |
| E4-S3 | Run the 3 customers DQ rules via E3 engine | `quarantine_records` matches manifest (EMAIL-FMT=11, ID-DUP=7, NEAR-DUP=7 on seed 42) | 3 | M3 | To Do |
| E4-S4 | Emit `metadata_lineage` rows for customers | Bronze field → silver field mapping present | 2 | M3 | To Do |
| E4-S5 | Minimal `silver.investigation_context` and `silver.investigation_context_ai` for 1–2 sample cases | AI-ready context grain present; a `legal_hold` case is excluded | 5 | M1 | To Do |
| E4-S6 | First automated test: manifest-vs-quarantine reconciliation (customers only) | Test passes; proves the test strategy | 3 | M5 | To Do |

> **Demo gate:** this epic is the MVP walkthrough. Do not fan out until it demos clean.

### Epic 5 — Silver fan-out: all 25 tables + PII masking + column masks
**Goal:** replicate the E4 Silver pattern across the remaining 24 tables; full PII matrix; column masks everywhere PII exists. · **Effort L · ~4–5 days · Track C/D (parallel by table group) · Depends on: E4 · Covers AC4, AC6, AC7, AC9**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E5-S1a | Silver transforms — reference dims (`merchant_categories`, `channels`, `case_status_types`, `dispute_reason_codes`, `fraud_types`, `countries`, `currencies`, `branches`, `date_dim`, `merchants`) | Types conformed; matches contracts | 5 | M5 | To Do |
| E5-S1b | Silver transforms — `employees` (hash name, mask/hash email), `accounts`, `cards` (keep masked PAN last4) | No full PAN persists past Silver | 5 | M3 | To Do |
| E5-S1c | Silver transforms — `transactions`, `auth_attempts`, `transaction_devices` (truncate/hash ip, hash device_id/type) | Types conformed; device PII hashed | 5 | M4 | To Do |
| E5-S1d | Silver transforms — `disputes`, `chargebacks`, `fraud_alerts`, `investigation_cases`, `investigation_notes`, `case_transactions`, `case_parties`, `customer_contact_logs` | Types conformed; matches contracts | 5 | M5 | To Do |
| E5-S2 | Enum conformance + DQ-flag (lowercase `risk_rating`/`disputes.status`; map `on_hold`→`suspended`) | Conformance applied; original defect still flagged | 3 | M5 | To Do |
| E5-S3 | Free-text PII redaction (`investigation_notes.note_text`, `customer_contact_logs.note`): regex sweep, redact in Silver, keep `_pii_flags` | No raw email/phone/PAN in Silver notes; still-leaking rows quarantined | 5 | M5 | To Do |
| E5-S4 | Column-mask install across all PII fields; register each in `masking_policies` | Every §6-matrix field masked at storage layer | 5 | M3 | To Do |
| E5-S5 | Near-dup handling (`DQ-CUST-NEAR-DUP`/`DQ-EMP-NAME-NEAR-DUP`) | Duplicate quarantined; earliest by `_ingest_ts`/PK kept | 3 | M3 | To Do |
| E5-S6 | RI handling: orphan-FK rows kept (marked) in Silver; final AI context won't join them | Orphan row present + marked; quarantine row carries `failure_reason` | 5 | M4 | To Do |

### Epic 6 — Governance tables population
**Goal:** all 7 governance tables reliably populated each run and queryable for the DQ-evidence / lineage / privacy deliverables. · **Effort M · ~2 days · Track B · Depends on: E3, E5 · Covers AC6, AC8, AC9**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E6-S1 | `dq_rules` seeded once (immutable/versioned) | 36 rows; stable across runs | 2 | M2 | To Do |
| E6-S2 | `dq_results` populated each run | Counts + sample keys per rule | 3 | M2 | To Do |
| E6-S3 | `quarantine_records` populated each run | Appended; partitioned by `run_id` | 3 | M2 | To Do |
| E6-S4 | `pipeline_runs` one row per Job run | start/end, status, per-stage counts (bronze_in, silver_out, quarantine, ai_context_out) | 3 | M1 | To Do |
| E6-S5 | `metadata_lineage` emitted by each transform | source→target field mapping; covers all AI-context-bound + masked fields | 5 | M3 | To Do |
| E6-S6 | `masking_policies` seeded (one row per masked field) | Matches §6 matrix | 3 | M3 | To Do |
| E6-S7 | `access_policies` seeded (one row per field → `internal_only/customer_facing/ai_allowed`) | Drives Silver AI-view `ai_allowed` filter + UC grants | 3 | M2 | To Do |

### Epic 7 — Silver AI-ready context + Zero-Trust enforcement
**Goal:** build `silver.investigation_context` (one doc per case, `legal_hold` excluded) from all Silver tables; final AI-ready contract; enforce Zero-Trust via the UC dynamic view. · **Effort L · ~3 days · Track A/D · Depends on: E5, E6 · Covers AC7, AC8, AC9**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E7-S1 | Silver AI context aggregator (per case `WHERE legal_hold=false`): linked_transactions[], merchant_context[], dispute_context[], fraud_alerts[], redacted_notes[] | Arrays populated; legal_hold cases excluded; legal_hold notes excluded | 5 | M1 | To Do |
| E7-S2 | Compute quality_status, masking_status, warning_flags[], source_references[], context_version, last_refreshed_at, usage_restrictions | Final Silver context columns present and correct; records below quality threshold are excluded or flagged by a documented rule | 3 | M1 | To Do |
| E7-S3 | UC dynamic view `silver.investigation_context_ai` (ai_allowed cols only, legal_hold excluded) for `ai_consumer` | `ai_consumer` cannot select forbidden columns or legal_hold rows | 3 | M1 | To Do |
| E7-S4 | AI-safety assertion (defense in depth) | Run fails if any full PAN / raw email / raw phone / legal_hold case_id appears in the AI view | 3 | M5 | To Do |
| E7-S5 | Example-prompts doc (`docs/ai-prompts.md`) | Lists prompts AI can answer vs must refuse | 2 | M5 | To Do |
| E7-S6 | Data-contract deliverable check | `docs/data-model.md` or generated contract docs cover purpose, keys, fields, required/optional status, accepted values, examples, DQ rules, sensitive classification, and relationships for every source table | 3 | M5 | To Do |

### Epic 8 — Tests, stress, runbook, demo polish
**Goal:** every rubric item provable by an executable command; prove the 2M stress target; deliver runbook + demo. · **Effort M · ~3 days · Track D · Depends on: E7 (tests start in E4) · Covers AC2, AC4, AC6, AC7, AC9, AC10, AC11**

| ID | Story | Acceptance (verifiable) | Story Points | Assignee | Status |
|----|-------|-------------------------|--------------|----------|--------|
| E8-S1 | **Manifest-vs-quarantine reconciliation** (master evidence) | Per rule: precision = recall = 1.0 (modulo intentional ai_exclusion) | 5 | M5 | To Do |
| E8-S2 | Schema-contract tests | Silver types match contracts; Bronze has 8 metadata cols; final Silver context has the required context columns | 3 | M5 | To Do |
| E8-S3 | Masking-effectiveness tests | No raw PII in unprivileged/Silver AI-view; tokenize deterministic; mask behaves per role | 5 | M5 | To Do |
| E8-S4 | AI-safety / legal_hold tests | Zero legal_hold case_id in AI-view; legal_hold notes absent; raw PAN absent past Bronze | 3 | M5 | To Do |
| E8-S5 | Lineage test | Every final Silver AI-context field traces to a Bronze field | 3 | M5 | To Do |
| E8-S6 | Reconciliation/count test | bronze = CSV lines; conformed Silver = bronze − quarantined − rejected (+warned); Silver AI context = non-legal_hold cases above/within quality threshold | 5 | M5 | To Do |
| E8-S7 | Stress run (~2M txns) | `--stress` → full pipeline completes; correctness holds; wall-clock + cluster size recorded | 5 | M4 | To Do |
| E8-S8 | Runbook (`docs/runbook.md`) | Covers prerequisites, setup, run commands, validation commands, output locations, quarantine inspection, known limitations, troubleshooting; another team can reproduce the demo cold | 3 | M5 | To Do |
| E8-S9 | Demo script + committed masked samples (`data/sample/`) + DQ-evidence summary | Deliverables attached/linked from Confluence | 3 | M5 | To Do |

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
