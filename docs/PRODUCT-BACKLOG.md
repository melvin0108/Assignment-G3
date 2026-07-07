# Product Backlog - NAB Transaction-Investigation Pipeline

| | |
|---|---|
| **Project** | NAB Core Data Engineer assignment - "TAC@NABVNSC22" |
| **Scenario** | Transaction investigation using transactions, disputes, chargebacks, merchants, fraud alerts, cases, notes, and related reference data |
| **Goal** | Turn generated banking data into trusted, governed, Silver-layer context that is safe for an internal AI consumer to read |
| **Platform assumption** | Databricks Free Edition + Unity Catalog, using catalog `tx_inv` and schemas such as `bronze`, `silver`, and `gov` if supported by the tenant |
| **Status** | Mock data generation complete; backlog created; Databricks workspace linked with GitHub; catalog and layer schemas created; source data uploaded to Databricks Volume; Bronze ingestion under review; Silver, governance, tests, and demo evidence still to build |
| **Last updated** | 2026-07-07 |

### Team & Status Key

**Rule:** each backlog item has one accountable owner at a time. Pairing is allowed, but the table should show who is responsible for moving the item forward.

**Status values:** `To Do` - `In Progress` - `Blocked` - `Review` - `Done`

**Members (placeholders - replace with real names):**

| ID | Main area of ownership | What this person is accountable for |
|---|---|---|
| **M1** | Platform flow and final AI context | Keeps the runnable pipeline path coherent: environment choices, ingestion flow, orchestration, and final AI-facing data surface. |
| **M2** | Data quality and governance rules | Owns the rules, quarantine evidence, DQ summaries, and access-policy metadata that explain what data can be trusted or used. |
| **M3** | People/account Silver data and privacy | Owns cleaned Silver outputs for customer, employee, account, and card data, including masking or tokenization of sensitive fields. |
| **M4** | Transaction activity and scale | Owns cleaned transaction, authorization, and device activity, including broken relationship handling and stress-run evidence. |
| **M5** | Investigation data, validation, and demo evidence | Owns investigation-domain Silver data, tests, runbook, sample outputs, and final demo packaging. |

---

## 1. Context

The repository already has a mock data generator (`mock/`) that creates source CSV files and a `_defects_manifest.csv`. The manifest is the known list of intentionally injected data problems and should be used as evidence when validating data-quality work.

Current completed work reported by the team:

- Mock data generation is complete.
- Mock data generation script exists.
- Backlog list is created.
- Databricks workspace is set up and linked with the GitHub repository.
- Catalog and layer schemas are created.
- Generated source data has been uploaded to Databricks Volume.
- A small upload demo to Databricks Volume has been tested.
- Data ingestion into the Bronze layer has been written and is under review.

The backlog below is not a strict step-by-step waterfall plan. It is a work-and-adapt list. Each epic states a premise and the outcomes it should produce. The team can pull items in the order that creates the fastest learning, as long as the final release still proves the required controls: ingestion, Silver data, data quality, quarantine, masking, access control, lineage, AI-safe output, tests, stress evidence, and runbook.

### In Scope

- Ingest all generated source tables into a raw/Bronze area.
- Build governed Silver outputs for investigation use.
- Provide an AI-ready Silver context surface for internal AI use.
- Execute all injected data-quality rules and record failed records with reasons.
- Protect sensitive data through masking, redaction, tokenization, removal, or controlled access.
- Produce governance evidence: run tracking, DQ results, quarantine, lineage, masking policy, and access policy.
- Provide automated validation and a reproducible demo runbook.
- Record evidence that the pipeline can handle the required larger transaction volume.

### Out of Scope

- Real customer data.
- Production AI integration.
- Production infrastructure hardening beyond what is needed for the assignment.
- UI design, Figma, user research, or SDVF.

---

## 2. Working Assumptions

These are current implementation assumptions. They can change if the team learns that a simpler or more reliable approach is better.

| Area | Current assumption | Adaptation rule |
|---|---|---|
| Data platform | Use Databricks with Unity Catalog if available in the tenant. | If a feature is not supported, document the fallback and keep the same security outcome. |
| Raw ingestion | Use a simple repeatable batch load into Bronze tables. | If DLT is easier in the tenant, record the reason and keep the same metadata and idempotency evidence. |
| Data quality | Store rules in a registry and run executable failure queries. | Rule implementation can vary by rule shape, but every rule must produce auditable results. |
| Sensitive data | Mask, redact, tokenize, hash, or remove sensitive fields before AI use. | The exact method can differ by field, but unsafe raw values must not appear in the AI surface. |
| AI surface | Provide a Silver AI-ready table or view with only allowed fields. | The physical implementation may be a table or view if it proves the same access and exclusion controls. |
| Validation | Use automated checks where possible and keep manual demo checks only where automation is not practical. | Add tests as each capability becomes stable instead of waiting until the end. |

---

## 3. Grading Rubric Coverage

| # | Acceptance criterion | Where it is mainly proven |
|---|---|---|
| AC1 | Repo understandable by another engineer | E1, E8 |
| AC2 | Pipeline runs source to Silver AI-ready output | E2, E4, E6, E7 |
| AC3 | Mock data has valid and invalid records | Existing mock generator |
| AC4 | Data contracts match implementation | E2, E4, E7 |
| AC5 | DQ checks are executable, not only described | E3, E7 |
| AC6 | Failed records are quarantined with useful reasons | E3, E5, E7 |
| AC7 | Sensitive fields are protected before AI output | E4, E5, E6, E7 |
| AC8 | Final Silver output has metadata and traceability | E2, E5, E6, E7 |
| AC9 | AI output exposes no unsafe or unsupported data | E5, E6, E7 |
| AC10 | Tests and validation are runnable and produce evidence | E7 |
| AC11 | Runbook is clear enough to reproduce the demo | E1, E8 |

---

## 4. How to Use This Backlog

### Backlog Principles

- Start with the smallest useful outcome, not the largest setup.
- Treat each epic as a capability area, not a fixed phase.
- Pull work when its premise is clear and its completion evidence is testable.
- Move or split items if new learning shows they belong in a different epic.
- Keep platform setup lightweight until it is needed by a deliverable.
- Add validation near the work being built instead of saving all tests for the end.

### Definition of Ready

- The item has one accountable owner.
- The objective explains why the item matters.
- The deliverable is clear enough to review.
- The completion evidence can be checked by a query, command, test, screenshot, or short written note.
- Unknowns are small enough to handle inside the item; otherwise the item should become a short discovery task.

### Definition of Done

- The deliverable exists in the repo, Databricks workspace, or documented evidence location.
- The completion evidence has been checked.
- Any changed contract, rule, or assumption is reflected in the relevant documentation.
- Sensitive-data and AI-safety effects have been considered if the item touches user, account, card, note, or case data.

---

## 5. Adaptive Product Backlog

### Epic 1 - Shared Working Model and Minimum Runnable Skeleton

**Premise:** The team needs a lightweight way to work together and run the project, but should avoid heavy platform setup before the purpose of each setup item is clear.

**Outcome:** Another engineer can understand the repo, run basic commands, and see how work is organized.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E1-I1 | Define the minimum local and Databricks setup needed for the first working demo. | Avoid setting up tools or permissions that do not yet serve a visible outcome. | Databricks workspace exists, GitHub repo is linked, catalog/layer schemas exist, and the setup assumptions are noted for the team. | S | M1 | Done |
| E1-I2 | Keep the mock-data generation script documented as the first shared project command. | Let every team member regenerate the same source data without guessing the command. | Mock generation script exists and is documented; upload, ingestion, test, and clean commands can be added later when those workflows are stable. | S | M5 | Done |
| E1-I3 | Record the team ownership model and how work moves between `To Do`, `In Progress`, `Review`, and `Done`. | Make responsibilities visible without forcing a fixed delivery sequence. | This backlog and/or runbook shows owner, status meaning, and review expectations. | S | M5 | Done |

### Epic 2 - Source Data Landing and Raw Data Capture

**Premise:** The team needs a reliable raw-data base before cleansing, DQ, and AI-safety work can be trusted.

**Outcome:** Generated source files can be loaded into raw/Bronze tables with enough metadata to trace and rerun the load.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E2-I1 | Prepare and smoke-test the raw-data storage locations for generated source files. | Give each source table a predictable place to land before ingestion. | Landing locations exist or are documented for all generated tables, and a small upload demo to Databricks Volume has succeeded. | S | M1 | Done |
| E2-I2 | Create raw/Bronze tables for all source files using source-like columns plus standard metadata. | Preserve the original data while adding fields needed for audit, rerun, and lineage. | All expected raw tables exist and include required metadata columns. | M | M1 | Review |
| E2-I3 | Load generated source files into the raw/Bronze tables in a repeatable way. | Prove the team can move data from local mock output into the platform without duplicating rows. | Re-running the same load does not duplicate records; source file metadata is available. | M | M1 | Review |
| E2-I4 | Load the defects manifest as a reference table for validation. | Bring the known expected data problems into the platform for later reconciliation. | Manifest table exists and contains the generated defect rows. | S | M2 | To Do |
| E2-I5 | Add a simple raw-load validation check. | Catch missing files, missing rows, or broken ingestion early. | Row counts match source files, and the check result is saved or printed for review. | S | M5 | To Do |

### Epic 3 - Data-Quality Rules and Issue Handling

**Premise:** The assignment requires bad records to be found by executable checks, not only described in documentation.

**Outcome:** Data-quality rules can be run, failed records are captured with reasons, and results can be compared with the defects manifest.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E3-I1 | Create a data-quality rule inventory from the generator, manifest, and data-model documents. | Make the expected rule set visible before implementing individual checks. | Rule inventory includes rule ID, target data, short description, severity, and expected handling. | M | M2 | To Do |
| E3-I2 | Group rules by implementation pattern, such as single-record checks, relationship checks, duplicate checks, text checks, or AI-exclusion checks. | Help the team implement rules with the right level of logic instead of forcing one pattern onto all checks. | Each rule has an implementation pattern and owner. | S | M2 | To Do |
| E3-I3 | Build the common quarantine output for failed records. | Keep a consistent record of what failed, why it failed, and which run found it. | Quarantine output contains run ID, rule ID, source table, record key, reason, severity, and raw-record reference or snapshot. | M | M2 | To Do |
| E3-I4 | Implement executable checks for the highest-value or first-slice rules. | Prove the rule approach on a small useful set before implementing all rules. | Selected rules produce pass/fail results and quarantine rows that can be reviewed. | M | M2 | To Do |
| E3-I5 | Expand rule execution until all injected rules are covered. | Satisfy the assignment requirement that all intended defects are checked. | All expected rule IDs have runnable checks and result records. | L | M2 | To Do |
| E3-I6 | Compare actual quarantined records with the defects manifest. | Prove that the DQ implementation catches the intended bad records and avoids unexpected false positives. | Reconciliation report shows match quality per rule, with exceptions explained. | M | M5 | To Do |

### Epic 4 - Silver Data Products and Sensitive-Data Treatment

**Premise:** The raw data needs to become typed, cleaner, and safer before it can support investigation or AI use.

**Outcome:** Silver tables provide useful investigation data while treating sensitive fields according to policy.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E4-I1 | Build the first Silver table or small table group that proves the raw-to-clean pattern. | Learn the transformation, typing, DQ, lineage, and privacy pattern on a small slice. | One useful Silver output exists, matches its contract, and has basic validation evidence. | M | M3 | To Do |
| E4-I2 | Build Silver outputs for customer, employee, account, and card data. | Provide clean people and account entities while protecting direct identifiers and card data. | Silver outputs exist; sensitive fields are masked, tokenized, hashed, reduced, or removed as appropriate. | M | M3 | To Do |
| E4-I3 | Build Silver outputs for transactions, authorization activity, and device data. | Provide clean transaction activity for investigations and AI context. | Silver outputs exist with typed amounts/timestamps and protected device-related fields. | M | M4 | To Do |
| E4-I4 | Build Silver outputs for disputes, chargebacks, fraud alerts, cases, notes, links, parties, contact logs, and reference tables. | Provide the investigation story around each transaction or case. | Silver outputs exist and match the documented purpose, keys, and accepted values. | L | M5 | To Do |
| E4-I5 | Standardize values that should use a controlled set, while preserving evidence of invalid raw values. | Make Silver easier to query without hiding that the source contained defects. | Controlled fields are conformed or flagged; original defect evidence remains available. | S | M5 | To Do |
| E4-I6 | Redact sensitive values from free-text notes and logs. | Prevent emails, phone numbers, card numbers, or other unsafe text from leaking into Silver or AI context. | Redacted text is available; rows with remaining leaks are flagged or quarantined. | M | M5 | To Do |
| E4-I7 | Handle duplicate and broken-relationship records in a documented way. | Keep investigation evidence without letting duplicates or broken joins mislead downstream users. | Duplicates and orphan relationships are flagged, quarantined, excluded, or retained according to documented handling. | M | M4 | To Do |

### Epic 5 - Governance, Access, Lineage, and Run Evidence

**Premise:** Trustworthy data needs evidence about rules, access, masking, lineage, and pipeline runs.

**Outcome:** Governance outputs explain what happened in each run, what data is protected, and where AI-facing fields came from.

Before implementing access controls or AI-facing views, confirm which Unity Catalog security features are supported in the workspace and document the fallback if a preferred feature is unavailable.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E5-I1 | Capture one run record for each pipeline execution. | Make every demo or test run traceable. | Run tracking includes run ID, start/end time, status, and key row counts. | S | M1 | To Do |
| E5-I2 | Publish DQ summary results for each run. | Let reviewers see rule outcomes without reading every quarantined row. | DQ summary includes rule-level totals and sample failed keys. | S | M2 | To Do |
| E5-I3 | Maintain masking-policy metadata for sensitive fields. | Document how sensitive fields are protected and who can see what. | Masking policy table or document lists field, classification, method, and owner. | S | M3 | To Do |
| E5-I4 | Maintain access-policy metadata for internal, masked, auditor, and AI-consumer use. | Make allowed data use explicit instead of relying on assumptions. | Access policy table or document identifies fields allowed or blocked for each role. | M | M2 | To Do |
| E5-I5 | Implement the access controls needed for the demo environment. | Prove that sensitive data is not available to roles that should not see it. | Supported Unity Catalog controls are confirmed, fallback is documented if needed, and role-based query checks show allowed and blocked access behavior. | M | M1 | To Do |
| E5-I6 | Emit lineage for important Silver and AI-facing fields. | Let reviewers trace final values back to source fields. | Lineage evidence covers AI-facing fields and masked sensitive fields. | M | M3 | To Do |
| E5-I7 | Keep governance outputs aligned with actual pipeline behavior. | Avoid stale governance tables that describe rules or policies differently from implementation. | Spot checks show rule, policy, lineage, and run evidence match current outputs. | S | M2 | To Do |

### Epic 6 - AI-Ready Investigation Context

**Premise:** AI should read only a curated, policy-safe view of the investigation data, not the full raw or unrestricted Silver layer.

**Outcome:** The project provides an AI-ready Silver context surface with safe fields, source references, quality signals, and legal-hold exclusion.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E6-I1 | Define the AI-ready context contract. | Agree what one AI-readable investigation record should contain before building it broadly. | Contract lists grain, allowed fields, required metadata, exclusions, and source references. | S | M1 | To Do |
| E6-I2 | Build an initial AI-ready context from the first useful slice. | Prove the AI surface early and expose gaps before all tables are complete. | Sample context records exist and can be inspected safely. | M | M1 | To Do |
| E6-I3 | Expand the AI-ready context to include relevant transactions, merchants, disputes, alerts, parties, and redacted notes. | Give AI enough context to help with investigation questions while staying within policy. | Final context includes the expected investigation sections and source references. | L | M1 | To Do |
| E6-I4 | Exclude legal-hold, unsafe, unsupported, or below-threshold records from the AI-facing surface. | Prevent AI use of data that should not be used or cannot be trusted. | Query checks show excluded records do not appear in the AI surface. | M | M1 | To Do |
| E6-I5 | Add final context metadata such as quality status, masking status, warning flags, version, refresh time, and usage restrictions. | Help AI consumers and reviewers understand what the context can and cannot support. | Metadata fields are present and populated for context records. | M | M1 | To Do |
| E6-I6 | Add safety checks against raw PII, raw card numbers, and legal-hold leakage in the AI surface. | Catch unsafe exposure even if an upstream transformation changes. | Automated or repeatable checks fail when unsafe values appear. | M | M5 | To Do |

### Epic 7 - Validation, Evidence, and Scale Confidence

**Premise:** The assignment needs proof, not only implementation. Validation should grow with the features rather than wait until the final week.

**Outcome:** Tests and evidence show that ingestion, contracts, DQ, masking, AI safety, lineage, and scale behave as expected.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E7-I1 | Validate source-to-raw loading. | Prove that all expected source rows landed in the platform. | Row-count evidence compares source files with raw tables. | S | M5 | To Do |
| E7-I2 | Validate schema and contract alignment. | Prove that implemented outputs match documented structures and types. | Test or checklist covers raw metadata, Silver types, and final AI context fields. | M | M5 | To Do |
| E7-I3 | Validate data-quality and quarantine behavior. | Prove failed records are detected and recorded with useful reasons. | Manifest-vs-quarantine evidence is produced per rule. | M | M5 | To Do |
| E7-I4 | Validate masking, redaction, and access behavior. | Prove sensitive data is protected for unprivileged and AI users. | Checks show no unsafe raw PII or full card values in restricted outputs. | M | M5 | To Do |
| E7-I5 | Validate AI-safety and legal-hold exclusion. | Prove the AI surface contains only allowed records and fields. | Checks show zero legal-hold records and no forbidden columns or unsafe values. | M | M5 | To Do |
| E7-I6 | Validate lineage and source traceability. | Prove final context fields can be explained from source data. | Lineage check covers AI-facing fields. | S | M5 | To Do |
| E7-I7 | Run and record the larger-volume transaction test. | Show the pipeline can handle the expected stress-size dataset and record practical limits. | Stress run result includes command, row volume, runtime, compute size, and correctness outcome. | M | M4 | To Do |
| E7-I8 | Create a concise evidence summary for grading. | Make it easy for reviewers to see which acceptance criteria passed and where the proof is. | Evidence summary links tests, queries, outputs, screenshots, or run logs. | S | M5 | To Do |

### Epic 8 - Runbook, Demo, and Final Handoff

**Premise:** The work is only useful if another person can reproduce it and understand the controls without relying on the builders.

**Outcome:** The final handoff explains how to run, validate, inspect, and demo the pipeline.

| ID | Backlog item | Objective | Deliverable / completion evidence | Size | Assignee | Status |
|---|---|---|---|---|---|---|
| E8-I1 | Write the runbook for setup, execution, validation, and troubleshooting. | Let another engineer reproduce the project from a clean state. | Runbook includes prerequisites, commands, expected outputs, validation checks, and common issues. | M | M5 | To Do |
| E8-I2 | Prepare masked sample outputs for review. | Show representative data without exposing raw sensitive values. | Sample files or screenshots are stored in the agreed location and contain no unsafe raw PII. | S | M5 | To Do |
| E8-I3 | Prepare a short demo script. | Keep the presentation focused on the required outcomes and evidence. | Demo script covers mock generation, ingestion, Silver outputs, DQ/quarantine, masking, AI context, and tests. | S | M5 | To Do |
| E8-I4 | Perform a cold-run rehearsal by someone other than the main builder. | Prove the runbook works for a reader who does not already know the project. | Rehearsal notes show success, corrections, or remaining gaps. | M | M5 | To Do |
| E8-I5 | Finalize the submission links and evidence package. | Make grading materials easy to find. | Confluence or submission page links repo, runbook, evidence summary, sample outputs, and known limitations. | S | M5 | To Do |

---

## 6. The 36 DQ Rules - by Failure-Query Shape

Use this section as a guide for implementation planning. The exact SQL can adapt, but each rule should produce reviewable results and, where appropriate, quarantine evidence.

| Shape | Rules | Implementation guidance |
|---|---|---|
| **Single-record checks** | `DQ-TXN-AMT-POS`, `DQ-TXN-MERCH-REQ`, `DQ-TXN-TS-FUTURE`, `DQ-ACC-OPENDATE-FUTURE`, `DQ-CUST-EMAIL-FMT`, `DQ-CARD-EXPIRED-ACTIVE`, `DQ-MERCH-RISK-CASING`, `DQ-DISP-STATUS-ENUM`, `DQ-DISP-REASON-REQ`, `DQ-CASE-STATUS-ENUM`, `DQ-ALT-SCORE-RANGE`, `DQ-DEV-TYPE-REQ`, `DQ-NOTE-PII-LEAK`, `DQ-CTL-NOTE-PII`, uniqueness checks such as `DQ-CUST-ID-DUP`, `DQ-TXN-ID-DUP`, `DQ-CARD-DUP`, `DQ-EMP-EMAIL-UNIQ` | Usually implemented as a predicate, duplicate grouping, or text scan against one table. |
| **Relationship, join, or window checks** | FK checks such as `DQ-ACC-CUST-FK`, `DQ-TXN-ACCT-FK`, `DQ-AUTH-TXN-FK`, `DQ-DISP-TXN-FK`, `DQ-CBK-DISP-FK`, `DQ-DEV-TXN-FK`, `DQ-CASETXN-TXN-FK`; plus `DQ-TXN-CARD-ACTIVE`, `DQ-AUTH-TS-ORDER`, `DQ-CASE-STALE`, `DQ-CUST-NEAR-DUP`, `DQ-EMP-NAME-NEAR-DUP`, `DQ-CASEPARTY-RESOLVE`, `DQ-CASEPARTY-TYPE-ENUM`, `DQ-CTL-DNC-VIOLATION` | Needs joins, anti-joins, grouping, or window logic. Do not reduce these to row-only checks. |
| **AI-exclusion checks** | `DQ-CASE-LEGALHOLD`, `DQ-NOTE-LEGALHOLD` | Produces evidence and prevents affected case or note content from reaching the AI-facing surface. |

Expected counts can change when the seed or generator changes. Tests should read expected records from the manifest instead of hardcoding counts.

---

## 7. Governance Outputs

The implementation can use tables, views, files, or documented query outputs, but the final evidence should cover these governance needs.

| Governance output | Purpose | Minimum evidence |
|---|---|---|
| Rule inventory | Shows what DQ checks exist and how they should behave. | Rule ID, target data, severity, category, and handling. |
| DQ results | Summarizes pass/fail outcomes for each run. | Run ID, rule ID, totals, failed count, and sample keys. |
| Quarantine records | Stores detailed evidence for failed records. | Run ID, rule ID, table, record key, reason, severity, and raw-record reference or snapshot. |
| Pipeline run tracking | Shows what happened in each execution. | Start/end, status, row counts, and output locations. |
| Lineage evidence | Explains where important final fields came from. | Source-to-target mapping for AI-facing and sensitive fields. |
| Masking policy | Explains how sensitive fields are protected. | Field, classification, protection method, and allowed role. |
| Access policy | Explains who can read what. | Role or use case mapped to allowed, masked, or blocked fields. |

---

## 8. Test Strategy

| Test area | What it proves |
|---|---|
| Source-to-raw counts | All generated source rows landed in raw/Bronze storage. |
| Contract checks | Raw, Silver, and AI-facing outputs match documented structures. |
| Manifest-vs-quarantine | Intended defects are detected and recorded correctly. |
| DQ summary consistency | DQ result totals match quarantine evidence. |
| Masking and redaction | Restricted outputs do not expose unsafe raw sensitive values. |
| Access behavior | Roles can read only what policy allows. |
| AI-safety checks | Legal-hold and unsupported data do not appear in the AI surface. |
| Lineage completeness | AI-facing fields can be traced to source data. |
| Run reproducibility | A clean rerun produces explainable and consistent outputs. |
| Stress run | The larger-volume dataset completes with correctness evidence. |

---

## 9. Risks and Adaptation Points

1. **Unity Catalog feature support may be limited.** Confirm supported controls before implementing access or AI-facing views, then document fallback controls that preserve the same privacy outcome.
2. **The team may overbuild setup before learning what is needed.** Keep setup tied to a visible deliverable or validation need.
3. **Cross-record DQ checks may be slower at larger volume.** Implement them in a way that can be measured and optimized individually.
4. **Masking may protect too little or too much.** Pair implementation with role-based query checks.
5. **Expected defect counts may drift.** Use the manifest as the oracle instead of fixed counts.
6. **Free-text notes can leak sensitive data.** Treat redaction and leak tests as required for AI safety.
7. **A polished final demo can hide weak evidence.** Keep evidence linked to runnable checks, not only screenshots.

---

## 10. Collaboration Model

This backlog should be used as a pull-based workboard:

- Start with a thin useful slice, then expand by table group, rule group, or evidence gap.
- Work can happen in parallel when outputs and contracts are clear.
- If an item becomes too large, split it by source table group, rule group, or validation type.
- If a setup item has no clear consumer, pause it until the consumer is known.
- If a later epic exposes missing platform or governance work, add that work to the most suitable epic instead of treating the backlog order as fixed.

Suggested ownership lanes:

| Lane | Typical owner | Pulls work from |
|---|---|---|
| Platform and flow | M1 | E1, E2, E5, E6 |
| DQ and governance | M2 | E3, E5 |
| People/account Silver and privacy | M3 | E4, E5 |
| Transaction activity and scale | M4 | E4, E7 |
| Investigation data, tests, and demo | M5 | E4, E7, E8 |

---

## 11. Release Checklist

- [ ] Source data can be generated and loaded into raw/Bronze storage. *(Bronze ingestion is written and under review.)*
- [ ] Silver outputs exist for the required source domains.
- [ ] DQ checks run and produce rule-level summaries.
- [ ] Failed records are quarantined with useful reasons.
- [ ] Sensitive fields are masked, redacted, tokenized, hashed, removed, or access-controlled before AI use.
- [ ] The AI-facing Silver surface contains only allowed fields and excludes legal-hold or unsupported records.
- [ ] Governance evidence exists for runs, DQ, quarantine, masking, access, and lineage.
- [ ] Automated or repeatable validation proves the main controls.
- [ ] Larger-volume stress evidence is recorded.
- [ ] A second engineer can follow the runbook and reproduce the demo.

---

## 12. Source-of-Truth Files

- `docs/data-model.md` - contracts, enums, sensitive-data guidance, quarantine schema, and final AI-context expectations.
- `docs/bronze-layer.md` - raw/Bronze metadata requirements and ingestion guidance.
- `mock/config.py` - source table schemas, generation order, enums, and configured run values.
- `mock/generators.py` - generated source data and injected defect rules.
- `data/raw/_defects_manifest.csv` - validation oracle for expected bad records after mock data generation.
