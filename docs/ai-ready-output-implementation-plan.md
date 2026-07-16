# AI-Ready Output: Design and Implementation Plan

## 1. Decision summary

Build one curated Databricks Delta table, `gold.investigation_context`, with one row/document per safe investigation case. This is the retrieval unit for an internal transaction-investigation AI assistant.

This design is deliberately context-first:

- Do not build a Gold star schema for this assignment.
- Do not export every cleaned Silver table as CSV.
- Use explicit field allow-lists; never use `select("*")` in Gold.
- Commit only a small, masked JSON output sample because JSON represents nested arrays better than CSV.
- Generate the summary deterministically from approved fields; do not call an LLM.

The implementation must satisfy the official requirements for a curated output, masking, quality status, source traceability, context versioning, generated output samples, safe/unsafe question examples, and executable validation.

## 2. Deliverables

Implement the milestone with these artifacts:

| Artifact | Purpose |
|---|---|
| `pipeline/gold/01_gold_investigation_context.py` | Build and overwrite the current-state Gold Delta table. |
| `pipeline/validation/validate_m3_gold.py` | Fail-fast executable validation for Gold safety and contract checks. |
| `docs/models/gold_investigation_context.yml` | Machine-readable model metadata and field-level AI/PII policy. |
| `data/sample/investigation_context.sample.json` | Three to five reviewed, masked example documents. |
| Existing runbook/README and job definition | Add Gold build, validation, output locations, and troubleshooting commands. |

Do not create a generic Gold framework or one notebook per source table. This milestone has one output and should have one focused builder.

## 3. Inputs and boundaries

### 3.1 Silver content inputs

Use these cleaned Silver models:

| Area | Models | Gold use |
|---|---|---|
| Case | `investigation_cases`, `case_transactions`, `case_parties` | Base grain, transaction links, safe party roles. |
| Transaction evidence | `transactions`, `auth_attempts` | Transaction facts and authorization decisions. |
| Payment context | `accounts`, `cards` | Product/status and masked PAN last four only. |
| Merchant context | `merchants`, `merchant_categories` | Merchant risk and category descriptions. |
| Investigation outcome | `fraud_alerts`, `disputes`, `chargebacks`, `investigation_notes` | Alerts, dispute/chargeback progress, and safe notes. |
| Reference enrichment | `channels`, `fraud_types`, `dispute_reason_codes` | Human-readable descriptions. |

Use the existing `silver.quarantine_records` to derive warning flags. Continue using `gov.metadata_lineage` and `gov.masking_policies` as governance evidence; extend their documentation/rows for Gold fields where the existing conventions require it.

### 3.2 Intentionally excluded from retrievable content

- `customers`: direct identity is unnecessary for the investigation question set.
- `employees`: do not expose investigator identity or contact information.
- `transaction_devices`: the current contract excludes device/IP identifiers from Gold.
- `customer_contact_logs`: free-text and communication-preference risk is outside this scenario.
- `date_dim`, `branches`, `countries`, `currencies`, `case_status_types`: no material context beyond fields already carried by the selected inputs.
- Defect manifests and rule registries: validation/governance evidence, not business context.

An excluded model may support validation or lineage without making its business fields retrievable.

## 4. Public Gold contract

Create `gold.investigation_context` with this schema. Nested fields must be `ARRAY<STRUCT<...>>`, not JSON strings inside the Delta table.

| Field | Type | Required behavior |
|---|---|---|
| `case_id` | string | Primary key; one row per case. |
| `context_category` | string | Constant `transaction_investigation`. |
| `case_context` | struct | `priority`, `status_code`, `fraud_type_code`, `fraud_type_description`, `opened_at`, `closed_at`. |
| `case_summary` | string | Deterministic sentence built from `case_context`; no generated inference. |
| `linked_transactions` | array of structs | `transaction_id`, `amount`, `currency`, `channel_code`, `channel_name`, `txn_ts`, `status`. |
| `payment_instrument_context` | array of structs | Per linked transaction: `transaction_id`, `account_product_type`, `account_status`, `card_type`, `card_last4`, `card_status`. Never include `customer_id`, `account_id`, `card_id`, or full/masked PAN text. |
| `merchant_context` | array of structs | `merchant_id`, `merchant_name`, `mcc`, `category_name`, `category_group`, `country`, `risk_rating`, `merchant_status`. |
| `authorization_context` | array of structs | `attempt_id`, `transaction_id`, `decision`, `decline_reason`, `auth_ts`. |
| `dispute_context` | array of structs | `dispute_id`, `transaction_id`, `reason_code`, `reason_description`, `amount`, `status`, `raised_at`, plus nested safe chargeback structs. |
| `fraud_alerts` | array of structs | `alert_id`, `transaction_id`, `rule_name`, `score`, `triggered_at`, `disposition`. |
| `party_context` | array of structs | `party_type` and `role` only. Do not expose `party_id`. |
| `safe_notes` | array of structs | `note_id`, `note_text`, `created_at`; never expose `author_employee_id`. Rows quarantined for PII or legal hold are absent. |
| `quality_status` | string | `pass` or `partial`. A `fail` case is excluded rather than published. |
| `masking_status` | string | Constant `masked` after all safety checks pass. |
| `warning_flags` | array of strings | Sorted, distinct flags described below. |
| `source_references` | array of structs | Distinct `{source_table, source_record_id}` pairs for included records. |
| `usage_restrictions` | string | Constant `internal_only`. |
| `pipeline_run_id` | string | Common current Silver snapshot run ID. |
| `context_version` | string | Start at `1.0.0`. |
| `last_refreshed_at` | timestamp | Gold processing timestamp in UTC. |

`card_last4` must be derived from the already masked Silver `cards.pan`; retain only the final four characters. Do not copy the `XXXX-XXXX-XXXX-` prefix into Gold.

## 5. Data flow and transformation rules

### 5.1 Validate the snapshot before joining

1. Read all required Silver tables from the selected `catalog` widget.
2. Confirm every required input exists and contains the same current `_batch_id`/`_run_id` snapshot used by `silver_all_tables.py`.
3. Raise an error before writing Gold if required tables disagree on the snapshot.
4. Treat lookup tables with no matching row as missing enrichment, not permission to expose the raw value differently.

### 5.2 Build from the safe case base

Use `silver.investigation_cases` as the base. Apply a second Gold guard:

```text
case_id is not null
AND legal_hold = false
```

Also anti-join case-level quarantine failures for the current run. This defense-in-depth check prevents an upstream regression from publishing legal-hold or failed cases.

### 5.3 Pre-aggregate before joining

Build one DataFrame per nested collection, grouped by `case_id`, and only then join those aggregates to the case base. This avoids a transaction x alert x dispute x note Cartesian multiplication.

For every collection:

- Use explicitly named columns.
- Remove duplicates before aggregation.
- Use deterministic ordering, for example `sort_array(collect_set(struct(...)))` where field ordering supports it.
- Convert a missing collection to a correctly typed empty array, not `null`.

Join path summary:

```text
investigation_cases
  -> case_transactions -> transactions
       -> accounts -> cards
       -> channels
       -> merchants -> merchant_categories
       -> auth_attempts
       -> fraud_alerts
       -> disputes -> dispute_reason_codes -> chargebacks
  -> case_parties
  -> investigation_notes
  -> fraud_types
```

### 5.4 Quality status and warnings

Set warning flags with these minimum rules:

| Condition | Flag |
|---|---|
| No valid linked transaction remains | `partial_data` |
| A quarantined `investigation_notes` record maps to the case | `redacted_notes` |
| A quarantined `case_transactions` record maps to the case | `transaction_link_removed` |
| A quarantined `case_parties` record maps to the case | `party_link_removed` |

Map quarantine rows to cases using `raw_record` JSON fields, not substring matching against arbitrary text. Use the current pipeline run only.

Set:

```text
quality_status = "pass"    when warning_flags is empty
quality_status = "partial" when warning_flags is non-empty
```

Cases failing case-level safety or quality rules are excluded. `partial` cases remain retrievable with their warnings because the limitation is explicit.

### 5.5 Deterministic summary

Create `case_summary` with Spark string expressions from approved fields, for example:

```text
"<priority> priority <fraud-type-description> investigation opened <opened-at>; current status <status-code>."
```

Do not summarize notes or infer conclusions. Missing optional descriptions must produce neutral text such as `unknown fraud type`, not invented information.

### 5.6 Write behavior

- Create the `gold` schema if it does not exist.
- Write the current snapshot with Delta `mode("overwrite")` and `overwriteSchema=true`, matching the current-state Silver convention.
- The write must be idempotent for the same inputs.
- Append Gold field-level rows to `gov.metadata_lineage` using the existing lineage schema and remove/rewrite prior lineage rows for this target to prevent duplicates.
- Print row counts, pass/partial counts, excluded-case counts, and a non-sensitive sample.

## 6. YAML metadata and JSON sample

Create `docs/models/gold_investigation_context.yml` with:

- Model name, description, grain, owner/team, storage format, and refresh mode.
- All source models.
- Every top-level field and nested structure.
- Required/optional status and semantic meaning.
- PII classification and transformation.
- `ai_allowed` boolean.
- Quality rules and exclusion behavior.
- `context_version` policy and known limitations.

Export three to five `g3_test` records as newline-delimited JSON after Gold validation passes. Review the sample for PII patterns before placing it in `data/sample/`. Do not commit a full dataset or any Bronze values.

## 7. Validation and test evidence

Implement `validate_m3_gold.py` as an executable Databricks validation notebook. It must raise an exception on blocking failures and check:

1. `gold.investigation_context` exists and is non-empty.
2. `case_id` is non-null and unique.
3. All required top-level columns exist with the documented types.
4. Every `pipeline_run_id` matches the Silver snapshot used to build Gold.
5. No Gold case is `legal_hold=true` in Bronze or Silver.
6. `quality_status` is only `pass|partial`; `masking_status` is `masked`.
7. `context_category`, `usage_restrictions`, and `context_version` contain only approved values.
8. `source_references` is non-empty and contains no null table/record identifier.
9. No forbidden top-level or nested field names are present: customer/staff identity, account/card IDs, party IDs, device/IP fields, or PAN.
10. String content does not match the repository's email, phone, tax ID, or PAN leakage patterns.
11. Legal-hold and quarantined PII-note fixtures from `bronze.defects_manifest` do not appear in Gold.
12. Rebuilding from unchanged Silver inputs preserves the same case IDs and nested business content. Ignore `last_refreshed_at` when comparing.

Print a concise validation summary suitable for screenshots or submission evidence.

## 8. AI usage examples

Questions this context may answer:

- "Which transactions and merchants are linked to CASE-123?"
- "What fraud alerts and authorization decisions support this investigation?"
- "What is the current dispute and chargeback status for this case?"
- "What warnings or missing context should an investigator know about?"

Questions the AI must refuse or declare unsupported:

- Requests for customer names, addresses, phone numbers, email addresses, tax IDs, or full PANs.
- Requests for investigator personal information.
- Requests for legal-hold/SAR case content excluded from Gold.
- Requests for a fraud conclusion when the context only contains evidence and status.
- Questions about records absent from `source_references` or fields not in the model contract.

## 9. Implementation order

1. Add the Gold builder with input/snapshot assertions and case-level guards.
2. Implement and unit-check each pre-aggregated nested collection separately.
3. Join aggregates, calculate warnings/status, and write the Delta table.
4. Add Gold lineage rows.
5. Add `validate_m3_gold.py` and make all blocking checks pass in `g3_test`.
6. Add YAML model metadata and the reviewed JSON sample.
7. Add the Gold build and validation after Silver in the job definition and runbook.
8. Run Bronze -> DQ -> Silver -> Gold -> M1/M2/M3 validations from a clean `g3_test` state and retain the summary as submission evidence.

## 10. Definition of done

- A clean run produces one safe Gold document per eligible case.
- The output uses the expanded transaction-investigation inputs without exposing excluded identities.
- Legal-hold, failed case-level records, raw PII, and unsafe notes cannot reach Gold.
- Partial context is explicit through status and warning flags.
- Every document has run, version, refresh, restriction, and source-traceability metadata.
- The YAML contract matches the implemented Delta schema.
- A small masked JSON sample exists; full cleaned CSV exports do not.
- M1, M2, and M3 executable validations complete with no blocking failure.
