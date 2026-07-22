# Gold Dimensional Mart and AI-Ready Context Plan

## Summary

Build a current-state Gold fact constellation in the selected catalog's `gold` schema. All Gold tables are AI-safe, use natural business grains, and are documented with one YAML contract per model. A materialized `investigation_context` table is assembled exclusively from these Gold models.

The design follows the supervisor's guidance:

- Silver remains atomic and integrated.
- Gold becomes dimensional.
- AI retrieves complete case context directly; analytics over facts and dimensions is generated from scoped YAML metadata for execution by a future trusted broker.
- YAML metadata explains every model, relationship, field, safety rule, and supported use case.

## Gold Model Contracts

Every Gold model is an overwrite-based Delta table carrying:

- `pipeline_run_id STRING`
- `batch_id BIGINT`
- `last_refreshed_at TIMESTAMP`
- `quality_status STRING` -- `pass` or `partial`
- `warning_flags ARRAY<STRING>`
- `source_references ARRAY<STRUCT<source_table:STRING,source_record_id:STRING>>`
- `usage_restrictions STRING` -- `internal_only` for dimensions and facts; `ai_allowed` for `investigation_context`

Dimensions and facts use business IDs and natural composite grains; SHA-256 surrogate and fact keys are not published. `dim_date.date_key` uses `yyyyMMdd`; key `0` represents an unknown date.

### Dimensions

| Model | Grain and principal fields |
|---|---|
| `dim_date` | One calendar date: `date_key`, `date_id`, year, month, quarter, weekend flag, `is_unknown`. |
| `dim_case` | One eligible case keyed by `case_id`, with priority, status code/description, fraud type code/severity, opened/closed timestamps and date keys. Includes case-level quality and warnings aggregated from its facts. |
| `dim_merchant` | One merchant keyed by `merchant_id`, with name, MCC, category name/group, country, risk rating, status, effective timestamp, `is_unknown`. |
| `dim_channel` | One channel keyed by `channel_code`, with name and `is_unknown`. |
| `dim_dispute_reason` | One dispute reason keyed by `reason_code`, with description and `is_unknown`. |
| `dim_currency` | One currency keyed by `currency_code`, with name, decimals and `is_unknown`. |

Case status and fraud type are flattened into `dim_case`; merchant-category attributes are flattened into `dim_merchant`. This keeps the consumer model star-like instead of introducing unnecessary snowflakes.

### Facts

| Model | Grain and principal fields |
|---|---|
| `fact_case_transaction` | One `(case_id, transaction_id)` link with merchant, channel, currency and date business keys, amount `DECIMAL(18,2)`, timestamp and status. |
| `fact_authorization_attempt` | One `(case_id, attempt_id)` outcome with transaction ID, authorization date key, decision, decline reason and timestamp. |
| `fact_dispute` | One `(case_id, dispute_id)` record with reason/currency/date business keys, transaction ID, amount `DECIMAL(18,2)`, status and raised timestamp. |
| `fact_chargeback` | One `(case_id, chargeback_id)` record with currency/date business keys, transaction and dispute IDs, scheme, amount `DECIMAL(18,2)`, stage and processed timestamp. |
| `fact_fraud_alert` | One `(case_id, alert_id)` record with date key, transaction ID, rule name, score, disposition and triggered timestamp. |
| `fact_investigation_note` | One `(case_id, note_id)` PII-screened note with date key and created timestamp. Author identity is excluded. |
| `fact_case_party_summary` | One `(case_id, party_type, role)` count. Raw party identifiers are excluded. |

Customer, employee, account, card, device/IP, PAN, and contact-log fields are outside this Gold mart. Synthetic case, transaction, dispute, alert, note, and merchant IDs remain available as internal business identifiers.

### AI Context Interface

`gold.investigation_context` has one row per `dim_case` record and contains:

- `case_id`, `context_category`, and a case-detail struct
- A deterministic case summary; no LLM-generated inference
- Sorted arrays of linked transactions and merchant/channel descriptions
- Authorization attempts
- Disputes with associated chargebacks
- Fraud alerts
- Safe notes
- Party-type/role counts
- `quality_status`, `masking_status`, and sorted warning flags
- Distinct original source references
- `usage_restrictions`, `pipeline_run_id`, `context_version = "2.0.0"`, and refresh timestamp

Collections use typed `ARRAY<STRUCT>` values and typed empty arrays rather than JSON strings or nulls. The context contains business-readable attributes, not surrogate keys.

## Implementation and Data Flow

1. Add shared Gold helpers for catalog validation, snapshot consistency, unknown members, eligible-case filtering, standard metadata, typed arrays, and overwrite writes.
2. At the start of the Gold runner, confirm all required Silver inputs contain exactly one matching latest `_batch_id` and `_run_id`. Filter quarantine evidence to that run.
3. Build the five reference dimensions from Silver. Add a deterministic `UNKNOWN` member to each; `dim_date` uses key `0`.
4. Build facts from eligible case links first, then join the larger transaction tables. This limits processing to investigation-related records rather than scanning and publishing the entire two-million-row transaction domain.
5. Build `dim_case` after the facts so it can aggregate fact warnings, removed-link warnings, redacted-note evidence, and missing-transaction conditions.
6. Build `investigation_context` only after every Gold dimension and fact reports the same run and batch.
7. Execute Gold notebooks through an ordered `gold_all_tables.py` runner, following the existing Silver runner pattern. Replace the stale single-context job task with `gold_all_tables`, then run `validate_m3_gold`.
8. Add a job-level `catalog` parameter defaulting to `g3_catalog` and pass it explicitly to Gold and validation tasks.
9. Label the six dimensions and seven facts as `internal_only`, and label only `investigation_context` as `ai_allowed`. Document Bronze, Silver, and quarantine as non-AI operational outputs; do not provision users, groups, or Unity Catalog grants in this prototype.

### Failure Policy

- Abort before publication for snapshot mismatch, duplicate declared grains, missing/null case keys, unresolved eligible-case relationships, legal-hold leakage, forbidden fields, PII leakage, or inconsistent Gold run IDs.
- Exclude legal-hold and failed cases, including their related facts.
- Preserve otherwise valid facts with missing optional enrichment by assigning the appropriate `UNKNOWN` dimension key.
- Mark unknown enrichment as `partial` and add flags such as `missing_channel_enrichment` or `missing_dispute_reason_enrichment`.
- Mark cases with no surviving transaction as `partial_data`.
- Derive `redacted_notes`, `transaction_link_removed`, and `party_link_removed` by parsing quarantine `raw_record` JSON and joining identifiers; do not use substring matching.
- Treat legitimately absent alerts, disputes, notes, or chargebacks as empty collections, not failures.

## Metadata and AI Guidance

Create one YAML file per Gold model under the Gold model documentation directory. Each contract contains:

- Model name, type, purpose, grain, owner, storage and refresh mode
- Natural business keys and composite grains
- Source Silver models
- Columns, types, required status, semantic definitions, and allowed values
- Fact-to-dimension join relationships
- Measures and aggregation guidance
- PII classification and model-specific `internal_only` or `ai_allowed` status
- Quality, exclusion, unknown-member, and warning behavior
- Usage restrictions and known limitations
- Example AI questions supported by that model

At query time, the AI reads `questions-to-metrics.yaml` first. A matched route
provides the metric IDs plus the primary and supporting tables, after which only
those model YAML contracts are loaded for grain, relationships, dimensions, and
metric expressions. Detail routes use `gold.investigation_context`; analytics
SQL over internal models is returned to the user or, in a future phase, sent to
an allowlisted trusted broker. Unmatched or ambiguous questions do not trigger
full-schema discovery and must be clarified or reported as unsupported.

Update the Gold architecture documentation and data model to show the constellation, join paths, context derivation, answerable questions, and required refusals. Export three to five validated `g3_catalog` context records as reviewed JSON samples; do not commit full datasets.

## Test and Acceptance Plan

### Model and Data Tests

- Every expected Gold table exists, is non-empty, and matches its documented schema.
- Dimension business keys and fact natural grains are non-null and unique.
- Every fact foreign key resolves to a real or documented `UNKNOWN` dimension member.
- Fact counts reconcile to eligible Silver records through their declared join paths.
- All Gold models share one `pipeline_run_id` and `batch_id`.
- Rebuilding from unchanged Silver inputs preserves all business content, ignoring refresh timestamps.

### Safety Tests

- No legal-hold case or related fact reaches Gold.
- No forbidden customer, employee, account, card, party-ID, device/IP, PAN, phone, email, tax-ID, or address fields exist.
- All note text passes the repository's PII/PAN leakage expressions.
- `fact_case_party_summary` contains counts only, never raw party identifiers.
- Only approved values appear in quality, restriction, masking, and context-version fields.
- Policy metadata marks only `investigation_context` for direct AI access; all facts and dimensions are `internal_only` broker sources.

### Context Reconciliation

- Exactly one context row exists per `dim_case`.
- Nested array counts reconcile to the corresponding Gold facts.
- Every nested item is traceable through `source_references`.
- Missing collections are typed empty arrays.
- Context summaries are deterministic and do not infer guilt or fraud conclusions.
- Cases with fact or quarantine warnings are `partial`; clean cases are `pass`.
- Context validation fails if it reads from Silver or exposes legacy hashed keys.

### Documentation and Pipeline Evidence

- Every Gold table has exactly one YAML contract, and documented fields/types match the Delta schema.
- YAML relationships match implemented foreign keys.
- The complete `g3_catalog` job runs Bronze -> DQ -> Silver -> Gold -> M3 validation successfully.
- Validation prints table counts, pass/partial counts, exclusions, unknown-member usage, referential-integrity results, safety results, and policy-label evidence.

## Assumptions

- `g3_catalog` is the catalog; `bronze`, `silver`, `gold`, and `gov` are schemas beneath it.
- Gold remains current-state Type 1 because Silver does not preserve SCD2 history.
- `investigation_context` is AI-allowed; PII-safe supporting Gold models are internal-only, while restricted operational models stay in Bronze/Silver.
- YAML contracts provide question-to-metric routing and scoped technical metadata; no Unity Catalog Metric Views are created.
- No chatbot, query broker implementation, vector database, semantic search index, LLM summary, dashboard, or full enterprise warehouse is added.
- Gold tables are not partitioned because only case-linked investigation records are published.
- Rebuilds are batch operations with no supported concurrent-reader atomicity guarantee.
- The reverted context-only implementation is not restored wholesale, and the user's deleted/unstaged files are preserved.
