# Schema Evolution: Design and Implementation Plan

## 1. Decision summary

Keep the raw landing area lossless and make Bronze promotion contract-controlled.

- Auto Loader continues to use an explicit all-string schema and `cloudFiles.schemaEvolutionMode="rescue"`.
- Unexpected append-only values are preserved in `_rescued_data`; the Bronze table does not automatically gain new columns.
- A `schema_drift_policy` setting controls whether append-only drift fails or warns; destructive drift always fails.
- Supported values are `fail` and `warn`; default is `fail`.
- No `auto-adapt` mode is implemented for this rubric-complete submission.
- Silver and Gold keep explicit allow-lists, so an unexpected field never propagates automatically.
- Classify header changes as `append_only` or `destructive` before ingestion.
- Only append-only columns may be ingested under `rescue`; removals, renames, reorders, duplicates, case changes, and columns inserted between existing fields fail before Bronze parsing.
- `warn` applies only to append-only additions. Destructive drift always fails.

This separates two responsibilities:

1. **Preservation:** the source CSV remains unchanged in the raw Volume; safe append-only values are also retained in Bronze `_rescued_data`.
2. **Promotion:** the pipeline blocks or warns until the data contract is deliberately updated, without allowing positional CSV shifts into Bronze.

## 2. Current state and gap

`pipeline/bronze/autoloader_common.py` already configures:

```python
.schema(source_schema)
.option("cloudFiles.schemaEvolutionMode", "rescue")
.option("rescuedDataColumn", "_rescued_data")
```

Therefore, when a six-column CSV becomes an eight-column CSV, the six declared fields stay as Bronze columns and the two unexpected values are stored as a JSON string in `_rescued_data`.

Current gap: ingestion does not inspect the file header or `_rescued_data`, so it neither warns nor fails. With an explicit CSV schema, a missing or reordered middle column can shift values into the wrong declared fields because CSV parsing is positional. `validate_m1_bronze.py` verifies that the rescue column exists but does not enforce its contents.

## 3. Deliverables

| Artifact | Purpose |
|---|---|
| `pipeline/bronze/schema_contract.py` | Pure, locally testable header comparison and policy decision functions. |
| `pipeline/bronze/autoloader_common.py` | Inspect headers, block destructive drift before ingestion, and apply `fail|warn` to append-only drift after ingestion. |
| `pipeline/validation/validate_m1_bronze.py` | Validate/report current-batch drift using the same policy. |
| `tests/test_schema_contract.py` | Standard-library unit tests, including six-to-eight additions and trailing/middle ten-to-eight removals. |
| Job/runbook configuration | Pass `schema_drift_policy=fail` for the submission and document `warn` for demos. |

Do not add a generic schema registry service or automatic DDL migration.

## 4. Public configuration and interfaces

### 4.1 Databricks widget

Add a widget available to every Bronze wrapper through `autoloader_common.py`:

```text
name: schema_drift_policy
allowed values: fail, warn
default: fail
```

Reject any other value immediately with a clear `ValueError`.

The Databricks job must pass `fail` explicitly so screenshots and test evidence show the intended submission policy.

### 4.2 Pure contract comparison

Add a small module with no Spark or Databricks imports:

```python
def compare_schema_headers(expected_columns, observed_columns):
    """Return header differences and classify drift before ingestion."""

def schema_drift_action(policy, comparison, rescued_row_count=None):
    """Return action and enforcement stage; reject unsupported policies."""
```

Return a simple dictionary/dataclass with:

- `added_columns`
- `missing_columns`
- `duplicate_columns`
- `order_changed`
- `has_drift`
- `drift_class`: `none`, `append_only`, or `destructive`

`schema_drift_action` returns `action` (`pass`, `warn`, or `fail`) and `enforcement_stage` (`before_ingest` or `after_ingest`). A change is `append_only` only when the complete expected header remains an unchanged prefix and all new columns appear after it. Every other structural header change is `destructive`.

Keep the comparison case-sensitive because case-only changes can be rescued and should be reviewed.

## 5. Detection and runtime behavior

### 5.1 Inspect CSV headers

For each source file in `/Volumes/<catalog>/bronze/raw_data/<table>/`:

1. Read only enough bytes for the first CSV record with `dbutils.fs.head`; do not load a large file into driver memory.
2. Parse the header with Python's `csv.reader`, not `str.split(',')`.
3. Compare it with the table's expected columns from `TABLE_CONFIG`.
4. Record the filename and comparison result in memory for the current notebook run.

Header comparison is required because `_rescued_data IS NOT NULL` alone can miss an added column whose values are blank in every row.

Treat these as schema drift:

- Added columns at the end of an otherwise identical header: `append_only`.
- Missing column: `destructive`.
- Renamed column, represented as one missing plus one added column: `destructive`.
- Duplicate header name: `destructive`.
- Column-order change, case-only change, or a column inserted between expected fields: `destructive`.

Ignore non-CSV files and directories. If no CSV file is present, raise the existing ingestion/source error rather than reporting schema drift.

### 5.2 Enforce destructive drift before ingestion

Apply the header preflight before starting Auto Loader:

1. If any inspected file has `drift_class=destructive`, print the structured drift report and raise `RuntimeError` before parsing or writing that file to Bronze. The untouched CSV remains in the raw Volume for correction, contract promotion, and replay.
2. If every file is `none` or `append_only`, run the current Auto Loader `rescue` ingestion.
3. After `query.awaitTermination()`, query `_rescued_data` counts grouped by `_source_file` for files inspected in this run.
4. Combine header differences and rescued-value counts into one result, then apply `fail|warn` to append-only drift.
5. If rescued values appear without a matching append-only header change, classify the anomaly as destructive and fail after ingestion regardless of policy because it could not be detected during header preflight.

The structured report contains table, file, expected columns, observed columns, added/missing columns, order/duplicate status, drift class, rescued row count when available, policy, action, and enforcement stage.

Do not print full rescued payloads. Show at most a small sample with values truncated because a future unexpected field could contain PII.

### 5.3 Policy outcomes

#### `fail` — default

- For append-only drift, Bronze data remains written and unexpected populated values remain in `_rescued_data`.
- Raise `RuntimeError` after ingestion for append-only drift.
- Raise `RuntimeError` before ingestion for structural destructive drift; no row from the affected file is written to Bronze.
- Raise `RuntimeError` after ingestion for a rescued-only destructive anomaly that header inspection could not detect.
- The Bronze job task fails, so dependent DQ, Silver, and Gold tasks do not execute.
- The error must tell the operator which file/table drifted and how to use the contract-promotion workflow.

#### `warn`

- Apply only to append-only additions.
- Print the same report with `WARN` status and complete the Bronze task successfully.
- Unexpected append-only fields remain only in `_rescued_data`.
- Existing Silver and Gold allow-lists ignore them.
- Destructive drift still fails; `warn` never permits a missing, renamed, reordered, duplicate, case-changed, middle-inserted, or unexplained rescued field.

#### No drift

- Print `PASS` and continue without changing existing behavior.

## 6. Example: six columns become eight

Expected contract:

```csv
transaction_id,amount,currency,status,merchant_id,txn_ts
```

New source file:

```csv
transaction_id,amount,currency,status,merchant_id,txn_ts,risk_score,promotion_code
TXN-002,250.00,AUD,settled,MCH-002,2026-07-16T11:00:00Z,0.92,PROMO-10
```

Bronze result under `rescue`:

| transaction_id | amount | currency | status | merchant_id | txn_ts | `_rescued_data` |
|---|---:|---|---|---|---|---|
| TXN-002 | 250.00 | AUD | settled | MCH-002 | 2026-07-16 11:00 | `{"risk_score":"0.92","promotion_code":"PROMO-10","_file_path":"..."}` |

Contract comparison:

```text
added_columns   = [risk_score, promotion_code]
missing_columns = []
order_changed   = false
has_drift       = true
drift_class     = append_only
```

With `schema_drift_policy=fail`, the row remains in Bronze and the task fails after ingestion. With `warn`, the task succeeds, but neither new field becomes a Silver or Gold column.

## 7. Example: ten columns become eight

Expected ten-column customer contract:

```csv
customer_id,first_name,last_name,dob,email,phone,address,tax_id,created_at,effective_at
```

### 7.1 Two trailing columns are removed

```csv
customer_id,first_name,last_name,dob,email,phone,address,tax_id
CUST-002,Ada,Lovelace,1985-12-10,ada@example.com,+61400000000,1 Example St,TAX-002
```

Header comparison:

```text
added_columns   = []
missing_columns = [created_at, effective_at]
order_changed   = false
has_drift       = true
drift_class     = destructive
```

If positional parsing were allowed, the first eight fields would map correctly and `created_at` and `effective_at` would become null. The contract still rejects the file before ingestion because required source fields disappeared. `_rescued_data` does not preserve absent values.

### 7.2 Two middle columns are removed

If `email` and `phone` disappear, the observed header becomes:

```csv
customer_id,first_name,last_name,dob,address,tax_id,created_at,effective_at
```

Without preflight enforcement, positional CSV parsing could mis-map the remaining values:

```text
Bronze email        <- source address
Bronze phone        <- source tax_id
Bronze address      <- source created_at
Bronze tax_id       <- source effective_at
Bronze created_at   <- null
Bronze effective_at <- null
```

The header comparison reports `missing_columns=[email, phone]` and `drift_class=destructive`. The pipeline prints the drift report and fails before Auto Loader starts, under both `fail` and `warn`. No row from the affected file enters Bronze, and the unchanged source CSV remains in the raw Volume for correction or contract promotion.

## 8. Other schema-change behavior

| Change | Bronze observation | Pipeline result |
|---|---|---|
| Append-only populated column | Header difference plus JSON value in `_rescued_data`. | Ingest, then fail or warn according to policy. |
| Append-only all-null column | Header difference; rescued row count may be zero. | Ingest, then fail or warn according to policy. |
| Column inserted between expected fields | Header is not an unchanged expected prefix. | Fail before ingestion to prevent positional shifts. |
| Removed required column | Header reports missing. | Fail before ingestion; do not rely on null-filling or downstream DQ. |
| Renamed column | Old name missing and new name added. | Fail before ingestion; no automatic rename. |
| Column reorder | Header order differs. | Fail before ingestion to prevent positional misinterpretation. |
| Duplicate or case-only header change | Case-sensitive or duplicate comparison differs. | Fail before ingestion. |
| Source value changes format/type | Header unchanged; Bronze still stores string. | Silver cast/business DQ quarantines invalid values. |
| Key semantics change | May not be structural drift. | Required, uniqueness, and RI rules must fail until contract logic changes. |

## 9. Accepted-change promotion workflow

Do not edit `_rescued_data` into Silver manually. For an accepted field:

1. Classify the change as compatible or breaking and identify its business meaning.
2. Classify PII/sensitivity and set its access level (`internal_only`, `customer_facing`, or `ai_allowed`).
3. Update `docs/data-model.md` and the relevant YAML/model contract.
4. Update `mock/config.py` and generator logic if the mock source contract changes.
5. Update `TABLE_CONFIG` in `autoloader_common.py`.
6. Add explicit Silver typing, cleansing, DQ, quarantine, masking, and lineage behavior.
7. Add the field to Gold only if it is required by the AI use case and explicitly `ai_allowed`.
8. Increment `context_version` only if the Gold public contract changes.
9. Add/update tests before promotion.
10. In `g3_test`, perform the guarded clean reset and replay because Auto Loader checkpoints do not reprocess already-consumed files and the existing Delta table does not yet contain the promoted column.
11. Run M1, M2, and M3 validation before applying the same contract change to higher environments.

## 10. Unit and executable tests

### 10.1 Local unit tests

Use Python's built-in `unittest` so no new dependency is required. Add these cases to `tests/test_schema_contract.py`:

1. Exact six-column header: `drift_class=none`, action `pass`.
2. Six columns become eight at the end: both added columns reported and `drift_class=append_only`.
3. Append-only columns contain no row values: header comparison still reports drift.
4. Ten columns become eight by removing trailing columns: both missing columns reported and `drift_class=destructive`.
5. Ten columns become eight by removing middle columns: both missing columns reported, `drift_class=destructive`, and action `fail` at `before_ingest`.
6. Column inserted between expected fields: `drift_class=destructive`.
7. Column renamed: one added and one missing column, classified as destructive.
8. Columns reordered: `order_changed=true`, classified as destructive.
9. Duplicate or case-only header: classified as destructive.
10. `fail` policy with append-only drift: action `fail` at `after_ingest`.
11. `warn` policy with append-only drift: action `warn` at `after_ingest`.
12. `warn` policy with destructive drift: action `fail` at `before_ingest`.
13. Unsupported policy: `ValueError`.
14. Rescued rows with no header difference: destructive drift fails after ingestion regardless of policy.

Run with:

```powershell
python -m unittest tests.test_schema_contract -v
```

### 10.2 Databricks M1 validation

Extend `validate_m1_bronze.py` to accept the same widget and inspect the latest batch:

- Under `fail`, append-only drift or non-null `_rescued_data` causes validation failure after Bronze preservation.
- Under `warn`, append-only drift shows the affected table/file/count and finishes successfully.
- Under either policy, a destructive header change fails before ingestion; assert that no affected row enters Bronze.
- A rescued-only anomaly fails after ingestion under either policy.
- Assert that expected Bronze source columns and metadata columns still exist.
- For the controlled eight-column fixture, assert that `risk_score` and `promotion_code` are absent as top-level Bronze columns and present in `_rescued_data` when populated.
- For controlled trailing- and middle-removal fixtures, assert `drift_class=destructive`, pre-ingestion failure, and no positional values written under incorrect Bronze column names.

Use `g3_test` for the controlled fixture. Do not inject schema drift into the submission's normal `g3_dev` dataset.

## 11. Operational and security notes

- Keep the submission/default job policy at `fail`.
- Use `warn` only when intentionally demonstrating append-only capture-and-continue behavior.
- Do not implement `addNewColumns` or `addNewColumnsWithTypeWidening` for this contract-controlled pipeline.
- Do not log unrestricted rescued payloads; new fields have not yet been PII-classified.
- A failure before writing destructive drift is intentional: the raw source remains available while Bronze is protected from positional corruption.
- An append-only failure after writing is intentional: safe preservation succeeded, but contract promotion failed.
- After a failed run, do not rerun downstream notebooks manually until the drift is accepted, corrected, or the source file is replaced.

## 12. Definition of done

- The default policy is explicit, documented, and set to `fail` in the job.
- Append-only six-to-eight drift is detected even when new values are all null, and populated unexpected values are retained in `_rescued_data`.
- Trailing and middle ten-to-eight removals are classified as destructive and blocked before Bronze ingestion.
- No positional value shift from destructive drift can be written under an incorrect Bronze column name.
- `fail` blocks downstream work; `warn` continues only for append-only drift without schema promotion.
- Unit tests cover append-only, trailing/middle removal, inserted, renamed, reordered, duplicate, case-only, rescued-only, and invalid-policy cases.
- M1 provides executable evidence of the configured behavior.
- The accepted-change workflow updates contract, PII/access policy, DQ, lineage, and downstream models before replay.
- No unexpected field can enter Silver or Gold automatically.
