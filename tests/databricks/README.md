# Databricks Schema-Evolution Integration Test

This test creates controlled CSV batches and exercises the production Bronze
Auto Loader and Silver type-cast helpers. It is intended to produce executable
assignment evidence for added, missing, reordered, malformed, and invalid typed
source values.

## Run

1. Sync the repository into the Databricks workspace.
2. Open `tests/databricks/test_schema_evolution_databricks.py` as a notebook.
3. Attach the same Databricks environment used by the pipeline.
4. Set the `catalog` widget to `g3_test`.
5. Select **Run all**.
6. Save the completed output after the final
   `PASS: all schema-evolution integration scenarios completed` message.

The notebook refuses to run against any catalog other than `g3_test`. At the
start of every run it drops only its reserved test tables and removes only:

```text
/Volumes/g3_test/bronze/raw_data/schema_evolution_test
/Volumes/g3_test/bronze/autoloader_state/schema_evolution_test
```

The newly generated artifacts are retained after the run:

```text
g3_test.bronze.schema_evolution_test
g3_test.silver.schema_evolution_test_quarantine
g3_test.gov.schema_evolution_test_results
```

## Evidence queries

```sql
SELECT scenario, status, expected, actual, tested_at
FROM g3_test.gov.schema_evolution_test_results
ORDER BY scenario;
```

```sql
SELECT
  id,
  amount,
  currency,
  risk_score,
  _batch_id,
  _rescued_data,
  _corrupt_record
FROM g3_test.bronze.schema_evolution_test
ORDER BY _batch_id;
```

```sql
SELECT
  record_key,
  rule_id,
  failure_reason,
  disposition,
  raw_record
FROM g3_test.silver.schema_evolution_test_quarantine;
```

The expected evidence includes:

- `risk_score` added as a Bronze `STRING`;
- historical `risk_score` and missing `currency` values represented by `NULL`;
- reordered values mapped by header name;
- malformed CSV content retained in `_corrupt_record`;
- schema/type mismatches retained separately in `_rescued_data`;
- raw `not-a-number` retained in Bronze and rejected by Silver `try_cast`;
- a standard quarantine row containing the invalid raw value;
- structured `SCHEMA_EVOLUTION` warnings printed in the notebook run.
