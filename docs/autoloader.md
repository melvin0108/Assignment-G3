# Bronze Auto Loader

Run `generate_mock_databricks.py`, then run either the individual
`pipeline/bronze/bronze_<table>.py` notebooks or `bronze_all_tables.py` with a
shared catalog selection. Each loader processes unseen files with an
AvailableNow trigger.

## Paths and state

- Source: `/Volumes/<catalog>/bronze/raw_data/<table>/*.csv`
- Target: `<catalog>.bronze.<table>`
- Schema log: `/Volumes/<catalog>/bronze/autoloader_state/<table>/schema`
- Checkpoint: `/Volumes/<catalog>/bronze/autoloader_state/<table>/checkpoint`

The checkpoint provides file idempotency. Bronze is append-only and does not
perform row-level deduplication.

## Schema evolution

- Auto Loader infers CSV column names from headers and keeps all inferred
  source columns as `STRING` (`cloudFiles.inferColumnTypes=false`).
- Existing contract fields are supplied as `cloudFiles.schemaHints`; no fixed
  `.schema(...)` is supplied because that is incompatible with
  `cloudFiles.schemaEvolutionMode=addNewColumns`.
- New columns update the schema log, stop the stream once, and are retried by
  `ingest_table`. The Delta writer uses `mergeSchema=true`.
- Missing columns are retained in the Bronze schema and populated with `NULL`.
- Column reordering is accepted because Auto Loader reads inferred CSV files
  by header.
- Schema/type mismatches are retained through `_rescued_data`; malformed CSV
  rows are retained separately through `_corrupt_record`.
- Added columns, missing columns, retries, rescued-field counts, and corrupt-row
  counts are emitted as structured `SCHEMA_EVOLUTION` warnings in the notebook
  run log.

Silver uses explicit allow-lists, so an evolved Bronze column does not enter
Silver or Gold until its contract and governance treatment are reviewed.

Official behavior: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema

## Validation and reset

Run `pipeline/validation/validate_m1_bronze.py` after ingestion. It verifies
contract columns, metadata, all-string evolved fields, lineage, and reports
rescued rows.

For a controlled replay in DEV, run
`pipeline/bronze/reset_bronze_autoloader_dev.py` with `confirm_reset` set to
`RESET g3_dev`. This drops DEV Bronze targets and Auto Loader state but retains
raw source files.
