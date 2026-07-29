# Schema Evolution Handling

> **Document type:** Submission-ready, as-implemented specification  
> **Input format:** CSV with a required header row  
> **Pipeline scope:** Landing → Bronze → Silver → Gold  
> **Runtime:** Databricks Auto Loader, PySpark, and Delta Lake  
> **Supported catalogs:** `g3_dev`, `g3_test`, and `g3_catalog`  
> **Last updated:** 2026-07-29

## 1. Purpose and Handling Boundary

Schema evolution is a change in the structure or interpretation of source data
between CSV batches. The G3 pipeline preserves the raw input in Bronze, applies
explicit type and data-quality contracts in Silver, and exposes only reviewed
fields in Gold.

```text
CSV landing files
    ↓
Bronze: additive schema evolution, raw STRING values, parser evidence
    ↓
Silver: explicit allowlist, try-cast, DQ checks, quarantine
    ↓
Gold: explicit dimensional and AI-ready contracts
```

Only Bronze evolves automatically. A new source column does not enter Silver
or Gold until its data type, DQ rules, PII classification, masking, lineage,
and downstream use have been reviewed and implemented.

The production behavior is implemented by:

- `pipeline/bronze/schema_evolution.py` — CSV-header comparison and bounded
  retry helpers.
- `pipeline/bronze/autoloader_common.py` — shared Bronze ingestion,
  observability, and Auto Loader state management.
- `pipeline/silver/type_cast.py` — explicit Silver casts, warnings, and
  standard quarantine records.

## 2. Bronze Auto Loader Configuration

Each table reads files from
`/Volumes/<catalog>/bronze/raw_data/<table>/` and writes to
`<catalog>.bronze.<table>`. Auto Loader state is isolated by table:

| State | Location |
|---|---|
| Schema log | `/Volumes/<catalog>/bronze/autoloader_state/<table>/schema` |
| Checkpoint | `/Volumes/<catalog>/bronze/autoloader_state/<table>/checkpoint` |

The shared reader and writer use the following effective configuration:

```python
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{state_root}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaHints", schema_hints)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("rescuedDataColumn", "_rescued_data")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("mode", "PERMISSIVE")
    .option("header", "true")
    .option("multiLine", "true")
    .load(source_path)
)
```

The writer uses Delta, `mergeSchema=true`, append output mode, and an
`availableNow` trigger. Existing contract columns are supplied as
`cloudFiles.schemaHints`; the pipeline does not pass a fixed `.schema(...)`,
because that would prevent additive evolution with `addNewColumns`.

All source columns, including newly discovered columns, are stored as
`STRING`. Bronze also appends:

| Metadata column | Meaning |
|---|---|
| `_source_file` | Source file name recorded by Databricks file metadata |
| `_source_file_mod_time` | Source file modification timestamp |
| `_ingest_ts` | Ingestion timestamp |
| `_run_id` | Run identifier derived from the numbered CSV batch |
| `_batch_id` | Numeric suffix parsed from `<number>.csv`; otherwise `NULL` |
| `_source_record_id` | Deterministic concatenation of configured record-key fields |
| `_record_hash` | SHA-256 fingerprint of the raw source values and corrupt payload |
| `_rescued_data` | Data rescued because of schema, type, or case mismatch |
| `_corrupt_record` | Entire CSV payload retained when the parser cannot parse the row |

The checkpoint provides file-level idempotency. Bronze remains append-only and
does not perform row-level deduplication.

## 3. Handling Matrix

| Source change | Bronze behavior | Silver and Gold behavior |
|---|---|---|
| **Added column** | Header inspection emits `added_columns`. Auto Loader updates its schema log, may stop with `UnknownFieldException`, and is retried. Delta gains the new `STRING` column through `mergeSchema`; historical rows receive `NULL`. | The field is ignored until explicitly added to the Silver contract. Gold requires a separate contract change. |
| **Missing column** | The historical Bronze column remains in the table and the new row receives `NULL`. Header inspection emits `missing_columns`. | Requiredness and DQ rules determine whether the row is accepted, warned, or quarantined. The contract is not automatically removed. |
| **Reordered columns** | Values are mapped by CSV header name. Reordering alone is recorded by the comparison helper but is not treated as drift and emits no added/missing warning. | No contract change. |
| **Malformed CSV row** | `PERMISSIVE` mode retains the raw payload in `_corrupt_record`; `malformed_csv_rows` reports the affected batch and count. | Corrupt records remain available for investigation and must not be promoted as clean data. |
| **Schema/type/case mismatch handled by Auto Loader** | The rejected content is retained in `_rescued_data`; `rescued_data_rows` reports the affected batch and count. | The mismatch must be reviewed before downstream promotion. |
| **Scalar type change or invalid typed value** | Bronze retains the original text because source fields are `STRING`. | Configured Silver `TypeCastRule` objects use `try_cast` or an explicit tolerant expression. A nonblank value that produces `NULL` is excluded from the clean result and represented in `silver.quarantine_records`. |
| **Expected additive-schema stop** | Only an exception chain containing `UNKNOWN_FIELD_EXCEPTION` or `UnknownFieldException` is retryable. A `schema_restart` warning is emitted before retry. | Not applicable. |
| **Unrelated runtime or storage failure** | The exception is re-raised immediately. It is not hidden or converted into a schema-evolution retry. | Normal pipeline failure handling applies. |

Before ingestion, each unseen CSV header is parsed with Python's CSV parser.
This supports a UTF-8 BOM and quoted header values containing commas. An empty
file or a header containing only blank column names raises `ValueError`.

Known columns evolve cumulatively: newly observed names are appended, while
temporarily missing historical names remain known. The retry limit is
`max(2, inspected_file_count + 1)`, so retries are bounded even when several
new files are inspected in one run.

## 4. Rescued Data, Corrupt Rows, and Quarantine

The three failure channels have different meanings and are not interchangeable:

| Channel | Layer | Granularity | Purpose |
|---|---|---|---|
| `_rescued_data` | Bronze | Fields rejected by Auto Loader | Preserve schema, type, or case mismatches without losing the source row |
| `_corrupt_record` | Bronze | Entire malformed CSV row | Preserve input that the CSV parser cannot decode into normal columns |
| `silver.quarantine_records` | Silver | One row per failed record and cast/DQ rule | Preserve rejected business records with rule, reason, disposition, and raw evidence |

For every configured `TypeCastRule`, Silver:

1. Adds a typed working column without overwriting the raw Bronze value.
2. Treats a non-null, nonblank source value whose typed value is `NULL` as a
   cast failure.
3. Creates a standard quarantine record containing the source table, source
   record ID, record key, rule ID, failure reason, disposition, raw record,
   run ID, and detection timestamp.
4. Excludes records with any configured cast failure from the corresponding
   clean Silver output.

Blank strings are not classified as cast failures by the casting helper;
separate requiredness or format DQ rules handle those values.

## 5. Observability

Schema-evolution warnings are JSON messages written through the
`g3.schema_evolution` logger. Every message contains:

- `event: "SCHEMA_EVOLUTION"`
- `event_type`
- `table`
- an ISO-8601 UTC `timestamp`

Event-specific fields are:

| Event type | Additional context |
|---|---|
| `added_columns` | `columns`, `file_path`, `batch_id` |
| `missing_columns` | `columns`, `file_path`, `batch_id` |
| `schema_restart` | `attempt`, `reason` |
| `rescued_data_rows` | `file_path`, `batch_id`, `rescued_rows` |
| `malformed_csv_rows` | `file_path`, `batch_id`, `corrupt_rows` |
| `type_cast_failed` | `column`, `target_type`, `record_key`, `raw_value`, `batch_id` |
| `type_cast_failed_summary` | `column`, `failed_rows`, `logged_samples` |

Type-cast logging is bounded to 100 record samples per rule evaluation. When
more failures exist, `type_cast_failed_summary` reports the total count and
number of logged samples. Full rejected records remain in the access-controlled
quarantine table rather than being copied into logs.

## 6. Governance and Promotion Rules

- Bronze prioritizes preservation and traceability. It stores raw scalar
  values as strings and retains parser/schema evidence.
- Silver is contract-driven. Types are introduced only through configured
  cast rules, and invalid records are quarantined rather than silently dropped.
- Gold remains an explicit dimensional and AI-ready contract. Bronze schema
  growth never changes Gold automatically.
- A new field is promoted only after its type, nullability, DQ, PII/masking,
  access, and lineage requirements are approved.
- Resetting a checkpoint or schema log can cause files to be reprocessed and
  is an explicit operational action, not part of normal schema evolution.

## 7. Verification and Evidence

For executable Databricks evidence:

1. Open `tests/databricks/test_schema_evolution_databricks.py` as a notebook.
2. Attach the pipeline's Databricks environment.
3. Set `catalog` to `g3_test`; the test refuses other catalogs.
4. Run all cells and retain the output ending with
   `PASS: all schema-evolution integration scenarios completed`.

The integration notebook covers:

- an evolved `risk_score` column stored as Bronze `STRING`;
- `NULL` values for historical or currently missing fields;
- header-name mapping after column reordering;
- malformed CSV preservation in `_corrupt_record`;
- separation of `_rescued_data` and `_corrupt_record`;
- raw invalid numeric text retained in Bronze;
- Silver `try_cast` failure, warning, and standard quarantine output.

Evidence can be queried with:

```sql
SELECT scenario, status, expected, actual, tested_at
FROM g3_test.gov.schema_evolution_test_results
ORDER BY scenario;
```

```sql
SELECT
  id, amount, currency, risk_score, _batch_id,
  _rescued_data, _corrupt_record
FROM g3_test.bronze.schema_evolution_test
ORDER BY _batch_id;
```

```sql
SELECT record_key, rule_id, failure_reason, disposition, raw_record
FROM g3_test.silver.schema_evolution_test_quarantine;
```

For normal pipeline validation, run
`pipeline/validation/validate_bronze.py` after Bronze ingestion and
`pipeline/validation/validate_dq.py` after Silver/DQ processing. These
checks validate the all-string Bronze contract, required metadata, rescued and
corrupt row reporting, registered type rules, clean-table exclusion, and
quarantine completeness.

The Databricks integration output is expected evidence until the notebook has
actually been run and its completed output has been saved; this document does
not claim an unexecuted remote test as a completed result.
