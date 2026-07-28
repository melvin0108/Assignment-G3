# Schema Evolution Implementation

This document records the implemented interpretation of
`schema-evolution-handle.md` for the G3 Databricks pipeline.

## Runtime behavior

| Change | Bronze | Silver |
|---|---|---|
| Added CSV column | Auto Loader updates its schema log, the loader retries, and Delta gains a `STRING` column. | Ignored until added to an explicit Silver contract. |
| Missing CSV column | Existing Bronze column is `NULL`; a structured warning identifies the file and column. | Requiredness and null-rate DQ rules decide whether the row is accepted. |
| Reordered columns | Read by CSV header without a drift warning. | No change. |
| Malformed CSV row | Row is retained and `_corrupt_record` is populated; a warning reports file/batch/count. | Corrupt payload is retained for investigation and is not promoted. |
| Scalar type change | Raw text remains a Bronze `STRING`. | `try_cast` produces a typed value or a type-specific quarantine row. |

Bronze uses `cloudFiles.schemaHints` for known all-string fields rather than an
explicit Spark schema, because Databricks does not permit `addNewColumns` with
`.schema(...)`. `ingest_table` retries only `UnknownFieldException`; unrelated
stream or storage errors still fail normally.

`_rescued_data` stores fields rejected because of schema, type, or case
mismatches. `_corrupt_record` separately stores CSV rows that the parser cannot
parse. Both columns are nullable Bronze metadata.

## Observability and governance

Schema events are emitted as JSON warnings with table, event type, timestamp,
file/batch context, and affected columns. Cast warnings include the record key
and a bounded raw-value sample; every cast failure is retained in
`silver.quarantine_records` with a registered `DQ-*-TYPE` rule.

Silver and Gold remain allow-list based. The project retains its required Gold
AI-ready output; schema evolution stops at Bronze until a field is explicitly
approved for Silver and, separately, for Gold.

## Verification

- Local: `python -m unittest tests.test_schema_evolution -v`
- Databricks Bronze: `pipeline/validation/validate_m1_bronze.py`
- Databricks Silver/DQ: `pipeline/validation/validate_m2_dq.py`

Controlled `g3_test` fixtures should cover added, missing, reordered, malformed,
and invalid typed values. Reset the test checkpoint before replaying a file
with an already consumed name.
