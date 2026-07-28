# Schema Evolution — Handling Rules (Bronze → Silver, CSV Input)

## Definition
Schema evolution is the change in source data structure over time. The pipeline must handle these changes without losing data, crashing the entire system, or silently corrupting data.

## Architecture
```
Landing (CSV) → Bronze (permissive, never blocks ingestion) → Silver (contract enforcement, final output)
```

## Auto Loader Configuration (CSV)
```python
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", "/schema/orders_bronze")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("cloudFiles.inferColumnTypes", "false")   # read all columns as strings
.option("header", "true")                          # header is required
```

**CSV-specific considerations:**
- CSV has no nested structures (no structs or arrays), so there is no "structural mismatch" scenario as there is with JSON.
- Column names depend on the header, so `header=true` is always required.
- A column count that does not match the header because of extra or missing delimiters is a common CSV-specific error.

## Handling Rules by Scenario

**Added column** → `addNewColumns` handles the change automatically: the stream fails once, updates the schema log, restarts automatically, and writes the new column directly to Bronze as a string.
→ Log warning: "Schema evolved: new column [column_name] was added, batch [batch_id], timestamp [ts]."
→ Silver: review the column and add it to the contract only when it is needed.

**Missing column** → Delta automatically fills the column with NULL.
→ Log warning: "Schema evolved: column [column_name] was not present in batch [batch_id]; value set to NULL."
→ Silver: monitor the null rate and alert on abnormal spikes relative to the baseline.

**Column reordering** → No impact. Delta reads columns by header name, not by position.
→ No log entry is required.

**Unparseable CSV row** (malformed CSV with invalid quoting or escaping) → The raw row
is retained in `_corrupt_record`.
→ Log warning: "Malformed CSV row: [file_path]; raw payload retained."

**Scalar type change** → This does not occur in Bronze because all values are strings. Convert types in Silver with `try_cast()` rather than a strict `cast()`.
→ Parse failure → NULL; route the record to a quarantine table in Silver and do not propagate it to Gold.
→ Log warning: "Type cast failed: column [column_name], raw value [value], batch [batch_id]."

## Roles of `_rescued_data` and `_corrupt_record`
`_rescued_data` retains fields that do not match the schema, type, or case. `_corrupt_record`
retains the entire raw CSV row when the parser cannot parse it. Added and missing columns
are still handled separately by Auto Loader and Delta.

## Logging and Warning Principles
Every schema evolution event must produce a warning that includes the event type, table or column name, timestamp, batch ID, file path, and the raw value when a type cast fails.

```python
def check_schema_drift(current_schema, previous_schema, batch_id):
    added = set(current_schema) - set(previous_schema)
    missing = set(previous_schema) - set(current_schema)
    if added:
        log.warning(f"[SCHEMA_EVOLUTION] batch={batch_id} added_columns={added}")
    if missing:
        log.warning(f"[SCHEMA_EVOLUTION] batch={batch_id} missing_columns={missing}")
```

```python
corrupt_count = bronze_df.filter(F.col("_corrupt_record").isNotNull()).count()
if corrupt_count > 0:
    log.warning(f"[SCHEMA_EVOLUTION] corrupt_rows={corrupt_count} — inspect _corrupt_record")
```

```python
# Silver type casting and quarantine with a warning for each record
silver_df = bronze_df.withColumn("amount_clean", F.expr("try_cast(amount as decimal(18,2))"))

fail_df = silver_df.filter(F.col("amount").isNotNull() & F.col("amount_clean").isNull())
if fail_df.count() > 0:
    for row in fail_df.select("order_id", "amount").collect():
        log.warning(f"[SCHEMA_EVOLUTION] type_cast_failed column=amount order_id={row['order_id']} raw_value={row['amount']}")

fail_df.write.mode("append").saveAsTable("silver.orders_quarantine")
valid_df = silver_df.filter(~(F.col("amount").isNotNull() & F.col("amount_clean").isNull()))
valid_df.write.mode("append").saveAsTable("silver.orders")
```

## General Principles
1. Bronze: prioritize preventing data loss, avoid blocking the pipeline, and store scalar columns as strings.
2. Silver: the only layer that enforces types, validation, and business rules; it is the **final output of the pipeline**.
3. Invalid records → route them to a quarantine table in Silver. Do not raise an exception that terminates the batch, and do not silently drop them.
4. Every schema evolution event must produce a warning with enough context for traceability.
5. There is no Gold layer; Silver is the final stage. Downstream consumers, if any, read directly from Silver and `silver_quarantine`.

## Application to the G3 Project

The G3 project retains an AI-ready Gold layer because it is a mandatory assignment
requirement. Schema evolution expands Bronze automatically only; Silver and Gold use
explicit allowlists. Therefore, a new column is propagated to each subsequent layer
only after its contract, data quality, PII, and access policies have been reviewed.
