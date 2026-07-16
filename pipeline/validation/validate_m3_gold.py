# Databricks notebook source
# ============================================================================
# Validation: M3 Gold investigation context
# ============================================================================

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, functions as F

from pipeline.gold.contract import forbidden_field_names


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def _catalog_widget():
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


CATALOG = _catalog_widget()
GOLD_TABLE = f"{CATALOG}.gold.investigation_context"


def fail_if_rows(name, df):
    if df.limit(1).count():
        print(f"FAIL: {name}")
        df.show(20, truncate=False)
        raise Exception(f"Validation failed: {name}")
    print(f"PASS: {name}")


def recursive_field_names(schema, prefix=""):
    names = []
    for field in schema.fields:
        path = f"{prefix}.{field.name}" if prefix else field.name
        names.append(path)
        data_type = field.dataType
        if hasattr(data_type, "fields"):
            names.extend(recursive_field_names(data_type, path))
        elif hasattr(data_type, "elementType") and hasattr(data_type.elementType, "fields"):
            names.extend(recursive_field_names(data_type.elementType, path))
    return names


print("=== M3 Gold validation ===")
if not spark.catalog.tableExists(GOLD_TABLE):
    raise Exception(f"Validation failed: required table does not exist: {GOLD_TABLE}")

gold = spark.read.table(GOLD_TABLE)
if gold.limit(1).count() == 0:
    raise Exception("Validation failed: Gold table is empty")
print(f"PASS: Gold table exists and has {gold.count()} rows")

required_types = {
    "case_id": "string",
    "context_category": "string",
    "case_context": "struct",
    "case_summary": "string",
    "linked_transactions": "array",
    "payment_instrument_context": "array",
    "merchant_context": "array",
    "authorization_context": "array",
    "dispute_context": "array",
    "fraud_alerts": "array",
    "party_context": "array",
    "safe_notes": "array",
    "quality_status": "string",
    "masking_status": "string",
    "warning_flags": "array",
    "source_references": "array",
    "usage_restrictions": "string",
    "pipeline_run_id": "string",
    "context_version": "string",
    "last_refreshed_at": "timestamp",
}
actual_types = {field.name: field.dataType.typeName() for field in gold.schema.fields}
missing_or_wrong = {
    field: expected
    for field, expected in required_types.items()
    if actual_types.get(field) != expected
}
if missing_or_wrong:
    raise Exception(f"Validation failed: missing or wrong Gold column types: {missing_or_wrong}")
print("PASS: required top-level Gold columns and types are present")

fail_if_rows("case_id is non-null", gold.where(F.col("case_id").isNull()))
fail_if_rows(
    "case_id is unique",
    gold.groupBy("case_id").count().where(F.col("count") != 1),
)
fail_if_rows(
    "approved quality and masking status values",
    gold.where(
        ~F.col("quality_status").isin("pass", "partial")
        | (F.col("masking_status") != "masked")
    ),
)
fail_if_rows(
    "approved category, restriction, and version values",
    gold.where(
        (F.col("context_category") != "transaction_investigation")
        | (F.col("usage_restrictions") != "internal_only")
        | (F.col("context_version") != "1.0.0")
    ),
)
fail_if_rows(
    "source references are non-empty and fully identified",
    gold.where(F.size("source_references") == 0),
)
fail_if_rows(
    "source references have non-null table and record identifiers",
    gold.select("case_id", F.explode("source_references").alias("source")).where(
        F.col("source.source_table").isNull()
        | (F.trim("source.source_table") == "")
        | F.col("source.source_record_id").isNull()
        | (F.trim("source.source_record_id") == "")
    ),
)

forbidden = forbidden_field_names(recursive_field_names(gold.schema))
if forbidden:
    raise Exception(f"Validation failed: forbidden Gold field names: {forbidden}")
print("PASS: Gold schema does not expose forbidden identity, identifier, device, or PAN fields")

silver_cases = spark.read.table(f"{CATALOG}.silver.investigation_cases")
bronze_cases = spark.read.table(f"{CATALOG}.bronze.investigation_cases")
latest_bronze_batch = bronze_cases.agg(F.max("_batch_id").alias("_batch_id")).first()["_batch_id"]
fail_if_rows(
    "no Gold case is legal hold in Silver",
    gold.join(silver_cases.where(F.col("legal_hold") == F.lit(True)).select("case_id"), "case_id"),
)
fail_if_rows(
    "no Gold case is legal hold in Bronze",
    gold.join(
        bronze_cases.where(
            (F.col("_batch_id") == latest_bronze_batch)
            & (F.lower(F.trim("legal_hold")) == "true")
        ).select("case_id"),
        "case_id",
    ),
)

silver_run_ids = [row["_run_id"] for row in silver_cases.select("_run_id").distinct().limit(2).collect()]
if len(silver_run_ids) != 1:
    raise Exception(f"Validation failed: Silver cases do not have one current run: {silver_run_ids}")
fail_if_rows(
    "Gold pipeline_run_id matches the Silver snapshot",
    gold.where(F.col("pipeline_run_id") != F.lit(silver_run_ids[0])),
)

content = gold.select(F.to_json(F.struct(*gold.columns)).alias("document"))
leakage_pattern = r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})|(\+\d{6,15})|(\b\d{3}-\d{2}-\d{4}\b)|(\b\d{13,19}\b)|(\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b)"
fail_if_rows("no email, phone, tax-ID, or PAN-shaped content leaks", content.where(F.col("document").rlike(leakage_pattern)))

manifest = spark.read.table(f"{CATALOG}.bronze.defects_manifest").where(F.col("_batch_id") == latest_bronze_batch)
legal_hold_fixtures = manifest.where(F.col("rule_id") == "DQ-CASE-LEGALHOLD").select(F.col("record_key").alias("case_id"))
fail_if_rows("legal-hold manifest fixtures are absent from Gold", gold.join(legal_hold_fixtures, "case_id"))
pii_note_fixtures = manifest.where(F.col("rule_id") == "DQ-NOTE-PII-LEAK").select(F.col("record_key").alias("note_id"))
fail_if_rows(
    "quarantined PII-note manifest fixtures are absent from Gold",
    gold.select(F.explode("safe_notes").alias("note"))
    .select(F.col("note.note_id").alias("note_id"))
    .join(pii_note_fixtures, "note_id"),
)

# Idempotency evidence is available after a second overwrite from unchanged
# Silver inputs. The first build has no comparable Delta version, so it is a
# non-blocking informational result rather than a false failure.
history = spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}").select("version").orderBy(F.desc("version")).limit(2).collect()
if len(history) == 2:
    current = spark.read.table(GOLD_TABLE).drop("last_refreshed_at")
    previous = spark.sql(f"SELECT * FROM {GOLD_TABLE} VERSION AS OF {history[1]['version']}").drop("last_refreshed_at")
    current_runs = [row["pipeline_run_id"] for row in current.select("pipeline_run_id").distinct().limit(2).collect()]
    previous_runs = [row["pipeline_run_id"] for row in previous.select("pipeline_run_id").distinct().limit(2).collect()]
    if current_runs == previous_runs:
        current_hashes = current.select(F.sha2(F.to_json(F.struct(*current.columns)), 256).alias("hash"))
        previous_hashes = previous.select(F.sha2(F.to_json(F.struct(*previous.columns)), 256).alias("hash"))
        fail_if_rows("unchanged-input rebuild preserves Gold business content", current_hashes.exceptAll(previous_hashes).unionByName(previous_hashes.exceptAll(current_hashes)))
    else:
        print("INFO: previous Gold version used a different Silver run; idempotency comparison skipped")
else:
    print("INFO: idempotency comparison will run after a second Gold overwrite")

print("PASS: M3 Gold validation completed with no blocking failures.")
