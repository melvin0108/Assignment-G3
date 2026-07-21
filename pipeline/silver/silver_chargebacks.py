# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: chargebacks
# ============================================================================
# Implements Bronze -> Silver transformation for the chargebacks dataset:
#   1. Reads from bronze.chargebacks
#   2. Performs referential integrity checks against disputes
#   3. Enforces Data Quality (DQ) rules and identifies failures
#   4. Quarantines failed records to silver.quarantine_records
#   5. Writes clean records to silver.chargebacks
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import (
    deduplicate_quarantine_rows, exclude_dq_quarantined_rows,
    latest_batch_snapshot, snapshot_run_id,
)
from pipeline.silver.type_cast import TypeCastRule, any_cast_failure, apply_type_casts, type_cast_quarantine_rows

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
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
SCHEMA = "silver"
TABLE_NAME = "chargebacks"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA & REFERENCES
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))
CAST_RULES = [
    TypeCastRule("amount", "amount_typed", "DOUBLE", "DQ-CBK-AMOUNT-TYPE"),
    TypeCastRule("processed_at", "processed_at_typed", "TIMESTAMP", "DQ-CBK-PROCESSED-TYPE"),
]
df = apply_type_casts(df, CAST_RULES)
RUN_ID = snapshot_run_id(df)

# Load disputes for referential integrity (FK) check.
# Try silver first, fall back to bronze if silver is not available yet.
print("Loading disputes reference table...")
try:
    disputes_df = spark.read.table(f"{CATALOG}.{SCHEMA}.disputes").select("dispute_id").distinct()
    print("Using silver.disputes for FK validation.")
except Exception as e:
    print(f"Silver disputes not found, falling back to bronze: {e}")
    disputes_df = spark.read.table(f"{CATALOG}.bronze.disputes").select("dispute_id").distinct()

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Left join with disputes to check if dispute_id exists
df_joined = df.join(
    disputes_df.withColumn("disp_exists", F.lit(True)),
    on="dispute_id",
    how="left"
)

# DQ conditions
is_missing_fk = F.col("dispute_id").isNotNull() & F.col("disp_exists").isNull()

# DQ failure expressions aligning with gov.dq_rules / dq_failures SQL
rule_id_expr = F.when(is_missing_fk, "DQ-CBK-DISP-FK")
rule_name_expr = F.when(is_missing_fk, "dispute_id must exist in disputes")
failure_reason_expr = F.when(is_missing_fk, F.lit("orphan dispute_id"))

# Filter out failed records for quarantine
failed_df = df_joined.filter(is_missing_fk)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("chargebacks").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("chargeback_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "chargeback_id", "dispute_id", "stage"
    )).alias("raw_record"),
    F.current_timestamp().alias("detected_at")
)
quarantine_df = quarantine_df.unionByName(type_cast_quarantine_rows(
    df_joined, CAST_RULES, TABLE_NAME, "chargeback_id", RUN_ID
))
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

# ---------------------------------------------------------------------------
# 3. WRITE TO QUARANTINE SINK
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Idempotent write: clean up this run's prior quarantine rows first
try:
    print(f"Cleaning prior quarantine records for chargebacks under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'chargebacks' AND run_id = '{RUN_ID}'
    """)
except Exception as e:
    print(f"Quarantine delete skipped (table might not exist yet): {e}")

# Append new failures to quarantine
if not quarantine_df.isEmpty():
    print(f"Writing {quarantine_df.count()} failed records to quarantine...")
    quarantine_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(QUARANTINE_TABLE_NAME)
else:
    print("No failed records found to quarantine.")

# ---------------------------------------------------------------------------
# 4. FILTER CLEAN RECORDS
# ---------------------------------------------------------------------------
# Get clean records using a left anti-join on _source_record_id
clean_df = df_joined.join(
    failed_df,
    on="_source_record_id",
    how="left_anti"
).filter(~any_cast_failure(CAST_RULES))

# Construct Silver DataFrame
silver_chargebacks_df = clean_df.select(
    F.col("chargeback_id"),
    F.col("dispute_id"),
    F.col("scheme"),
    F.col("amount_typed").alias("amount"),
    F.col("stage"),
    F.col("processed_at_typed").alias("processed_at"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)
silver_chargebacks_df = exclude_dq_quarantined_rows(
    silver_chargebacks_df, spark, CATALOG, TABLE_NAME, RUN_ID
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CHARGEBACKS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_chargebacks_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Chargebacks:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
