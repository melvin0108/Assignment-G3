# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: case_transactions
# ============================================================================
# Implements Bronze -> Silver transformation for the case_transactions dataset:
#   1. Reads from bronze.case_transactions
#   2. Performs referential integrity checks against transactions
#   3. Enforces Data Quality (DQ) rules and identifies failures
#   4. Quarantines failed records to silver.quarantine_records
#   5. Writes clean records to silver.case_transactions
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import latest_batch_snapshot

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
TABLE_NAME = "case_transactions"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
RUN_ID = "RUN-20260706-1"  # Run ID used to track this execution batch

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA & REFERENCES
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))

# Load transactions for referential integrity (FK) check.
# Try silver first, fall back to bronze if silver is not available yet.
print("Loading transactions reference table...")
try:
    tx_df = spark.read.table(f"{CATALOG}.{SCHEMA}.transactions").select("transaction_id").distinct()
    print("Using silver.transactions for FK validation.")
except Exception as e:
    print(f"Silver transactions not found, falling back to bronze: {e}")
    tx_df = spark.read.table(f"{CATALOG}.bronze.transactions").select("transaction_id").distinct()

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Left join with transactions to check if transaction_id exists
df_joined = df.join(
    tx_df.withColumn("tx_exists", F.lit(True)),
    on="transaction_id",
    how="left"
)

# DQ conditions
is_missing_fk = F.col("transaction_id").isNotNull() & F.col("tx_exists").isNull()

# DQ failure expressions aligning with gov.dq_rules / dq_failures SQL
rule_id_expr = F.when(is_missing_fk, "DQ-CASETXN-TXN-FK")
rule_name_expr = F.when(is_missing_fk, "transaction_id must exist in transactions")
failure_reason_expr = F.when(is_missing_fk, F.lit("orphan transaction_id"))

# Filter out failed records for quarantine
failed_df = df_joined.filter(is_missing_fk)

# Structure the quarantined DataFrame matching silver.quarantine_records schema.
# Note the composite record_key: case_id|transaction_id
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("case_transactions").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.concat_ws("|", F.col("case_id"), F.col("transaction_id")).alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "case_id", "transaction_id"
    )).alias("raw_record"),
    F.current_timestamp().alias("detected_at")
)

# ---------------------------------------------------------------------------
# 3. WRITE TO QUARANTINE SINK
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Idempotent write: clean up this run's prior quarantine rows first
try:
    print(f"Cleaning prior quarantine records for case_transactions under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'case_transactions' AND run_id = '{RUN_ID}'
    """)
except Exception as e:
    print(f"Quarantine delete skipped (table might not exist yet): {e}")

# Append new failures to quarantine
if failed_df.count() > 0:
    print(f"Writing {failed_df.count()} failed records to quarantine...")
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
)

# Construct Silver DataFrame
silver_case_transactions_df = clean_df.select(
    F.col("case_id"),
    F.col("transaction_id"),
    F.col("linked_at").cast("timestamp").alias("linked_at"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CASE_TRANSACTIONS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_case_transactions_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Case Transactions:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
