# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: accounts
# ============================================================================
# Implements Bronze -> Silver transformation for the accounts dataset:
#   1. Reads from bronze.accounts
#   2. Performs referential integrity checks against silver.customers
#   3. Enforces Data Quality (DQ) rules and identifies failures
#   4. Quarantines failed records to silver.quarantine_records
#   5. Writes clean records to silver.accounts
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CATALOG = "g3_dev"
SCHEMA = "silver"
TABLE_NAME = "accounts"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
RUN_ID = "RUN-20260706-1"  # Run ID used to track this execution batch

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
# Load raw conformed data from Bronze
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = spark.read.table(BRONZE_TABLE_NAME)

# Load clean customers from Silver to perform referential integrity (FK) check
print(f"Reading from Silver customers for FK validation...")
customers_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customers").select("customer_id").distinct()

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Left join with customers to check if customer_id exists
df_joined = df.join(
    customers_df.withColumn("cust_exists", F.lit(True)),
    on="customer_id",
    how="left"
)

# DQ conditions
is_future_date = (F.col("open_date").isNotNull()) & (F.col("open_date") != "") & (
            F.col("open_date").cast("date") > F.lit("2026-07-06").cast("date"))
is_missing_fk = F.col("cust_exists").isNull()

# DQ failure expressions aligning with gov.dq_rules
rule_id_expr = F.when(is_future_date, "DQ-ACC-OPENDATE-FUTURE") \
    .when(is_missing_fk, "DQ-ACC-CUST-FK")

rule_name_expr = F.when(is_future_date, "open_date must not be in the future") \
    .when(is_missing_fk, "customer_id must exist in customers")

failure_reason_expr = F.when(is_future_date, F.concat(F.lit("open_date in the future: "), F.col("open_date"))) \
    .when(is_missing_fk, F.concat(F.lit("Referential integrity break: customer_id "), F.col("customer_id"),
                                  F.lit(" not found in silver.customers")))

# Filter out failed records for quarantine
failed_df = df_joined.filter(is_future_date | is_missing_fk)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("accounts").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("account_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "account_id", "customer_id", "product_type", "open_date", "status", "currency"
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
    print(f"Cleaning prior quarantine records for accounts under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'accounts' AND run_id = '{RUN_ID}'
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
silver_accounts_df = clean_df.select(
    F.col("account_id"),
    F.col("customer_id"),
    F.col("product_type"),
    F.col("open_date").cast("date").alias("open_date"),
    F.col("status"),
    F.col("currency"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER ACCOUNTS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_accounts_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Accounts:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
