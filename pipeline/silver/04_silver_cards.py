# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: cards
# ============================================================================
# Implements Bronze -> Silver transformation for the cards dataset:
#   1. Reads from bronze.cards
#   2. Enforces Data Quality (DQ) rules and identifies failures
#   3. Quarantines failed records to silver.quarantine_records
#   4. Applies PII masking (masks PAN showing only the last 4 digits)
#   5. Writes clean records to silver.cards
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.sql.window import Window

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CATALOG = "g3_dev"
SCHEMA = "silver"
TABLE_NAME = "cards"
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

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Window function for card_id duplicates:
# - rn_pk: Detects exact card_id duplicates (keeps first by _ingest_ts)
pk_window = Window.partitionBy("card_id").orderBy("_ingest_ts")

# Rank the records to detect duplicates
df_ranked = df.withColumn("rn_pk", F.row_number().over(pk_window))

# Expired but active check
is_expired_active = (
        (F.col("status") == "active") &
        (F.col("expiry").isNotNull()) &
        (F.col("expiry") != "") &
        (F.to_date(F.concat(F.col("expiry"), F.lit("-01")), "yyyy-MM-dd") < F.to_date(F.lit("2026-07-01"),
                                                                                      "yyyy-MM-dd"))
)

# DQ failure expressions aligning with gov.dq_rules
rule_id_expr = F.when(F.col("rn_pk") > 1, "DQ-CARD-DUP") \
    .when(is_expired_active, "DQ-CARD-EXPIRED-ACTIVE")

rule_name_expr = F.when(F.col("rn_pk") > 1, "card_id must be unique") \
    .when(is_expired_active, "active card must not have a past expiry")

failure_reason_expr = F.when(F.col("rn_pk") > 1, F.concat(F.lit("Duplicate card_id found: "), F.col("card_id"))) \
    .when(is_expired_active, F.concat(F.lit("Card active but expired. expiry: "), F.col("expiry")))

# Filter out failed records for quarantine
failed_df = df_ranked.filter(
    (F.col("rn_pk") > 1) |
    is_expired_active
)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("cards").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("card_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "card_id", "account_id", "card_type", "pan", "expiry", "status"
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
    print(f"Cleaning prior quarantine records for cards under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'cards' AND run_id = '{RUN_ID}'
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
# 4. FILTER CLEAN RECORDS & APPLY PII MASKING
# ---------------------------------------------------------------------------
# Get clean records using a left anti-join on _source_record_id
clean_df = df_ranked.join(
    failed_df,
    on="_source_record_id",
    how="left_anti"
)

# PII Masking logic for PAN: XXXX-XXXX-XXXX-1234 (keeping only last 4 digits)
pan_masked = F.concat(F.lit("XXXX-XXXX-XXXX-"), F.substring(F.col("pan"), -4, 4))

# Construct Silver DataFrame
silver_cards_df = clean_df.select(
    F.col("card_id"),
    F.col("account_id"),
    F.col("card_type"),
    pan_masked.alias("pan"),
    F.col("expiry"),
    F.col("status"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CARDS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_cards_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Cards:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
