# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: branches
# ============================================================================
# Implements Bronze -> Silver transformation for the branches dataset:
#   1. Reads from bronze.branches
#   2. Performs basic typecasting and validation
#   3. Writes clean records to silver.branches
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
TABLE_NAME = "branches"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
RUN_ID = "RUN-20260706-1"  # Run ID used to track this execution batch

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = spark.read.table(BRONZE_TABLE_NAME)

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Branches are a clean reference lookup table, so there are no failures.
# We will write the structure to quarantine but do not filter any records.
failed_df = spark.createDataFrame([], df.schema)

# ---------------------------------------------------------------------------
# 3. FILTER CLEAN RECORDS
# ---------------------------------------------------------------------------
# Construct Silver DataFrame
silver_branches_df = df.select(
    F.col("branch_code"),
    F.col("name"),
    F.col("country"),
    F.col("region"),
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
# 4. WRITE CLEAN SILVER BRANCHES TABLE
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_branches_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 5. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Branches:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
