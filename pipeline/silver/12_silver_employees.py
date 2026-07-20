# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: employees
# ============================================================================
# Implements Bronze -> Silver transformation for the employees dataset:
#   1. Reads from bronze.employees
#   2. Enforces Data Quality (DQ) rules and identifies failures
#   3. Quarantines failed records to silver.quarantine_records
#   4. Applies PII masking (hashing full_name and email)
#   5. Writes clean records to silver.employees
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.sql.window import Window
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import (
    deduplicate_quarantine_rows, exclude_dq_quarantined_rows,
    latest_batch_snapshot, snapshot_run_id,
)

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
TABLE_NAME = "employees"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
# Load raw conformed data from Bronze
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))
RUN_ID = snapshot_run_id(df)

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Window functions for uniqueness checks. Use the same physical-row ordering as
# the authoritative Bronze DQ queries.
# - rn_email: Detects duplicate email.
email_window = Window.partitionBy("email").orderBy(F.col("_source_record_id").asc())

# - rn_name: Detects duplicate names.
name_window = Window.partitionBy("full_name").orderBy(F.col("_source_record_id").asc())

# Rank the records to detect duplicates
df_ranked = df \
    .withColumn("rn_email", F.row_number().over(email_window)) \
    .withColumn("rn_name", F.row_number().over(name_window))

# DQ failure expressions aligning with gov.dq_rules
rule_id_expr = F.when(F.col("rn_email") > 1, "DQ-EMP-EMAIL-UNIQ") \
    .when(F.col("rn_name") > 1, "DQ-EMP-NAME-NEAR-DUP")

rule_name_expr = F.when(F.col("rn_email") > 1, "email must be unique") \
    .when(F.col("rn_name") > 1, "flag near-duplicate employee names")

failure_reason_expr = F.when(F.col("rn_email") > 1, F.concat(F.lit("Duplicate email found: "), F.col("email"))) \
    .when(F.col("rn_name") > 1, F.concat(F.lit("Duplicate employee name found: "), F.col("full_name")))

# Filter out failed records for quarantine
failed_df = df_ranked.filter(
    (F.col("rn_email") > 1) |
    (F.col("rn_name") > 1)
)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("employees").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("employee_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "employee_id", "full_name", "email", "team", "role"
    )).alias("raw_record"),
    F.current_timestamp().alias("detected_at")
)
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

# ---------------------------------------------------------------------------
# 3. WRITE TO QUARANTINE SINK
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Idempotent write: clean up this run's prior quarantine rows first
try:
    print(f"Cleaning prior quarantine records for employees under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'employees' AND run_id = '{RUN_ID}'
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

# PII Masking/Hashing parameters
salt = "NAB_SALT_2026"

# Construct Silver DataFrame
silver_employees_df = clean_df.select(
    F.col("employee_id"),
    F.sha2(F.concat(F.lower(F.trim(F.col("full_name"))), F.lit(salt)), 256).alias("full_name"),
    F.sha2(F.concat(F.lower(F.trim(F.col("email"))), F.lit(salt)), 256).alias("email"),
    F.col("team"),
    F.col("role"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)
silver_employees_df = exclude_dq_quarantined_rows(
    silver_employees_df, spark, CATALOG, TABLE_NAME, RUN_ID
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER EMPLOYEES TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_employees_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Employees:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
