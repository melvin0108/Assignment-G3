# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: customers
# ============================================================================
# Implements Bronze -> Silver transformation for the customers dataset:
#   1. Reads from bronze.customers
#   2. Enforces Data Quality (DQ) rules and identifies failures
#   3. Quarantines failed records to silver.quarantine_records
#   4. Applies PII masking (FPE/tokenization, hashing, age-banding, email/phone masking)
#   5. Writes clean records to silver.customers
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.sql.window import Window
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import deduplicate_quarantine_rows, latest_batch_snapshot, snapshot_run_id
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
TABLE_NAME = "customers"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
# Load raw conformed data from Bronze
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))
CAST_RULES = [
    TypeCastRule("dob", "dob_typed", "DATE", "DQ-CUST-DOB-TYPE"),
    TypeCastRule("created_at", "created_at_typed", "TIMESTAMP", "DQ-CUST-CREATED-TYPE"),
]
df = apply_type_casts(df, CAST_RULES)
RUN_ID = snapshot_run_id(df)

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Window functions for duplication checks:
# - rn_pk: Detects exact customer_id duplicates (keeps first by created_at & ingest_ts)
pk_window = Window.partitionBy("customer_id").orderBy("created_at", "_ingest_ts")

# - rn_near: Detects near-duplicates based on names, DOB, address, and tax ID
near_dup_window = Window.partitionBy("first_name", "last_name", "dob", "address", "tax_id") \
    .orderBy("customer_id", "_ingest_ts")

# Rank the records to detect duplicates
df_ranked = df \
    .withColumn("rn_pk", F.row_number().over(pk_window)) \
    .withColumn("rn_near", F.row_number().over(near_dup_window))

# Regex pattern for email format validation
email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+$"
is_invalid_email = (F.col("email").isNotNull()) & (F.col("email") != "") & (~F.col("email").rlike(email_pattern))

# DQ failure expressions aligning with gov.dq_rules
rule_id_expr = F.when(F.col("rn_pk") > 1, "DQ-CUST-ID-DUP") \
    .when(is_invalid_email, "DQ-CUST-EMAIL-FMT") \
    .when(F.col("rn_near") > 1, "DQ-CUST-NEAR-DUP")

rule_name_expr = F.when(F.col("rn_pk") > 1, "customer_id must be unique") \
    .when(is_invalid_email, "email must match pattern if present") \
    .when(F.col("rn_near") > 1, "no two customers share name+dob+address+tax_id")

failure_reason_expr = F.when(F.col("rn_pk") > 1, F.concat(F.lit("Duplicate customer_id found: "), F.col("customer_id"))) \
    .when(is_invalid_email, F.concat(F.lit("Invalid email format: "), F.col("email"))) \
    .when(F.col("rn_near") > 1,
          F.concat(F.lit("Near duplicate customer found with same details. tax_id: "), F.col("tax_id")))

# Filter out failed records for quarantine
failed_df = df_ranked.filter(
    (F.col("rn_pk") > 1) |
    is_invalid_email |
    (F.col("rn_near") > 1)
)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("customers").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("customer_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "customer_id", "first_name", "last_name", "dob", "email", "phone", "address", "tax_id", "created_at"
    )).alias("raw_record"),
    F.current_timestamp().alias("detected_at")
)
quarantine_df = quarantine_df.unionByName(type_cast_quarantine_rows(
    df_ranked, CAST_RULES, TABLE_NAME, "customer_id", RUN_ID
))
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

# ---------------------------------------------------------------------------
# 3. WRITE TO QUARANTINE SINK
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Idempotent write: clean up this run's prior quarantine rows first
try:
    print(f"Cleaning prior quarantine records for customers under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'customers' AND run_id = '{RUN_ID}'
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
# 4. FILTER CLEAN RECORDS & APPLY PII MASKING
# ---------------------------------------------------------------------------
# Get clean records using a left anti-join on _source_record_id
clean_df = df_ranked.join(
    failed_df,
    on="_source_record_id",
    how="left_anti"
).filter(~any_cast_failure(CAST_RULES))

# PII Masking/Hashing parameters
salt = "NAB_SALT_2026"
run_date_lit = F.lit("2026-07-06").cast("date")  # Pinned run date for age bands

# DOB Age-banding expression
age_expr = F.floor(F.months_between(run_date_lit, F.col("dob_typed")) / 12)
dob_masked = F.when(F.col("dob").isNull() | (F.col("dob") == ""), "UNKNOWN") \
    .otherwise(
    F.when(age_expr < 18, "Under 18") \
        .when(age_expr.between(18, 25), "18-25") \
        .when(age_expr.between(26, 35), "26-35") \
        .when(age_expr.between(36, 45), "36-45") \
        .when(age_expr.between(46, 55), "46-55") \
        .when(age_expr.between(56, 65), "56-65") \
        .otherwise("66+")
)

# Email masking logic: show first char + mask user + mask domain name (j***@***.com)
email_split = F.split(F.col("email"), "@")
email_user = email_split.getItem(0)
email_domain = email_split.getItem(1)
masked_domain = F.regexp_replace(email_domain, "^[^.]+", "***")

email_masked = F.when(F.col("email").isNull() | (F.col("email") == ""), F.lit(None).cast("string")) \
    .when(~F.col("email").contains("@"), "invalid_masked_email") \
    .otherwise(
    F.concat(
        F.substring(email_user, 1, 1),
        F.lit("***@"),
        masked_domain
    )
)

# Phone masking logic: ******1234
phone_masked = F.when(F.col("phone").isNull() | (F.col("phone") == ""), F.lit(None).cast("string")) \
    .otherwise(F.concat(F.lit("******"), F.substring(F.col("phone"), -4, 4)))

# Construct Silver DataFrame
silver_customers_df = clean_df.select(
    F.col("customer_id"),
    F.concat(
        F.lit("TOK_"),
        F.substring(F.sha2(F.concat(F.lower(F.trim(F.col("first_name"))), F.lit(salt)), 256), 1, 16)
    ).alias("first_name"),
    F.concat(
        F.lit("TOK_"),
        F.substring(F.sha2(F.concat(F.lower(F.trim(F.col("last_name"))), F.lit(salt)), 256), 1, 16)
    ).alias("last_name"),
    dob_masked.alias("dob"),
    email_masked.alias("email"),
    phone_masked.alias("phone"),
    F.when(F.col("address").isNull() | (F.col("address") == ""), F.lit(None).cast("string")) \
        .otherwise(F.sha2(F.concat(F.lower(F.trim(F.col("address"))), F.lit(salt)), 256)).alias("address"),
    F.when(F.col("tax_id").isNull() | (F.col("tax_id") == ""), F.lit(None).cast("string")) \
        .otherwise(F.sha2(F.concat(F.lower(F.trim(F.col("tax_id"))), F.lit(salt)), 256)).alias("tax_id"),
    F.col("created_at_typed").alias("created_at"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CUSTOMERS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Customers:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
