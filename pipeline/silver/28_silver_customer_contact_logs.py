# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: customer_contact_logs
# ============================================================================
# Implements Bronze -> Silver transformation for the customer_contact_logs dataset:
#   1. Reads from bronze.customer_contact_logs
#   2. Enforces Data Quality (DQ) rules and identifies failures
#   3. Quarantines failed records to silver.quarantine_records
#   4. Writes clean records to silver.customer_contact_logs
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import deduplicate_quarantine_rows, latest_batch_snapshot, snapshot_run_id

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
TABLE_NAME = "customer_contact_logs"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))
RUN_ID = snapshot_run_id(df)

print("Loading Silver customer and employee references...")
customers_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customers") \
    .select("customer_id").distinct()
employees_df = spark.read.table(f"{CATALOG}.{SCHEMA}.employees") \
    .select("employee_id").distinct()
df_joined = (
    df.join(
        customers_df.withColumn("customer_exists", F.lit(True)),
        on="customer_id",
        how="left",
    )
    .join(
        employees_df.withColumn("employee_exists", F.lit(True)),
        on="employee_id",
        how="left",
    )
)

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# DQ conditions
is_dnc_violation = (F.col("direction") == "outbound") & (F.col("do_not_contact") == "true")

# Regex pattern matching email, phone number (+d{6,15}), card/PAN (13-19 digits, or 4 blocks of 4 digits)
regex_pattern = r'([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})|(\+\d{6,15})|(\b\d{13,19}\b)|(\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b)'
is_pii_leak = F.col("note").rlike(regex_pattern)
is_missing_customer = F.col("customer_id").isNull() | F.col("customer_exists").isNull()
is_missing_employee = F.col("employee_id").isNull() | F.col("employee_exists").isNull()

# DQ failure expressions aligning with gov.dq_rules / dq_failures SQL
rule_id_expr = F.when(is_dnc_violation, "DQ-CTL-DNC-VIOLATION") \
                .when(is_pii_leak, "DQ-CTL-NOTE-PII") \
                .when(is_missing_customer, "DQ-CTL-CUST-FK") \
                .when(is_missing_employee, "DQ-CTL-EMP-FK")

rule_name_expr = F.when(is_dnc_violation, "no outbound contact when do_not_contact=true") \
                  .when(is_pii_leak, "note must not contain raw PII/PAN") \
                  .when(is_missing_customer, "customer_id must exist in Silver customers") \
                  .when(is_missing_employee, "employee_id must exist in Silver employees")

failure_reason_expr = F.when(is_dnc_violation, F.lit("DNC business-rule break")) \
                      .when(is_pii_leak, F.lit("leaked PII in contact note")) \
                      .when(is_missing_customer, F.lit("customer_id does not resolve to Silver customers")) \
                      .when(is_missing_employee, F.lit("employee_id does not resolve to Silver employees"))

# Filter out failed records for quarantine
failed_df = df_joined.filter(is_dnc_violation | is_pii_leak | is_missing_customer | is_missing_employee)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("customer_contact_logs").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.col("contact_id").alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "contact_id", "customer_id", "direction", "contact_method", "do_not_contact", "contacted_at", "employee_id", "note"
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
    print(f"Cleaning prior quarantine records for customer_contact_logs under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'customer_contact_logs' AND run_id = '{RUN_ID}'
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
silver_customer_contact_logs_df = clean_df.select(
    F.col("contact_id"),
    F.col("customer_id"),
    F.col("direction"),
    F.col("contact_method"),
    F.col("do_not_contact").cast("boolean").alias("do_not_contact"),
    F.col("contacted_at").cast("timestamp").alias("contacted_at"),
    F.col("employee_id"),
    F.col("note"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CUSTOMER_CONTACT_LOGS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_customer_contact_logs_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Customer Contact Logs:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
