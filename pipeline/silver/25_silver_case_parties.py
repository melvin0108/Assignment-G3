# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: case_parties
# ============================================================================
# Implements Bronze -> Silver transformation for the case_parties dataset:
#   1. Reads from bronze.case_parties
#   2. Performs conditional referential integrity checks (by party_type)
#   3. Enforces Data Quality (DQ) rules and identifies failures
#   4. Quarantines failed records to silver.quarantine_records
#   5. Writes clean records to silver.case_parties
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
TABLE_NAME = "case_parties"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
RUN_ID = "RUN-20260706-1"  # Run ID used to track this execution batch

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA & REFERENCES
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))

# Load customers for referential integrity (FK) check.
# Try silver first, fall back to bronze if silver is not available yet.
print("Loading customers reference table...")
try:
    customers_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customers").select("customer_id").distinct()
    print("Using silver.customers for FK validation.")
except Exception as e:
    print(f"Silver customers not found, falling back to bronze: {e}")
    customers_df = spark.read.table(f"{CATALOG}.bronze.customers").select("customer_id").distinct()

# Load merchants for referential integrity (FK) check.
# Try silver first, fall back to bronze if silver is not available yet.
print("Loading merchants reference table...")
try:
    merchants_df = spark.read.table(f"{CATALOG}.{SCHEMA}.merchants").select("merchant_id").distinct()
    print("Using silver.merchants for FK validation.")
except Exception as e:
    print(f"Silver merchants not found, falling back to bronze: {e}")
    merchants_df = spark.read.table(f"{CATALOG}.bronze.merchants").select("merchant_id").distinct()

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
# Left join with customers to check if customer party exists
df_joined = df.join(
    customers_df.withColumn("cust_exists", F.lit(True)),
    on=(df.party_type == "customer") & (df.party_id == customers_df.customer_id),
    how="left"
)

# Left join with merchants to check if merchant party exists
df_joined = df_joined.join(
    merchants_df.withColumn("merch_exists", F.lit(True)),
    on=(df_joined.party_type == "merchant") & (df_joined.party_id == merchants_df.merchant_id),
    how="left"
)

# DQ conditions
is_invalid_type = ~F.col("party_type").isin("customer", "merchant", "third_party")
is_unresolved = (
    ((F.col("party_type") == "customer") & F.col("cust_exists").isNull()) |
    ((F.col("party_type") == "merchant") & F.col("merch_exists").isNull())
)

# DQ failure expressions aligning with gov.dq_rules / dq_failures SQL
rule_id_expr = F.when(is_invalid_type, "DQ-CASEPARTY-TYPE-ENUM") \
                .when(is_unresolved, "DQ-CASEPARTY-RESOLVE")

rule_name_expr = F.when(is_invalid_type, "party_type must be in {customer,merchant,third_party}") \
                  .when(is_unresolved, "party_id must resolve per party_type")

failure_reason_expr = F.when(is_invalid_type, F.lit("invalid party_type")) \
                      .when(is_unresolved, F.lit("unresolvable party_id for party_type"))

# Filter out failed records for quarantine
failed_df = df_joined.filter(is_invalid_type | is_unresolved)

# Structure the quarantined DataFrame matching silver.quarantine_records schema
# Note the composite record_key: case_id|party_type|party_id
quarantine_df = failed_df.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("case_parties").alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"),
    F.concat_ws("|", F.col("case_id"), F.col("party_type"), F.col("party_id")).alias("record_key"),
    rule_id_expr.alias("rule_id"),
    rule_name_expr.alias("rule_name"),
    failure_reason_expr.alias("failure_reason"),
    F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct(
        "case_id", "party_type", "party_id", "role"
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
    print(f"Cleaning prior quarantine records for case_parties under run {RUN_ID}...")
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE_NAME} 
        WHERE source_table = 'case_parties' AND run_id = '{RUN_ID}'
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
silver_case_parties_df = clean_df.select(
    F.col("case_id"),
    F.col("party_type"),
    F.col("party_id"),
    F.col("role"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER CASE_PARTIES TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_case_parties_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Case Parties:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
