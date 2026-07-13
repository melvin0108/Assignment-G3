# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: auth_attempts
# ============================================================================
# Implements Bronze -> Silver transformation for authorization attempts:
#   1. Reads the latest bronze.auth_attempts snapshot
#   2. Checks transaction FK and timestamp ordering
#   3. Quarantines failed records to silver.quarantine_records
#   4. Writes clean records to silver.auth_attempts
#   5. Appends lineage rows to gov.metadata_lineage
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CATALOG = "g3_dev"
SCHEMA = "silver"
TABLE_NAME = "auth_attempts"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
TRANSACTIONS_TABLE_NAME = f"{CATALOG}.{SCHEMA}.transactions"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
LINEAGE_TABLE_NAME = f"{CATALOG}.gov.metadata_lineage"
RUN_ID = "RUN-20260706-1"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE AND REFERENCE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
auth_attempts_all_df = spark.read.table(BRONZE_TABLE_NAME)
latest_batch_id = auth_attempts_all_df.select(F.max("_batch_id").alias("batch_id")).first()["batch_id"]
print(f"Using latest auth_attempts batch: {latest_batch_id}")
auth_attempts_df = auth_attempts_all_df.filter(F.col("_batch_id") == latest_batch_id).alias("a")

print(f"Reading Silver transactions for FK validation: {TRANSACTIONS_TABLE_NAME}")
transactions_df = spark.read.table(TRANSACTIONS_TABLE_NAME).select("transaction_id", "txn_ts").alias("t")

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
checked_df = (
    auth_attempts_df
    .withColumn("auth_ts_typed", F.expr("try_to_timestamp(replace(replace(auth_ts, 'T', ' '), 'Z', ''))"))
    .join(transactions_df, F.col("a.transaction_id") == F.col("t.transaction_id"), "left")
    .select(
        "a.*",
        "auth_ts_typed",
        F.col("t.transaction_id").alias("silver_transaction_id"),
        F.col("t.txn_ts").alias("silver_txn_ts"),
    )
)

fk_missing = F.col("silver_transaction_id").isNull()
auth_ts_invalid = F.col("auth_ts_typed").isNull() | (
    F.col("silver_transaction_id").isNotNull() &
    (F.col("auth_ts_typed") > F.col("silver_txn_ts"))
)


def quarantine_rows(condition, rule_id, rule_name, failure_reason):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("attempt_id").alias("record_key"),
        F.lit(rule_id).alias("rule_id"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(failure_reason).alias("failure_reason"),
        F.lit("quarantine").alias("severity"),
        F.lit("quarantined").alias("disposition"),
        F.to_json(F.struct(
            "attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"
        )).alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
    )


quarantine_df = (
    quarantine_rows(
        fk_missing,
        "DQ-AUTH-TXN-FK",
        "transaction_id must exist in transactions",
        "transaction_id does not resolve to Silver transactions",
    )
    .unionByName(quarantine_rows(
        auth_ts_invalid,
        "DQ-AUTH-TS-ORDER",
        "auth_ts must not be later than txn_ts",
        "auth_ts is missing, invalid, or after linked transaction timestamp",
    ))
)

# ---------------------------------------------------------------------------
# 3. WRITE TO QUARANTINE SINK
# ---------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gov")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE_NAME} (
      run_id STRING,
      source_table STRING,
      source_record_id STRING,
      record_key STRING,
      rule_id STRING,
      rule_name STRING,
      failure_reason STRING,
      severity STRING,
      disposition STRING,
      raw_record STRING,
      detected_at TIMESTAMP
    ) USING DELTA
""")

print(f"Cleaning prior quarantine records for {TABLE_NAME} under run {RUN_ID}...")
spark.sql(f"""
    DELETE FROM {QUARANTINE_TABLE_NAME}
    WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'
""")

failed_count = quarantine_df.count()
if failed_count > 0:
    print(f"Writing {failed_count} failed records to quarantine...")
    quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE_NAME)
else:
    print("No failed records found to quarantine.")

# ---------------------------------------------------------------------------
# 4. FILTER CLEAN RECORDS
# ---------------------------------------------------------------------------
decline_reason_trimmed = F.trim(F.col("decline_reason"))

silver_auth_attempts_df = checked_df.filter(
    F.col("silver_transaction_id").isNotNull() &
    F.col("auth_ts_typed").isNotNull() &
    (F.col("auth_ts_typed") <= F.col("silver_txn_ts"))
).select(
    F.col("attempt_id"),
    F.col("transaction_id"),
    F.lower(F.trim(F.col("decision"))).alias("decision"),
    F.when(decline_reason_trimmed == "", F.lit(None).cast("string"))
        .otherwise(decline_reason_trimmed)
        .alias("decline_reason"),
    F.col("auth_ts_typed").alias("auth_ts"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash"),
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER AUTH ATTEMPTS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_auth_attempts_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. UPDATE METADATA LINEAGE
# ---------------------------------------------------------------------------
lineage_schema = StructType([
    StructField("source_catalog", StringType(), nullable=True),
    StructField("source_schema", StringType(), nullable=True),
    StructField("source_table", StringType(), nullable=True),
    StructField("source_field", StringType(), nullable=True),
    StructField("target_catalog", StringType(), nullable=True),
    StructField("target_schema", StringType(), nullable=True),
    StructField("target_table", StringType(), nullable=True),
    StructField("target_field", StringType(), nullable=True),
    StructField("transformation_logic", StringType(), nullable=True),
])

lineage_rows = [
    (CATALOG, "bronze", TABLE_NAME, "attempt_id", CATALOG, SCHEMA, TABLE_NAME, "attempt_id", "Direct copy from latest Bronze batch"),
    (CATALOG, "bronze", TABLE_NAME, "transaction_id", CATALOG, SCHEMA, TABLE_NAME, "transaction_id", "Direct copy after Silver transaction relationship check"),
    (CATALOG, "bronze", TABLE_NAME, "decision", CATALOG, SCHEMA, TABLE_NAME, "decision", "Lowercased and trimmed"),
    (CATALOG, "bronze", TABLE_NAME, "decline_reason", CATALOG, SCHEMA, TABLE_NAME, "decline_reason", "Trimmed; empty string converted to NULL"),
    (CATALOG, "bronze", TABLE_NAME, "auth_ts", CATALOG, SCHEMA, TABLE_NAME, "auth_ts", "Parsed to TIMESTAMP; invalid or after transaction timestamp quarantined"),
]

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LINEAGE_TABLE_NAME} (
      source_catalog STRING,
      source_schema STRING,
      source_table STRING,
      source_field STRING,
      target_catalog STRING,
      target_schema STRING,
      target_table STRING,
      target_field STRING,
      transformation_logic STRING
    ) USING DELTA
""")
spark.sql(f"""
    DELETE FROM {LINEAGE_TABLE_NAME}
    WHERE target_schema = '{SCHEMA}' AND target_table = '{TABLE_NAME}'
""")
spark.createDataFrame(lineage_rows, schema=lineage_schema) \
    .write.format("delta").mode("append").saveAsTable(LINEAGE_TABLE_NAME)

# ---------------------------------------------------------------------------
# 7. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Auth Attempts:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS silver_rows FROM {FULL_TABLE_NAME}").show()
spark.sql(f"""
    SELECT COUNT(*) AS quarantine_rows
    FROM {QUARANTINE_TABLE_NAME}
    WHERE run_id = '{RUN_ID}' AND source_table = '{TABLE_NAME}'
""").show()
