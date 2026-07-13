# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: transaction_devices
# ============================================================================
# Implements Bronze -> Silver transformation for transaction device activity:
#   1. Reads the latest bronze.transaction_devices snapshot
#   2. Checks transaction FK and required device type
#   3. Quarantines failed records to silver.quarantine_records
#   4. Tokenizes device_id and protects IP values
#   5. Writes clean records to silver.transaction_devices
#   6. Appends governance rows for masking policies and lineage
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
CATALOG = "g3_test"
SCHEMA = "silver"
TABLE_NAME = "transaction_devices"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
TRANSACTIONS_TABLE_NAME = f"{CATALOG}.{SCHEMA}.transactions"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
MASKING_POLICIES_TABLE_NAME = f"{CATALOG}.gov.masking_policies"
LINEAGE_TABLE_NAME = f"{CATALOG}.gov.metadata_lineage"
RUN_ID = "RUN-20260706-1"
SALT = "NAB_SALT_2026"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE AND REFERENCE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
devices_all_df = spark.read.table(BRONZE_TABLE_NAME)
latest_batch_id = devices_all_df.select(F.max("_batch_id").alias("batch_id")).first()["batch_id"]
print(f"Using latest transaction_devices batch: {latest_batch_id}")
devices_df = devices_all_df.filter(F.col("_batch_id") == latest_batch_id).alias("d")

print(f"Reading Silver transactions for FK validation: {TRANSACTIONS_TABLE_NAME}")
transactions_df = spark.read.table(TRANSACTIONS_TABLE_NAME).select("transaction_id").distinct().alias("t")

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
checked_df = (
    devices_df
    .join(transactions_df, F.col("d.transaction_id") == F.col("t.transaction_id"), "left")
    .select(
        "d.*",
        F.col("t.transaction_id").alias("silver_transaction_id"),
    )
)

fk_missing = F.col("silver_transaction_id").isNull()
device_type_missing = F.col("device_type").isNull() | (F.trim(F.col("device_type")) == "")


def quarantine_rows(condition, rule_id, rule_name, failure_reason):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("device_id").alias("record_key"),
        F.lit(rule_id).alias("rule_id"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(failure_reason).alias("failure_reason"),
        F.lit("quarantine").alias("severity"),
        F.lit("quarantined").alias("disposition"),
        F.to_json(F.struct(
            "device_id", "transaction_id", "device_type", "ip", "geo_country"
        )).alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
    )


quarantine_df = (
    quarantine_rows(
        fk_missing,
        "DQ-DEV-TXN-FK",
        "transaction_id must exist in transactions",
        "transaction_id does not resolve to Silver transactions",
    )
    .unionByName(quarantine_rows(
        device_type_missing,
        "DQ-DEV-TYPE-REQ",
        "device_type is required",
        "missing device_type",
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
# 4. FILTER CLEAN RECORDS & APPLY PROTECTION
# ---------------------------------------------------------------------------
is_ipv4 = F.col("ip").rlike(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$")
ip_masked = (
    F.when(is_ipv4, F.regexp_replace(F.col("ip"), r"^(\d+\.\d+\.\d+)\.\d+$", "$1.0/24"))
    .when(F.col("ip").isNull() | (F.trim(F.col("ip")) == ""), F.lit(None).cast("string"))
    .otherwise(F.concat(
        F.lit("IP_HASH_"),
        F.substring(F.sha2(F.concat(F.lower(F.trim(F.col("ip"))), F.lit(SALT)), 256), 1, 16),
    ))
)

silver_transaction_devices_df = checked_df.filter(
    F.col("silver_transaction_id").isNotNull() &
    F.col("device_type").isNotNull() &
    (F.trim(F.col("device_type")) != "")
).select(
    F.concat(
        F.lit("DEV_"),
        F.substring(F.sha2(F.concat(F.lower(F.trim(F.col("device_id"))), F.lit(SALT)), 256), 1, 16),
    ).alias("device_id"),
    F.col("transaction_id"),
    F.lower(F.trim(F.col("device_type"))).alias("device_type"),
    ip_masked.alias("ip"),
    F.upper(F.trim(F.col("geo_country"))).alias("geo_country"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash"),
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER TRANSACTION DEVICES TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_transaction_devices_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 6. UPDATE MASKING POLICIES AND METADATA LINEAGE
# ---------------------------------------------------------------------------
masking_schema = StructType([
    StructField("table_name", StringType(), nullable=True),
    StructField("field_name", StringType(), nullable=True),
    StructField("classification", StringType(), nullable=True),
    StructField("protection_method", StringType(), nullable=True),
    StructField("allowed_role", StringType(), nullable=True),
    StructField("owner", StringType(), nullable=True),
])

masking_rows = [
    (TABLE_NAME, "device_id", "device/session identifier", "tokenize with salted SHA256 prefix", "unprivileged", "M4"),
    (TABLE_NAME, "ip", "network identifier", "truncate IPv4 to /24 or hash non-IPv4 value", "unprivileged", "M4"),
]

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
    (CATALOG, "bronze", TABLE_NAME, "device_id", CATALOG, SCHEMA, TABLE_NAME, "device_id", "Tokenized with salted SHA256 prefix from latest Bronze batch"),
    (CATALOG, "bronze", TABLE_NAME, "transaction_id", CATALOG, SCHEMA, TABLE_NAME, "transaction_id", "Direct copy after Silver transaction relationship check"),
    (CATALOG, "bronze", TABLE_NAME, "device_type", CATALOG, SCHEMA, TABLE_NAME, "device_type", "Lowercased and trimmed; missing values quarantined"),
    (CATALOG, "bronze", TABLE_NAME, "ip", CATALOG, SCHEMA, TABLE_NAME, "ip", "IPv4 reduced to /24-style network; non-IPv4 hashed"),
    (CATALOG, "bronze", TABLE_NAME, "geo_country", CATALOG, SCHEMA, TABLE_NAME, "geo_country", "Uppercased and trimmed"),
]

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MASKING_POLICIES_TABLE_NAME} (
      table_name STRING,
      field_name STRING,
      classification STRING,
      protection_method STRING,
      allowed_role STRING,
      owner STRING
    ) USING DELTA
""")
spark.sql(f"DELETE FROM {MASKING_POLICIES_TABLE_NAME} WHERE table_name = '{TABLE_NAME}'")
spark.createDataFrame(masking_rows, schema=masking_schema) \
    .write.format("delta").mode("append").saveAsTable(MASKING_POLICIES_TABLE_NAME)

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
print("\nVerifying Silver Transaction Devices:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS silver_rows FROM {FULL_TABLE_NAME}").show()
spark.sql(f"""
    SELECT COUNT(*) AS quarantine_rows
    FROM {QUARANTINE_TABLE_NAME}
    WHERE run_id = '{RUN_ID}' AND source_table = '{TABLE_NAME}'
""").show()
