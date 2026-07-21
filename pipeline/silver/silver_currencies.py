# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: currencies
# ============================================================================
# Implements Bronze -> Silver transformation for the currencies dataset:
#   1. Reads from bronze.currencies
#   2. Performs basic typecasting (decimals to INT) and validation
#   3. Writes clean records to silver.currencies
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from pipeline.silver.snapshot import deduplicate_quarantine_rows, latest_batch_snapshot, snapshot_run_id
from pipeline.silver.type_cast import (
    TypeCastRule, any_cast_failure, apply_type_casts,
    ensure_quarantine_table, type_cast_quarantine_rows,
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
TABLE_NAME = "currencies"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
df = latest_batch_snapshot(spark.read.table(BRONZE_TABLE_NAME))
RUN_ID = snapshot_run_id(df)
CAST_RULES = [TypeCastRule(
    "decimals", "decimals_typed", "INT", "DQ-CURR-DECIMALS-TYPE"
)]
checked_df = apply_type_casts(df, CAST_RULES)

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
quarantine_df = deduplicate_quarantine_rows(type_cast_quarantine_rows(
    checked_df, CAST_RULES, TABLE_NAME, "currency_code", RUN_ID
))
quarantine_table = ensure_quarantine_table(spark, CATALOG)
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

# ---------------------------------------------------------------------------
# 3. FILTER CLEAN RECORDS
# ---------------------------------------------------------------------------
# Construct Silver DataFrame
silver_currencies_df = checked_df.filter(~any_cast_failure(CAST_RULES)).select(
    F.col("currency_code"),
    F.col("name"),
    F.col("decimals_typed").alias("decimals"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash")
)

# ---------------------------------------------------------------------------
# 4. WRITE CLEAN SILVER CURRENCIES TABLE
# ---------------------------------------------------------------------------
# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_currencies_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 5. VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Silver Currencies:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
