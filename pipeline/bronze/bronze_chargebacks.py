# Databricks notebook source
# ============================================================================
# BRONZE INGEST: chargebacks
# ----------------------------------------------------------------------------
# Lands the raw source CSV for chargebacks as a string-typed Bronze Delta table
# with the 8 metadata columns (docs/bronze-layer.md S4). Mirrors the
# CREATE TABLE + COPY INTO block for chargebacks in 01_ingest_bronze.sql.
# Bronze contract: source cols as STRING; append-only + idempotent (skip rows
# whose _record_hash already exists). Dedup of dirty rows is Silver's job.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# `spark` is pre-initialized in a Databricks notebook.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
CATALOG = "g3_dev"
SCHEMA = "bronze"
TABLE_NAME = "chargebacks"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
RUN_ID = "RUN-20260706-1"
BATCH_ID = 1

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/{TABLE_NAME}.csv"
SOURCE_COLS = ["chargeback_id", "dispute_id", "scheme", "amount", "stage", "processed_at"]
RECORD_ID_COLS = ["chargeback_id"]

# ---------------------------------------------------------------------------
# 1. READ SOURCE CSV - all columns as STRING (typing is Silver's job)
# ---------------------------------------------------------------------------
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(SOURCE_PATH)
)

# ---------------------------------------------------------------------------
# 2. ATTACH METADATA + LINEAGE COLUMNS
#    _source_record_id = source PK, or concat_ws('|',...) for composite keys
#    _record_hash      = sha256 over the raw source row (all source cols)
# ---------------------------------------------------------------------------
record_id = (F.col(RECORD_ID_COLS[0])
             if len(RECORD_ID_COLS) == 1
             else F.concat_ws("|", *[F.col(c) for c in RECORD_ID_COLS]))
record_hash = F.sha2(F.concat_ws("|", *[F.col(c) for c in SOURCE_COLS]), 256)

bronze_df = df.select(
    *[F.col(c).cast("string") for c in SOURCE_COLS],
    F.col("_metadata.file_name").alias("_source_file"),
    F.col("_metadata.file_modification_time").alias("_source_file_mod_time"),
    F.current_timestamp().alias("_ingest_ts"),
    F.lit(RUN_ID).alias("_run_id"),
    F.lit(BATCH_ID).cast("long").alias("_batch_id"),
    record_id.alias("_source_record_id"),
    record_hash.alias("_record_hash"),
    F.lit(None).cast("string").alias("_rescued_data"),
)

# ---------------------------------------------------------------------------
# 3. CREATE SCHEMA IF NOT EXISTS
# ---------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# ---------------------------------------------------------------------------
# 4. IDEMPOTENT APPEND - skip rows whose _record_hash is already loaded
# ---------------------------------------------------------------------------
try:
    existing_hashes = spark.read.table(FULL_TABLE_NAME).select("_record_hash")
    new_rows = bronze_df.join(existing_hashes, on="_record_hash", how="left_anti")
    print(f"Target exists; deduping incoming rows against existing _record_hash...")
except AnalysisException:
    new_rows = bronze_df                       # first run - table absent
    print(f"Target {FULL_TABLE_NAME} does not exist yet; initial load.")

incoming = new_rows.count()
if incoming > 0:
    (new_rows.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME))
    print(f"Appended {incoming} new row(s) to {FULL_TABLE_NAME}")
else:
    print(f"No new rows to append to {FULL_TABLE_NAME} (all already loaded).")

# ---------------------------------------------------------------------------
# 5. VERIFY
# ---------------------------------------------------------------------------
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {FULL_TABLE_NAME}").show()
