# Databricks notebook source
# ============================================================================
# BRONZE INGEST: defects_manifest  (the DQ validation oracle)
# ----------------------------------------------------------------------------
# PySpark port of pipeline/bronze/02_ingest_defects_manifest.sql.
# Follows the SAME bronze contract as the 25 bronze_<source>.py files: source
# cols as STRING + the 8 metadata columns, append-only + idempotent (dedup by
# _record_hash). defects_manifest.csv is generated alongside the 25 source CSVs
# by python -m mock.generate, so it is treated as a first-class source file.
#
# WHAT THIS IS
#   The mock generator emits a "defects manifest" alongside the 25 source CSVs:
#   one row per *intentionally injected* bad record, with columns
#     source_table, record_key, rule_id, rule_name, failure_reason, severity
#   It is the GROUND TRUTH for data quality: later, the DQ engine writes the
#   records it actually caught into quarantine, and the reconciliation test
#   compares quarantine vs. this manifest (precision/recall per rule). So this
#   table is not business data -- it is the oracle that proves the DQ rules work.
#
# COMPOSITE KEY
#   Grain = one injected defect -> (source_table, record_key, rule_id).
#   _source_record_id = concat_ws('|', source_table, record_key, rule_id)
#   _record_hash      = sha256 over all 6 source cols (change/replay detection)
#
# !! SOURCE FILENAME MUST NOT START WITH AN UNDERSCORE !!
#   The source file is `defects_manifest.csv` (NO leading underscore). Databricks
#   treats any file/dir whose name begins with `_` or `.` as HIDDEN and silently
#   filters it out, making the read return 0 rows and look like success.
#   Do not re-add the leading underscore.
#
# IDEMPOTENCY / REGENERATING MOCK DATA
#   Append + dedup by _record_hash: re-running with the SAME (seed, defect-rate)
#   is a no-op (the regenerated CSV is byte-identical -> every hash already
#   loaded). If you regenerate with a DIFFERENT seed/defect-rate, drop the table
#   first (or use `make clean`) so the oracle reflects only the current manifest.
#
# EXPECTED ROW COUNT (default seed 42, full/stress scale) ~ 1,070,282. Counts
# drift if the seed or defect-rate changes; automated tests must derive expected
# sets from this table at runtime -- never hardcode the number.
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
TABLE_NAME = "defects_manifest"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
RUN_ID = "RUN-20260706-1"
BATCH_ID = 1

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/{TABLE_NAME}.csv"
SOURCE_COLS = ["source_table", "record_key", "rule_id", "rule_name", "failure_reason", "severity"]
RECORD_ID_COLS = ["source_table", "record_key", "rule_id"]  # composite -> _source_record_id

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
#    _source_record_id = concat_ws('|',...) composite key (one defect per row)
#    _record_hash      = sha256 over the raw source row (all source cols)
# ---------------------------------------------------------------------------
record_id = F.concat_ws("|", *[F.col(c) for c in RECORD_ID_COLS])
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

# Junk guard: no all-NULL rule_id rows. EXPECTED: 0 (guards against the earlier
# COPY INTO mis-load that produced ~2.4M all-NULL junk rows).
spark.sql(
    f"SELECT COUNT(*) AS null_rule_rows FROM {FULL_TABLE_NAME} "
    f"WHERE rule_id IS NULL OR rule_id = ''"
).show()

# Per-rule spot-check (top 10 by failure count).
spark.sql(
    f"SELECT rule_id, COUNT(*) AS n FROM {FULL_TABLE_NAME} "
    f"GROUP BY rule_id ORDER BY n DESC LIMIT 10"
).show()
