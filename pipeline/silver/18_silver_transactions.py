# Databricks notebook source
# ============================================================================
# SILVER TRANSFORMATION & DATA QUALITY PIPELINE: transactions
# ============================================================================
# Implements Bronze -> Silver transformation for the transactions dataset:
#   1. Reads from bronze.transactions
#   2. Uses the latest fact snapshot and validates relationships against clean
#      Silver parent tables
#   3. Quarantines failed records to silver.quarantine_records
#   4. Writes clean records to silver.transactions
#   5. Appends lineage rows to gov.metadata_lineage
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
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
    """Create or reuse the shared catalog widget and validate its value."""
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
TABLE_NAME = "transactions"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
BRONZE_TABLE_NAME = f"{CATALOG}.bronze.{TABLE_NAME}"
QUARANTINE_TABLE_NAME = f"{CATALOG}.{SCHEMA}.quarantine_records"
LINEAGE_TABLE_NAME = f"{CATALOG}.gov.metadata_lineage"
RUN_TS_LIMIT = "2026-07-06 23:59:59"

# ---------------------------------------------------------------------------
# 1. LOAD BRONZE AND REFERENCE DATA
# ---------------------------------------------------------------------------
print(f"Reading from Bronze table: {BRONZE_TABLE_NAME}")
transactions_all_df = spark.read.table(BRONZE_TABLE_NAME)
print("Using latest transactions batch snapshot")
transactions_df = latest_batch_snapshot(transactions_all_df).alias("t")
RUN_ID = snapshot_run_id(transactions_df)

print("Reading clean Silver accounts, cards, and merchants for FK validation...")
accounts_df = spark.read.table(f"{CATALOG}.{SCHEMA}.accounts") \
    .select(F.col("account_id").alias("silver_account_id")).distinct().alias("a")
cards_df = spark.read.table(f"{CATALOG}.{SCHEMA}.cards") \
    .select(
        F.col("card_id").alias("silver_card_id"),
        F.lower(F.trim(F.col("status"))).alias("silver_card_status"),
    ).distinct().alias("c")
merchants_df = spark.read.table(f"{CATALOG}.{SCHEMA}.merchants") \
    .select(F.col("merchant_id").alias("silver_merchant_id")).distinct().alias("m")

# ---------------------------------------------------------------------------
# 2. RUN DQ RULES & IDENTIFY FAILURES (QUARANTINE)
# ---------------------------------------------------------------------------
txn_window = Window.partitionBy("transaction_id").orderBy("_ingest_ts", "_record_hash")
CAST_RULES = [
    TypeCastRule("amount", "amount_typed", "DECIMAL(12,2)", "DQ-TXN-AMOUNT-TYPE"),
    TypeCastRule(
        "txn_ts", "txn_ts_typed", "TIMESTAMP", "DQ-TXN-TS-TYPE",
        "try_to_timestamp(replace(replace(txn_ts, 'T', ' '), 'Z', ''))",
    ),
]

typed_transactions_df = (
    apply_type_casts(transactions_df, CAST_RULES)
    .withColumn("rn_txn", F.row_number().over(txn_window))
)

checked_df = (
    typed_transactions_df.alias("t")
    .join(cards_df, F.col("t.card_id") == F.col("c.silver_card_id"), "left")
    .join(accounts_df, F.col("t.account_id") == F.col("a.silver_account_id"), "left")
    .join(merchants_df, F.col("t.merchant_id") == F.col("m.silver_merchant_id"), "left")
    .select("t.*", "silver_card_id", "silver_card_status", "silver_account_id", "silver_merchant_id")
)

amount_invalid = F.col("amount_typed").isNull() | (F.col("amount_typed") <= 0)
merchant_missing = F.col("merchant_id").isNull() | (F.trim(F.col("merchant_id")) == "")
txn_ts_invalid = F.col("txn_ts_typed").isNull() | (
    F.col("txn_ts_typed") > F.lit(RUN_TS_LIMIT).cast("timestamp")
)
txn_duplicate = F.col("rn_txn") > 1
account_fk_missing = F.col("silver_account_id").isNull()
card_fk_missing = (
    F.col("card_id").isNotNull() &
    (F.trim(F.col("card_id")) != "") &
    F.col("silver_card_id").isNull()
)
merchant_fk_missing = ~merchant_missing & F.col("silver_merchant_id").isNull()
closed_card = F.col("silver_card_status") == "closed"


def quarantine_rows(condition, rule_id, rule_name, failure_reason):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("transaction_id").alias("record_key"),
        F.lit(rule_id).alias("rule_id"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(failure_reason).alias("failure_reason"),
        F.lit("quarantine").alias("severity"),
        F.lit("quarantined").alias("disposition"),
        F.to_json(F.struct(
            "transaction_id", "account_id", "card_id", "merchant_id", "channel",
            "amount", "currency", "txn_ts", "status"
        )).alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
    )


quarantine_df = (
    quarantine_rows(amount_invalid, "DQ-TXN-AMT-POS", "amount must be > 0", "amount is missing, invalid, or not positive")
    .unionByName(quarantine_rows(merchant_missing, "DQ-TXN-MERCH-REQ", "merchant_id is required", "missing merchant_id"))
    .unionByName(quarantine_rows(txn_ts_invalid, "DQ-TXN-TS-FUTURE", "txn_ts must not be in the future", "txn_ts is missing, invalid, or after RUN_DATE"))
    .unionByName(quarantine_rows(txn_duplicate, "DQ-TXN-ID-DUP", "transaction_id must be unique", "duplicate transaction_id"))
    .unionByName(quarantine_rows(account_fk_missing, "DQ-TXN-ACCT-FK", "account_id must exist in Silver accounts", "account_id does not resolve to Silver accounts"))
    .unionByName(quarantine_rows(card_fk_missing, "DQ-TXN-CARD-FK", "card_id must exist in Silver cards", "card_id does not resolve to Silver cards"))
    .unionByName(quarantine_rows(merchant_fk_missing, "DQ-TXN-MERCH-FK", "merchant_id must exist in Silver merchants", "merchant_id does not resolve to Silver merchants"))
    .unionByName(quarantine_rows(closed_card, "DQ-TXN-CARD-ACTIVE", "transaction must use an active card", "transaction uses a closed card"))
    .unionByName(type_cast_quarantine_rows(
        checked_df, CAST_RULES, TABLE_NAME, "transaction_id", RUN_ID
    ))
)
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

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
    distinct_failed_count = quarantine_df.select("source_record_id").distinct().count()
    print(f"Writing {failed_count} quarantine rule rows for {distinct_failed_count} distinct failed transactions...")
    print("Transaction quarantine counts by rule:")
    quarantine_df.groupBy("rule_id").count().orderBy("rule_id").show(truncate=False)
    quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE_NAME)
else:
    print("No failed records found to quarantine.")

# ---------------------------------------------------------------------------
# 4. FILTER CLEAN RECORDS
# ---------------------------------------------------------------------------
silver_transactions_df = checked_df.filter(
    (F.col("amount_typed") > 0) &
    F.col("merchant_id").isNotNull() &
    (F.trim(F.col("merchant_id")) != "") &
    (F.col("txn_ts_typed") <= F.lit(RUN_TS_LIMIT).cast("timestamp")) &
    (F.col("rn_txn") == 1) &
    F.col("silver_account_id").isNotNull() &
    (
        F.col("card_id").isNull() |
        (F.trim(F.col("card_id")) == "") |
        F.col("silver_card_id").isNotNull()
    ) &
    F.col("silver_merchant_id").isNotNull() &
    (F.coalesce(F.col("silver_card_status"), F.lit("")) != "closed")
    & ~any_cast_failure(CAST_RULES)
).select(
    F.col("transaction_id"),
    F.col("account_id"),
    F.col("card_id"),
    F.col("merchant_id"),
    F.lower(F.trim(F.col("channel"))).alias("channel"),
    F.col("amount_typed").alias("amount"),
    F.upper(F.trim(F.col("currency"))).alias("currency"),
    F.col("txn_ts_typed").alias("txn_ts"),
    F.lower(F.trim(F.col("status"))).alias("status"),
    F.col("_source_file"),
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    F.col("_run_id"),
    F.col("_batch_id").cast("long").alias("_batch_id"),
    F.col("_source_record_id"),
    F.col("_record_hash"),
)

# ---------------------------------------------------------------------------
# 5. WRITE CLEAN SILVER TRANSACTIONS TABLE
# ---------------------------------------------------------------------------
print(f"Writing clean records to Silver table: {FULL_TABLE_NAME}")
(
    silver_transactions_df.write
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
    (CATALOG, "bronze", TABLE_NAME, "transaction_id", CATALOG, SCHEMA, TABLE_NAME, "transaction_id", "Direct copy from latest Bronze batch after duplicate filtering"),
    (CATALOG, "bronze", TABLE_NAME, "account_id", CATALOG, SCHEMA, TABLE_NAME, "account_id", "Direct copy after Silver account existence check"),
    (CATALOG, "bronze", TABLE_NAME, "card_id", CATALOG, SCHEMA, TABLE_NAME, "card_id", "Direct copy after Silver card existence and closed-card checks"),
    (CATALOG, "bronze", TABLE_NAME, "merchant_id", CATALOG, SCHEMA, TABLE_NAME, "merchant_id", "Direct copy after Silver merchant existence check"),
    (CATALOG, "bronze", TABLE_NAME, "channel", CATALOG, SCHEMA, TABLE_NAME, "channel", "Lowercased and trimmed"),
    (CATALOG, "bronze", TABLE_NAME, "amount", CATALOG, SCHEMA, TABLE_NAME, "amount", "TRY_CAST to DECIMAL(12,2); non-positive or invalid values quarantined"),
    (CATALOG, "bronze", TABLE_NAME, "currency", CATALOG, SCHEMA, TABLE_NAME, "currency", "Uppercased and trimmed"),
    (CATALOG, "bronze", TABLE_NAME, "txn_ts", CATALOG, SCHEMA, TABLE_NAME, "txn_ts", "Parsed to TIMESTAMP; future or invalid timestamps quarantined"),
    (CATALOG, "bronze", TABLE_NAME, "status", CATALOG, SCHEMA, TABLE_NAME, "status", "Lowercased and trimmed"),
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
print("\nVerifying Silver Transactions:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS silver_rows FROM {FULL_TABLE_NAME}").show()
spark.sql(f"""
    SELECT COUNT(*) AS quarantine_rows
    FROM {QUARANTINE_TABLE_NAME}
    WHERE run_id = '{RUN_ID}' AND source_table = '{TABLE_NAME}'
""").show()
