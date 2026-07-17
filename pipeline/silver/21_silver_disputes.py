# Databricks notebook source
"""Bronze-to-Silver transformation and DQ quarantine for disputes."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import deduplicate_quarantine_rows, latest_batch_snapshot, snapshot_run_id
from pipeline.silver.type_cast import TypeCastRule, any_cast_failure, apply_type_casts, type_cast_quarantine_rows

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


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
TABLE_NAME = "disputes"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"
transactions_table = f"{CATALOG}.silver.transactions"
quarantine_table = f"{CATALOG}.silver.quarantine_records"

source_df = spark.read.table(bronze_table)
transactions_df = spark.read.table(transactions_table).select("transaction_id").distinct()
checked_df = (latest_batch_snapshot(source_df).alias("d")
    .join(transactions_df.alias("t"), F.col("d.transaction_id") == F.col("t.transaction_id"), "left")
    .select("d.*", F.col("t.transaction_id").alias("silver_transaction_id")))
CAST_RULES = [
    TypeCastRule("amount", "amount_typed", "DECIMAL(18,2)", "DQ-DISP-AMOUNT-TYPE"),
    TypeCastRule("raised_at", "raised_at_typed", "TIMESTAMP", "DQ-DISP-RAISED-TYPE"),
]
checked_df = apply_type_casts(checked_df, CAST_RULES)
RUN_ID = snapshot_run_id(checked_df)

def failures(condition, rule_id, rule_name, reason):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"), F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("dispute_id").alias("record_key"), F.lit(rule_id).alias("rule_id"),
        F.lit(rule_name).alias("rule_name"), F.lit(reason).alias("failure_reason"),
        F.lit("quarantine").alias("severity"), F.lit("quarantined").alias("disposition"),
        F.to_json(F.struct("dispute_id", "transaction_id", "reason_code", "status")).alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
    )

invalid_status = ~F.col("status").isin("open", "in_review", "resolved", "rejected", "withdrawn")
missing_reason = F.col("reason_code").isNull() | (F.trim("reason_code") == "")
missing_transaction = F.col("silver_transaction_id").isNull()
quarantine_df = (failures(invalid_status, "DQ-DISP-STATUS-ENUM", "status must be a lowercase dispute enum", "status casing/unknown")
    .unionByName(failures(missing_reason, "DQ-DISP-REASON-REQ", "reason_code is required", "missing reason_code"))
    .unionByName(failures(missing_transaction, "DQ-DISP-TXN-FK", "transaction_id must exist in transactions", "orphan transaction_id"))
    .unionByName(type_cast_quarantine_rows(checked_df, CAST_RULES, TABLE_NAME, "dispute_id", RUN_ID)))
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING, rule_id STRING,
    rule_name STRING, failure_reason STRING, severity STRING, disposition STRING, raw_record STRING,
    detected_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

silver_df = checked_df.filter(~invalid_status & ~missing_reason & ~missing_transaction & ~any_cast_failure(CAST_RULES)).select(
    "dispute_id", "transaction_id", F.trim("reason_code").alias("reason_code"),
    F.col("amount_typed").alias("amount"), "status",
    F.col("raised_at_typed").alias("raised_at"), "_source_file",
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
