# Databricks notebook source
"""Bronze-to-Silver transformation and AI-safety DQ quarantine for investigation notes."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import latest_batch_snapshot

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
TABLE_NAME = "investigation_notes"
RUN_ID = "RUN-20260713-1"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
cases_table = f"{CATALOG}.bronze.investigation_cases"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"
quarantine_table = f"{CATALOG}.silver.quarantine_records"
PII_PATTERN = r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})|(\+\d{6,15})|(\b\d{13,19}\b)|(\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b)"

cases_df = spark.read.table(cases_table).select("case_id", F.col("legal_hold").cast("boolean").alias("case_legal_hold")).distinct()
checked_df = latest_batch_snapshot(spark.read.table(bronze_table)).join(cases_df, "case_id", "left")

def failures(condition, rule_id, rule_name, reason, disposition="quarantined"):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"), F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("note_id").alias("record_key"), F.lit(rule_id).alias("rule_id"), F.lit(rule_name).alias("rule_name"),
        F.lit(reason).alias("failure_reason"), F.lit("quarantine").alias("severity"), F.lit(disposition).alias("disposition"),
        F.to_json(F.struct("note_id", "case_id", "note_text")).alias("raw_record"), F.current_timestamp().alias("detected_at"),
    )

contains_pii = F.col("note_text").rlike(PII_PATTERN)
legal_hold_note = F.coalesce(F.col("case_legal_hold"), F.lit(False))
quarantine_df = (failures(contains_pii, "DQ-NOTE-PII-LEAK", "note_text must not contain raw PII/PAN", "leaked PII and PAN in free text")
    .unionByName(failures(legal_hold_note, "DQ-NOTE-LEGALHOLD", "notes on legal_hold cases must not reach AI", "note on legal_hold case", "allowed_with_warning")))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING, rule_id STRING,
    rule_name STRING, failure_reason STRING, severity STRING, disposition STRING, raw_record STRING,
    detected_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

silver_df = checked_df.filter(~contains_pii & ~legal_hold_note).select(
    "note_id", "case_id", "author_employee_id", "note_text", F.to_timestamp("created_at").alias("created_at"),
    "_source_file", F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
