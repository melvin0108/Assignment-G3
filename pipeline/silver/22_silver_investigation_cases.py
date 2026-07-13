# Databricks notebook source
"""Bronze-to-Silver transformation and DQ quarantine for investigation cases."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

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
TABLE_NAME = "investigation_cases"
RUN_ID = "RUN-20260713-1"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"
quarantine_table = f"{CATALOG}.silver.quarantine_records"

checked_df = (spark.read.table(bronze_table)
    .withColumn("opened_at_typed", F.to_timestamp("opened_at"))
    .withColumn("closed_at_typed", F.to_timestamp("closed_at"))
    .withColumn("legal_hold_typed", F.col("legal_hold").cast("boolean")))

def failures(condition, rule_id, rule_name, reason, disposition="quarantined"):
    return checked_df.filter(condition).select(
        F.lit(RUN_ID).alias("run_id"), F.lit(TABLE_NAME).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("case_id").alias("record_key"), F.lit(rule_id).alias("rule_id"), F.lit(rule_name).alias("rule_name"),
        F.lit(reason).alias("failure_reason"), F.lit("quarantine").alias("severity"), F.lit(disposition).alias("disposition"),
        F.to_json(F.struct("case_id", "status_code", "fraud_type_code", "legal_hold")).alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
    )

invalid_status = ~F.col("status_code").isin("open", "in_progress", "suspended", "closed")
stale_open = (F.col("status_code") == "open") & (F.to_date("opened_at_typed") < F.date_sub(F.lit("2026-07-06").cast("date"), 180))
legal_hold = F.col("legal_hold_typed")
quarantine_df = (failures(invalid_status, "DQ-CASE-STATUS-ENUM", "status_code must be in case_status enum", "status not in enum")
    .unionByName(failures(stale_open, "DQ-CASE-STALE", "open cases older than 180 days are stale", "stale open case"))
    .unionByName(failures(legal_hold, "DQ-CASE-LEGALHOLD", "legal_hold cases excluded from AI output", "legal_hold=true (must-not-expose)", "allowed_with_warning")))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING, rule_id STRING,
    rule_name STRING, failure_reason STRING, severity STRING, disposition STRING, raw_record STRING,
    detected_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

silver_df = checked_df.filter(~invalid_status & ~stale_open & ~legal_hold).select(
    "case_id", "priority", "status_code", "fraud_type_code", "owner_employee_id", F.col("opened_at_typed").alias("opened_at"),
    F.col("closed_at_typed").alias("closed_at"), F.col("legal_hold_typed").alias("legal_hold"), "_source_file",
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
