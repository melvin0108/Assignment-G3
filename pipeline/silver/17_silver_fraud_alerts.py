# Databricks notebook source
"""Bronze-to-Silver transformation and score validation for fraud alerts."""

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
TABLE_NAME = "fraud_alerts"
RUN_ID = "RUN-20260713-1"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"
quarantine_table = f"{CATALOG}.silver.quarantine_records"

source_df = spark.read.table(bronze_table)
latest_batch_id = source_df.select(F.max("_batch_id").alias("batch_id")).first()["batch_id"]
checked_df = source_df.filter(F.col("_batch_id") == latest_batch_id).withColumn("score_typed", F.expr("try_cast(score AS DOUBLE)"))
invalid_score = (F.col("score_typed") < 0) | (F.col("score_typed") > 1)
quarantine_df = checked_df.filter(invalid_score).select(
    F.lit(RUN_ID).alias("run_id"), F.lit(TABLE_NAME).alias("source_table"), "_source_record_id",
    F.col("alert_id").alias("record_key"), F.lit("DQ-ALT-SCORE-RANGE").alias("rule_id"),
    F.lit("score must be within [0,1]").alias("rule_name"), F.lit("score out of range").alias("failure_reason"),
    F.lit("quarantine").alias("severity"), F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct("alert_id", "transaction_id", "score")).alias("raw_record"), F.current_timestamp().alias("detected_at"),
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING, rule_id STRING,
    rule_name STRING, failure_reason STRING, severity STRING, disposition STRING, raw_record STRING,
    detected_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

silver_df = checked_df.filter(~invalid_score).select(
    "alert_id", "transaction_id", "rule_name", F.col("score_typed").alias("score"),
    F.to_timestamp("triggered_at").alias("triggered_at"), "disposition", "_source_file",
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
