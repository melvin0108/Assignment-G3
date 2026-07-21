# Databricks notebook source
"""Bronze-to-Silver transformation and risk-rating validation for merchants."""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import (
    deduplicate_quarantine_rows, exclude_dq_quarantined_rows,
    latest_batch_snapshot, snapshot_run_id,
)
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
TABLE_NAME = "merchants"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"
quarantine_table = f"{CATALOG}.silver.quarantine_records"

CAST_RULES = [TypeCastRule(
    "effective_at", "effective_at_typed", "TIMESTAMP", "DQ-MERCH-EFFECTIVE-TYPE"
)]
checked_df = apply_type_casts((
    latest_batch_snapshot(spark.read.table(bronze_table))
    .withColumn("risk_rating_normalized", F.lower(F.trim("risk_rating")))
), CAST_RULES)
RUN_ID = snapshot_run_id(checked_df)
invalid_risk = ~F.col("risk_rating_normalized").isin("low", "medium", "high")

quarantine_df = checked_df.filter(invalid_risk).select(
    F.lit(RUN_ID).alias("run_id"), F.lit(TABLE_NAME).alias("source_table"),
    F.col("_source_record_id").alias("source_record_id"), F.col("merchant_id").alias("record_key"),
    F.lit("DQ-MERCH-RISK-CASING").alias("rule_id"),
    F.lit("risk_rating must be in {low,medium,high}").alias("rule_name"),
    F.lit("inconsistent casing").alias("failure_reason"), F.lit("quarantine").alias("severity"),
    F.lit("quarantined").alias("disposition"),
    F.to_json(F.struct("merchant_id", "risk_rating", "status")).alias("raw_record"),
    F.current_timestamp().alias("detected_at"),
)
quarantine_df = quarantine_df.unionByName(type_cast_quarantine_rows(
    checked_df, CAST_RULES, TABLE_NAME, "merchant_id", RUN_ID
))
quarantine_df = deduplicate_quarantine_rows(quarantine_df)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING,
    rule_id STRING, rule_name STRING, failure_reason STRING, severity STRING,
    disposition STRING, raw_record STRING, detected_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

# SCD Type 1: retain one current merchant row from the latest full snapshot.
dedupe_window = Window.partitionBy("merchant_id").orderBy(F.col("_ingest_ts").desc(), F.col("_record_hash").desc())
silver_df = (checked_df.filter(~invalid_risk & ~any_cast_failure(CAST_RULES)).withColumn("_row_num", F.row_number().over(dedupe_window))
    .filter(F.col("_row_num") == 1).select(
        "merchant_id", "name", "mcc", "country", "risk_rating_normalized", "status",
        F.col("effective_at_typed").alias("effective_at"), "_source_file",
        F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
        F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
    ).withColumnRenamed("risk_rating_normalized", "risk_rating"))
silver_df = exclude_dq_quarantined_rows(
    silver_df, spark, CATALOG, TABLE_NAME, RUN_ID
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
