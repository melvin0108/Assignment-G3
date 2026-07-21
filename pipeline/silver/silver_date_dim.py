# Databricks notebook source
"""Bronze-to-Silver transformation for the calendar reference dimension."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import deduplicate_quarantine_rows, latest_batch_snapshot, snapshot_run_id
from pipeline.silver.type_cast import (
    TypeCastRule, any_cast_failure, apply_type_casts,
    ensure_quarantine_table, type_cast_quarantine_rows,
)

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
TABLE_NAME = "date_dim"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"

source_df = latest_batch_snapshot(spark.read.table(bronze_table))
RUN_ID = snapshot_run_id(source_df)
CAST_RULES = [
    TypeCastRule("date_id", "date_id_typed", "DATE", "DQ-DATE-ID-TYPE", "cast(try_to_timestamp(date_id, 'yyyyMMdd') AS DATE)"),
    TypeCastRule("year", "year_typed", "INT", "DQ-DATE-YEAR-TYPE"),
    TypeCastRule("month", "month_typed", "INT", "DQ-DATE-MONTH-TYPE"),
    TypeCastRule("quarter", "quarter_typed", "INT", "DQ-DATE-QUARTER-TYPE"),
    TypeCastRule("is_weekend", "is_weekend_typed", "BOOLEAN", "DQ-DATE-WEEKEND-TYPE"),
]
checked_df = apply_type_casts(source_df, CAST_RULES)
quarantine_df = deduplicate_quarantine_rows(type_cast_quarantine_rows(
    checked_df, CAST_RULES, TABLE_NAME, "date_id", RUN_ID
))
quarantine_table = ensure_quarantine_table(spark, CATALOG)
spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{TABLE_NAME}' AND run_id = '{RUN_ID}'")
if not quarantine_df.isEmpty():
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

silver_df = checked_df.filter(~any_cast_failure(CAST_RULES)).select(
    F.col("date_id_typed").alias("date_id"),
    F.col("year_typed").alias("year"),
    F.col("month_typed").alias("month"),
    F.col("quarter_typed").alias("quarter"),
    F.col("is_weekend_typed").alias("is_weekend"),
    "_source_file", F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
