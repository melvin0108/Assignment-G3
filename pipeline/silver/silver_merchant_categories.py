# Databricks notebook source
"""Bronze-to-Silver transformation for merchant-category reference data."""

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
TABLE_NAME = "merchant_categories"
bronze_table = f"{CATALOG}.bronze.{TABLE_NAME}"
silver_table = f"{CATALOG}.silver.{TABLE_NAME}"

silver_df = latest_batch_snapshot(spark.read.table(bronze_table)).select(
    "mcc", "category_name", "category_group", "_source_file",
    F.col("_source_file_mod_time").cast("timestamp").alias("_source_file_mod_time"),
    F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"), "_run_id",
    F.col("_batch_id").cast("long").alias("_batch_id"), "_source_record_id", "_record_hash",
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"Table created/updated successfully: {silver_table}")
