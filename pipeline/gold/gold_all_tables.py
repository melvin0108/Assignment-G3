# Databricks notebook source
"""Run the Gold dimensional mart in its dependency-safe order."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from pipeline.gold.gold_common import catalog_widget, matching_silver_snapshot


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
BATCH_ID, RUN_ID = matching_silver_snapshot(spark, CATALOG)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
print(f"Validated Silver snapshot: batch {BATCH_ID}, run {RUN_ID}")

GOLD_NOTEBOOKS = [
    "dim_date",
    "dim_merchant",
    "dim_channel",
    "dim_dispute_reason",
    "dim_currency",
    "fact_case_transaction",
    "fact_authorization_attempt",
    "fact_dispute",
    "fact_chargeback",
    "fact_fraud_alert",
    "fact_investigation_note",
    "fact_case_party_summary",
    "dim_case",
    "investigation_context",
]

for position, notebook_name in enumerate(GOLD_NOTEBOOKS, start=1):
    print(f"[{position}/{len(GOLD_NOTEBOOKS)}] Starting gold.{notebook_name}")
    dbutils.notebook.run(f"./{notebook_name}", 0, {"catalog": CATALOG})

print("Completed Gold models with output-level role policy labels")
