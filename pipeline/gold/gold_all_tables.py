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
    "01_gold_dim_date",
    "02_gold_dim_merchant",
    "03_gold_dim_channel",
    "04_gold_dim_dispute_reason",
    "05_gold_dim_currency",
    "06_gold_fact_case_transaction",
    "07_gold_fact_authorization_attempt",
    "08_gold_fact_dispute",
    "09_gold_fact_chargeback",
    "10_gold_fact_fraud_alert",
    "11_gold_fact_investigation_note",
    "12_gold_fact_case_party_summary",
    "13_gold_dim_case",
    "14_gold_investigation_context",
]

for position, notebook_name in enumerate(GOLD_NOTEBOOKS, start=1):
    print(f"[{position}/{len(GOLD_NOTEBOOKS)}] Starting gold.{notebook_name}")
    dbutils.notebook.run(f"./{notebook_name}", 0, {"catalog": CATALOG})

print("Completed Gold models with output-level role policy labels")
