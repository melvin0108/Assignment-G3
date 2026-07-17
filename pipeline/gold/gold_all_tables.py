# Databricks notebook source
"""Run the Gold dimensional mart in its dependency-safe order."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from pipeline.gold.gold_common import catalog_widget, matching_silver_snapshot
from pipeline.gold.gold_models import build_case_and_facts, build_dimensions, build_investigation_context


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
BATCH_ID, RUN_ID = matching_silver_snapshot(spark, CATALOG)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
print(f"Validated Silver snapshot: batch {BATCH_ID}, run {RUN_ID}")
build_dimensions(spark, CATALOG, RUN_ID, BATCH_ID)
build_case_and_facts(spark, CATALOG, RUN_ID, BATCH_ID)
build_investigation_context(spark, CATALOG, RUN_ID, BATCH_ID)

# Free Edition exposes the built-in workspace users group for Gold access.
for statement in (
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `users`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `users`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `users`",
):
    spark.sql(statement)

print("Completed Gold models and users-group Gold-only grants")
