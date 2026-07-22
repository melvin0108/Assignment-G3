# Databricks notebook source
"""Gold transformation for fact_investigation_note."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pipeline.gold.gold_common import catalog_widget, matching_silver_snapshot
from pipeline.gold.gold_models import build_fact_investigation_note

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
BATCH_ID, RUN_ID = matching_silver_snapshot(spark, CATALOG)

build_fact_investigation_note(spark, CATALOG, RUN_ID, BATCH_ID)
