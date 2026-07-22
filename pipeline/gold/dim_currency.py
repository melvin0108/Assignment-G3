# Databricks notebook source
"""Gold transformation for dim_currency."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pipeline.gold.gold_common import catalog_widget, matching_silver_snapshot
from pipeline.gold.gold_models import build_dim_currency

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
BATCH_ID, RUN_ID = matching_silver_snapshot(spark, CATALOG)

build_dim_currency(spark, CATALOG, RUN_ID, BATCH_ID)
