# Databricks notebook source
"""Build the single safe Gold investigation-context Delta table."""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from pipeline.gold.case_context import build_case_base, build_warning_flags, current_quarantine
from pipeline.gold.common import assemble_context, catalog_widget, load_current_inputs, source_references, source_rows
from pipeline.gold.lineage import rewrite_gold_lineage
from pipeline.gold.supporting_context import build_supporting_context
from pipeline.gold.transaction_context import build_transaction_context


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
catalog = catalog_widget(dbutils)
gold_table = f"{catalog}.gold.investigation_context"

print("=== Gold investigation-context build ===")
inputs, batch_id, pipeline_run_id = load_current_inputs(spark, catalog)
print(f"INFO: building Gold from Silver batch {batch_id}, run {pipeline_run_id}")

quarantine = current_quarantine(spark, catalog, pipeline_run_id)
case_base, excluded_case_count = build_case_base(inputs, quarantine)
transaction = build_transaction_context(inputs)
supporting = build_supporting_context(inputs, quarantine)
warnings = build_warning_flags(quarantine)

source_frames = [
    source_rows(case_base, "investigation_cases"),
    case_base.selectExpr("case_id", "'fraud_types' AS source_table", "fraud_type_source_record_id AS source_record_id"),
    *transaction["sources"],
    *supporting["sources"],
]
collections = [*transaction["collections"], *supporting["collections"], warnings, source_references(source_frames)]
gold_df = assemble_context(case_base, collections, pipeline_run_id)

row_count = gold_df.count()
if row_count == 0:
    raise ValueError("Gold build produced no eligible investigation contexts")
pass_count = gold_df.where("quality_status = 'pass'").count()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)
rewrite_gold_lineage(spark, catalog)

print(f"Gold contexts: {row_count}; pass: {pass_count}; partial: {row_count - pass_count}; excluded cases: {excluded_case_count}")
spark.read.table(gold_table).select("case_id", "quality_status", "warning_flags", "source_references").show(10, truncate=False)
