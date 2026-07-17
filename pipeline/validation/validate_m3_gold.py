# Databricks notebook source
"""Acceptance validation for the Gold dimensional mart."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from pipeline.gold.gold_common import (
    FORBIDDEN_AI_COLUMNS,
    GOLD_MODELS,
    STANDARD_METADATA_COLUMNS,
    catalog_widget,
)


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


tables = {row.tableName for row in spark.sql(f"SHOW TABLES IN {CATALOG}.gold").collect()}
require(GOLD_MODELS <= tables, f"Missing Gold models: {sorted(GOLD_MODELS - tables)}")

identities = set()
for model in sorted(GOLD_MODELS):
    df = spark.read.table(f"{CATALOG}.gold.{model}")
    require(not df.isEmpty(), f"{model} must not be empty")
    require(STANDARD_METADATA_COLUMNS <= set(df.columns), f"{model} has incomplete metadata")
    require(not (set(df.columns) & FORBIDDEN_AI_COLUMNS), f"{model} exposes forbidden AI columns")
    identity_rows = df.select("pipeline_run_id", "batch_id").distinct().limit(2).collect()
    require(len(identity_rows) == 1, f"{model} must have one Gold batch/run identity")
    identities.add((identity_rows[0]["pipeline_run_id"], identity_rows[0]["batch_id"]))
    print(f"{model}: {df.count()} rows")
require(len(identities) == 1, f"Gold models have inconsistent snapshots: {identities}")

cases = spark.read.table(f"{CATALOG}.gold.dim_case")
context = spark.read.table(f"{CATALOG}.gold.investigation_context")
require(cases.select("case_key").distinct().count() == cases.count(), "dim_case keys must be unique")
require(context.select("case_id").distinct().count() == context.count(), "investigation_context must have one row per case")
require(context.count() == cases.count(), "context rows must reconcile to dim_case")
require(context.filter("context_version <> '2.0.0' OR masking_status NOT IN ('masked', 'partial')").isEmpty(), "invalid context version or masking status")

for fact, fk in (
    ("fact_case_transaction", "case_key"), ("fact_authorization_attempt", "case_key"),
    ("fact_dispute", "case_key"), ("fact_chargeback", "case_key"),
    ("fact_fraud_alert", "case_key"), ("fact_investigation_note", "case_key"),
    ("fact_case_party_summary", "case_key"),
):
    missing = (spark.read.table(f"{CATALOG}.gold.{fact}").select(fk).distinct()
        .join(cases.select("case_key"), fk, "left_anti").count())
    require(missing == 0, f"{fact} contains unresolved {fk}")

print("M3 Gold validation passed: inventory, metadata, safety, keys, context, and referential integrity")
