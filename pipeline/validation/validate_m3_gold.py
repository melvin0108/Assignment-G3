# Databricks notebook source
"""Acceptance validation for the Gold dimensional mart."""

from functools import reduce

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

from pipeline.gold.gold_common import (
    FORBIDDEN_AI_COLUMNS,
    GOLD_MODELS,
    STANDARD_METADATA_COLUMNS,
    catalog_widget,
    gold_model_dir,
)


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
OPTIONAL_FACT_MODELS = {model for model in GOLD_MODELS if model.startswith("fact_")}
EXPECTED_PRIMARY_KEYS = {
    "dim_date": ["date_key"],
    "dim_case": ["case_id"],
    "dim_merchant": ["merchant_id"],
    "dim_channel": ["channel_code"],
    "dim_dispute_reason": ["reason_code"],
    "dim_currency": ["currency_code"],
    "fact_case_transaction": ["case_id", "transaction_id"],
    "fact_authorization_attempt": ["case_id", "attempt_id"],
    "fact_dispute": ["case_id", "dispute_id"],
    "fact_chargeback": ["case_id", "chargeback_id"],
    "fact_fraud_alert": ["case_id", "alert_id"],
    "fact_investigation_note": ["case_id", "note_id"],
    "fact_case_party_summary": ["case_id", "party_type", "role"],
    "investigation_context": ["case_id"],
}
EXPECTED_USAGE_RESTRICTIONS = {
    model: "ai_allowed" if model == "investigation_context" else "internal_only"
    for model in GOLD_MODELS
}
LEGACY_HASHED_COLUMNS = {
    "case_key", "merchant_key", "channel_key", "currency_key", "dispute_reason_key",
    "case_transaction_key", "authorization_attempt_key", "dispute_key", "chargeback_key",
    "fraud_alert_key", "investigation_note_key", "case_party_summary_key",
}
MODEL_DIR = gold_model_dir()
CONTRACTS = {
    contract["model"]: contract
    for path in MODEL_DIR.glob("*.yml")
    for contract in [yaml.safe_load(path.read_text(encoding="utf-8"))]
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


tables = {row.tableName for row in spark.sql(f"SHOW TABLES IN {CATALOG}.gold").collect()}
require(GOLD_MODELS <= tables, f"Missing Gold models: {sorted(GOLD_MODELS - tables)}")
require(set(CONTRACTS) == GOLD_MODELS, "Gold YAML contract inventory does not match physical models")
require({model: contract["primary_key"] for model, contract in CONTRACTS.items()} == EXPECTED_PRIMARY_KEYS, "Gold natural grains do not match EXPECTED_PRIMARY_KEYS")

identities = set()
for model in sorted(GOLD_MODELS):
    df = spark.read.table(f"{CATALOG}.gold.{model}")
    contract = CONTRACTS[model]
    expected_types = {column["name"]: column["physical_type"] for column in contract["columns"]}
    actual_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    require(actual_types == expected_types, f"{model} physical columns/types do not match YAML contract: actual={actual_types}, expected={expected_types}")
    require(STANDARD_METADATA_COLUMNS <= set(df.columns), f"{model} has incomplete metadata")
    require(not (set(df.columns) & FORBIDDEN_AI_COLUMNS), f"{model} exposes forbidden AI columns")
    require(not (set(df.columns) & LEGACY_HASHED_COLUMNS), f"{model} contains legacy hashed columns")
    expected_restriction = EXPECTED_USAGE_RESTRICTIONS[model]
    restriction_column = next(column for column in contract["columns"] if column["name"] == "usage_restrictions")
    require(contract["ai_access"]["classification"] == expected_restriction, f"{model} YAML AI classification does not match policy")
    require(restriction_column["allowed_values"] == [expected_restriction], f"{model} YAML usage_restrictions values do not match policy")
    require(df.filter(F.col("usage_restrictions") != expected_restriction).isEmpty(), f"{model} has usage_restrictions other than {expected_restriction}")

    key_columns = EXPECTED_PRIMARY_KEYS[model]
    null_key = reduce(lambda left, right: left | right, (F.col(column).isNull() for column in key_columns))
    require(df.filter(null_key).isEmpty(), f"{model} has null natural-grain values")
    require(df.groupBy(*key_columns).count().filter(F.col("count") > 1).isEmpty(), f"{model} natural grain is not unique: {key_columns}")
    if df.isEmpty():
        require(model in OPTIONAL_FACT_MODELS, f"{model} must not be empty")
        print(f"{model}: 0 rows (optional fact)")
    else:
        identity_rows = df.select("pipeline_run_id", "batch_id").distinct().limit(2).collect()
        require(len(identity_rows) == 1, f"{model} must have one Gold batch/run identity")
        identities.add((identity_rows[0]["pipeline_run_id"], identity_rows[0]["batch_id"]))
        print(f"{model}: {df.count()} rows")
require(len(identities) == 1, f"Gold models have inconsistent snapshots: {identities}")

unknown_members = {
    "dim_date": ("date_key", 0),
    "dim_merchant": ("merchant_id", "UNKNOWN"),
    "dim_channel": ("channel_code", "UNKNOWN"),
    "dim_dispute_reason": ("reason_code", "UNKNOWN"),
    "dim_currency": ("currency_code", "UNKNOWN"),
}
for model, (key_column, unknown_value) in unknown_members.items():
    contract_column = next(column for column in CONTRACTS[model]["columns"] if column["name"] == key_column)
    require("UNKNOWN" in contract_column["description"], f"{model}.{key_column} does not document its UNKNOWN member")
    require(spark.read.table(f"{CATALOG}.gold.{model}").filter(F.col(key_column) == unknown_value).count() == 1, f"{model} must contain exactly one documented UNKNOWN member")

for model, contract in CONTRACTS.items():
    source = spark.read.table(f"{CATALOG}.gold.{model}")
    for relationship in contract["relationships"]:
        if relationship["cardinality"] == "one_to_many":
            continue
        target_name = relationship["target_model"].removeprefix("gold.")
        target = spark.read.table(f"{CATALOG}.gold.{target_name}")
        join_condition = reduce(
            lambda left, right: left & right,
            (F.col(f"source.{join['from']}") == F.col(f"target.{join['to']}") for join in relationship["join_columns"]),
        )
        unresolved = source.alias("source").join(target.alias("target"), join_condition, "left_anti").count()
        require(unresolved == 0, f"{model} has unresolved business-key relationship to {target_name}")

cases = spark.read.table(f"{CATALOG}.gold.dim_case")
context = spark.read.table(f"{CATALOG}.gold.investigation_context")
require(cases.select("case_id").distinct().count() == cases.count(), "dim_case case_id values must be unique")
require(context.select("case_id").distinct().count() == context.count(), "investigation_context must have one row per case_id")
require(context.count() == cases.count(), "context rows must reconcile to dim_case")
require(context.filter("context_version <> '2.0.0' OR masking_status NOT IN ('masked', 'partial')").isEmpty(), "invalid context version or masking status")

expected_chargebacks = (spark.read.table(f"{CATALOG}.silver.chargebacks")
    .join(spark.read.table(f"{CATALOG}.gold.fact_dispute").select("dispute_id"), "dispute_id")
    .count())
actual_chargebacks = spark.read.table(f"{CATALOG}.gold.fact_chargeback").count()
require(actual_chargebacks == expected_chargebacks, "fact_chargeback does not reconcile to case-scoped disputes")

print("M3 Gold validation passed: contracts, natural grains, metadata, AI policy, UNKNOWN members, context, and business-key referential integrity")
