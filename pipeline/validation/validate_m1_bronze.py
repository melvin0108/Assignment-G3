# Databricks notebook source
# ============================================================================
# Validation: M1 Bronze ingestion/platform
# ----------------------------------------------------------------------------
# Run this after all Bronze ingestion notebooks.
#
# A failed check raises an exception so the output can be used as executable
# validation evidence, not only a manual inspection screenshot.
# ============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
catalog = dbutils.widgets.get("catalog")

BRONZE_TABLES = [
    "accounts",
    "auth_attempts",
    "branches",
    "cards",
    "case_parties",
    "case_status_types",
    "case_transactions",
    "channels",
    "chargebacks",
    "countries",
    "currencies",
    "customer_contact_logs",
    "customers",
    "date_dim",
    "defects_manifest",
    "dispute_reason_codes",
    "disputes",
    "employees",
    "fraud_alerts",
    "fraud_types",
    "investigation_cases",
    "investigation_notes",
    "merchant_categories",
    "merchants",
    "scd_changes_manifest",
    "transaction_devices",
    "transactions",
]

INGESTED_BRONZE_TABLES = BRONZE_TABLES

BRONZE_METADATA_COLS = [
    "_source_file",
    "_source_file_mod_time",
    "_ingest_ts",
    "_run_id",
    "_batch_id",
    "_source_record_id",
    "_record_hash",
    "_rescued_data",
]


def sql(query):
    return spark.sql(query)


def fail_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        print(f"\nFAIL: {name}")
        df.show(truncate=False)
        raise Exception(f"Validation failed: {name}")
    print(f"PASS: {name}")


def fail_if_zero(name, query):
    value = sql(query).collect()[0][0]
    if value == 0:
        raise Exception(f"Validation failed: {name} returned 0")
    print(f"PASS: {name} = {value}")


def warn_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        print(f"\nWARN: {name}")
        df.show(truncate=False)
        return
    print(f"PASS: {name}")


expected_bronze_values = ",".join([f"('{t}')" for t in BRONZE_TABLES])
expected_ingested_bronze_values = ",".join([f"('{t}')" for t in INGESTED_BRONZE_TABLES])
expected_meta_values = ",".join([f"('{c}')" for c in BRONZE_METADATA_COLS])

print("=== M1 Bronze validation ===")

fail_if_rows(
    "all expected Bronze tables exist",
    f"""
    WITH expected(table_name) AS (VALUES {expected_bronze_values})
    SELECT e.table_name
    FROM expected e
    LEFT JOIN {catalog}.information_schema.tables t
      ON t.table_schema = 'bronze'
     AND t.table_name = e.table_name
    WHERE t.table_name IS NULL
    """,
)

fail_if_rows(
    "all ingested Bronze tables have required metadata columns",
    f"""
    WITH expected_tables(table_name) AS (VALUES {expected_ingested_bronze_values}),
         expected_cols(column_name) AS (VALUES {expected_meta_values})
    SELECT t.table_name, c.column_name AS missing_column
    FROM expected_tables t
    CROSS JOIN expected_cols c
    LEFT JOIN {catalog}.information_schema.columns actual
      ON actual.table_schema = 'bronze'
     AND actual.table_name = t.table_name
     AND actual.column_name = c.column_name
    WHERE actual.column_name IS NULL
    ORDER BY t.table_name, c.column_name
    """,
)

for table_name in ["transactions", "customers", "accounts", "cards", "defects_manifest"]:
    fail_if_zero(
        f"bronze.{table_name} has rows",
        f"SELECT COUNT(*) FROM {catalog}.bronze.{table_name}",
    )

for table_name in INGESTED_BRONZE_TABLES:
    fail_if_rows(
        f"bronze.{table_name} has valid file/batch lineage",
        f"""
        SELECT _source_file, _batch_id, _run_id
        FROM {catalog}.bronze.{table_name}
        WHERE _source_file IS NULL
           OR _batch_id IS NULL
           OR _run_id <> CONCAT('RUN-', LPAD(CAST(_batch_id AS STRING), 2, '0'))
        LIMIT 20
        """,
    )

for table_name in ["transactions", "customers", "accounts", "cards"]:
    warn_if_rows(
        f"bronze.{table_name} duplicate _record_hash sample",
        f"""
        SELECT _record_hash, COUNT(*) AS n
        FROM {catalog}.bronze.{table_name}
        GROUP BY _record_hash
        HAVING COUNT(*) > 1
        LIMIT 20
        """,
    )

fail_if_rows(
    "defects manifest has no null/blank rule_id or record_key",
    f"""
    SELECT *
    FROM {catalog}.bronze.defects_manifest
    WHERE rule_id IS NULL OR rule_id = ''
       OR record_key IS NULL OR record_key = ''
    LIMIT 20
    """,
)

print("\nPASS: M1 Bronze validation completed with no blocking failures.")

# COMMAND ----------


