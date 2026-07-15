# Databricks notebook source
"""Run all Silver transformations in dependency-safe order."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pipeline.silver.snapshot import assert_matching_latest_snapshots


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


# ---------------------------------------------------------------------------
# CATALOG WIDGET
# ---------------------------------------------------------------------------
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

SNAPSHOT_SOURCE_TABLES = [
    "date_dim",
    "defects_manifest",
    "countries",
    "currencies",
    "branches",
    "channels",
    "merchant_categories",
    "dispute_reason_codes",
    "fraud_types",
    "case_status_types",
    "customers",
    "employees",
    "accounts",
    "cards",
    "merchants",
    "transactions",
    "auth_attempts",
    "transaction_devices",
    "disputes",
    "chargebacks",
    "fraud_alerts",
    "investigation_cases",
    "investigation_notes",
    "case_transactions",
    "case_parties",
    "customer_contact_logs",
]

SNAPSHOT_BATCH_ID, SNAPSHOT_RUN_ID = assert_matching_latest_snapshots(
    spark, CATALOG, SNAPSHOT_SOURCE_TABLES
)
print(
    f"Validated shared Bronze snapshot: batch {SNAPSHOT_BATCH_ID}, "
    f"run {SNAPSHOT_RUN_ID}"
)

# COMMAND ----------

# Governance registries run before transaction, authentication, and device
# notebooks, which append device-masking and lineage records to those tables.
SILVER_NOTEBOOKS = [
    "01_silver_date_dim",
    "02_silver_defects_manifest",
    "03_silver_countries",
    "04_silver_currencies",
    "05_silver_branches",
    "06_silver_channels",
    "07_silver_merchant_categories",
    "08_silver_dispute_reason_codes",
    "09_silver_fraud_types",
    "10_silver_case_status_types",
    "11_silver_customers",
    "12_silver_employees",
    "13_silver_accounts",
    "14_silver_cards",
    "15_silver_merchants",
    "16_silver_masking_policies",
    "17_silver_metadata_lineage",
    "18_silver_transactions",
    "19_silver_auth_attempts",
    "20_silver_transaction_devices",
    "21_silver_disputes",
    "22_silver_chargebacks",
    "23_silver_fraud_alerts",
    "24_silver_investigation_cases",
    "25_silver_investigation_notes",
    "26_silver_case_transactions",
    "27_silver_case_parties",
    "28_silver_customer_contact_logs",
]

for position, notebook_name in enumerate(SILVER_NOTEBOOKS, start=1):
    print(f"[{position}/{len(SILVER_NOTEBOOKS)}] Starting silver.{notebook_name}")
    dbutils.notebook.run(f"./{notebook_name}", 0, {"catalog": CATALOG})

print(f"Completed Silver transformations for all {len(SILVER_NOTEBOOKS)} notebooks")
