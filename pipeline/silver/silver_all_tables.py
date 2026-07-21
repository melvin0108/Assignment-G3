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
    "silver_date_dim",
    "silver_defects_manifest",
    "silver_countries",
    "silver_currencies",
    "silver_branches",
    "silver_channels",
    "silver_merchant_categories",
    "silver_dispute_reason_codes",
    "silver_fraud_types",
    "silver_case_status_types",
    "silver_customers",
    "silver_employees",
    "silver_accounts",
    "silver_cards",
    "silver_merchants",
    "silver_masking_policies",
    "silver_metadata_lineage",
    "silver_transactions",
    "silver_auth_attempts",
    "silver_transaction_devices",
    "silver_disputes",
    "silver_chargebacks",
    "silver_fraud_alerts",
    "silver_investigation_cases",
    "silver_investigation_notes",
    "silver_case_transactions",
    "silver_case_parties",
    "silver_customer_contact_logs",
]

for position, notebook_name in enumerate(SILVER_NOTEBOOKS, start=1):
    print(f"[{position}/{len(SILVER_NOTEBOOKS)}] Starting silver.{notebook_name}")
    dbutils.notebook.run(f"./{notebook_name}", 0, {"catalog": CATALOG})

print(f"Completed Silver transformations for all {len(SILVER_NOTEBOOKS)} notebooks")
