# Databricks notebook source
"""Run all Silver transformations in dependency-safe order."""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


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

# COMMAND ----------

# Governance registries run before notebooks 16-18, which append their
# device-masking and lineage records to those tables.
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
    "27_silver_masking_policies",
    "28_silver_metadata_lineage",
    "16_silver_transactions",
    "17_silver_auth_attempts",
    "18_silver_transaction_devices",
    "19_silver_disputes",
    "20_silver_chargebacks",
    "21_silver_fraud_alerts",
    "22_silver_investigation_cases",
    "23_silver_investigation_notes",
    "24_silver_case_transactions",
    "25_silver_case_parties",
    "26_silver_customer_contact_logs",
]

for position, notebook_name in enumerate(SILVER_NOTEBOOKS, start=1):
    print(f"[{position}/{len(SILVER_NOTEBOOKS)}] Starting silver.{notebook_name}")
    dbutils.notebook.run(f"./{notebook_name}", 0, {"catalog": CATALOG})

print(f"Completed Silver transformations for all {len(SILVER_NOTEBOOKS)} notebooks")
