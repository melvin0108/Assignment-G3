"""Aggregate Bronze table configs without owning table-specific values."""

import importlib
import sys

_TABLE_NAMES = (
    "bronze_accounts",
    "bronze_auth_attempts",
    "bronze_branches",
    "bronze_cards",
    "bronze_case_parties",
    "bronze_case_status_types",
    "bronze_case_transactions",
    "bronze_channels",
    "bronze_chargebacks",
    "bronze_countries",
    "bronze_currencies",
    "bronze_customer_contact_logs",
    "bronze_customers",
    "bronze_date_dim",
    "bronze_defects_manifest",
    "bronze_dispute_reason_codes",
    "bronze_disputes",
    "bronze_employees",
    "bronze_fraud_alerts",
    "bronze_fraud_types",
    "bronze_investigation_cases",
    "bronze_investigation_notes",
    "bronze_merchant_categories",
    "bronze_merchants",
    "bronze_transaction_devices",
    "bronze_transactions",
)


def _get_dbutils():
    """Locate dbutils instance if running inside Databricks."""
    main_mod = sys.modules.get("__main__")
    if main_mod and hasattr(main_mod, "dbutils"):
        return getattr(main_mod, "dbutils")
    try:
        import builtins
        if hasattr(builtins, "dbutils"):
            return getattr(builtins, "dbutils")
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None


def _import_bronze_module(module_name: str):
    """Import a bronze notebook module using dbutils.import_notebook when available."""
    module_path = f"pipeline.bronze.{module_name}"
    dbutils = _get_dbutils()
    if dbutils is not None and hasattr(dbutils, "import_notebook"):
        return dbutils.import_notebook(module_path)
    return importlib.import_module(module_path)


_TABLE_MODULES = tuple(_import_bronze_module(name) for name in _TABLE_NAMES)

ALL_TABLE_CONFIGS = {
    module.TABLE_NAME: (module.SOURCE_COLUMNS, module.RECORD_ID_COLUMNS)
    for module in _TABLE_MODULES
}

if len(ALL_TABLE_CONFIGS) != len(_TABLE_MODULES):
    raise ValueError("Bronze table names must be unique")
