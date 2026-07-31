# Databricks notebook source
"""Deploy authenticated Consumer metric views and their broker grants."""

from pathlib import Path

import yaml
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from pipeline.gold.gold_common import catalog_widget


TARGET_SCHEMA = "consumer_metrics"
DEFINITIONS = [
    ("mv_consumer_accounts.yaml", "mv_consumer_accounts"),
    ("mv_consumer_cards.yaml", "mv_consumer_cards"),
    ("mv_consumer_transactions.yaml", "mv_consumer_transactions"),
    ("mv_consumer_disputes.yaml", "mv_consumer_disputes"),
]
BROKER_TABLES = [
    "dim_consumer_account",
    "dim_consumer_card",
    "fact_consumer_transaction",
    "fact_consumer_dispute",
]
TECHNICAL_FIELDS = {
    "customer_id",
    "account_id",
    "card_id",
    "pipeline_run_id",
    "batch_id",
    "source_references",
    "usage_restrictions",
}


def find_definition_dir() -> Path:
    """Find the pulled repository's Consumer definition directory."""
    current = Path.cwd().resolve()
    for root in (current, *current.parents):
        candidate = root / "consumer_metrics_view"
        if all((candidate / filename).is_file() for filename, _ in DEFINITIONS):
            return candidate
    raise FileNotFoundError(
        f"Cannot find consumer_metrics_view definitions from {current}"
    )


def quote_principal(principal: str) -> str:
    """Quote a Unity Catalog principal as an SQL identifier."""
    return f"`{principal.replace('`', '``')}`"


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
catalog_widget(dbutils)
try:
    dbutils.widgets.get("consumer_service_principal")
except Exception:
    dbutils.widgets.text("consumer_service_principal", "")

# COMMAND ----------

CATALOG = catalog_widget(dbutils)
SERVICE_PRINCIPAL = dbutils.widgets.get("consumer_service_principal").strip()
if not SERVICE_PRINCIPAL:
    raise ValueError(
        "consumer_service_principal is required; do not grant Consumer views "
        "to users, customers, or the LLM"
    )

DEFINITION_DIR = find_definition_dir()
QUOTED_PRINCIPAL = quote_principal(SERVICE_PRINCIPAL)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

for table_name in BROKER_TABLES:
    spark.sql(
        f"GRANT SELECT ON TABLE {CATALOG}.gold.{table_name} "
        f"TO {QUOTED_PRINCIPAL}"
    )

for position, (filename, view_name) in enumerate(DEFINITIONS, start=1):
    definition = (DEFINITION_DIR / filename).read_text(encoding="utf-8")
    parsed = yaml.safe_load(definition)
    fields = {field["name"] for field in parsed.get("fields", [])}
    parameters = parsed.get("parameters", [])
    if parsed.get("version") != 1.1:
        raise ValueError(f"{filename} must use metric-view YAML version 1.1")
    if not parsed.get("source", "").startswith("g3_catalog.gold."):
        raise ValueError(f"{filename} must declare a g3_catalog.gold source")
    if not parameters or parameters[0] != {
        "name": "scope_customer_id",
        "data_type": "string",
    }:
        raise ValueError(f"{filename} must require scope_customer_id")
    if fields & TECHNICAL_FIELDS:
        raise ValueError(
            f"{filename} exposes technical fields: {sorted(fields & TECHNICAL_FIELDS)}"
        )
    if "materialization" in parsed:
        raise ValueError(
            f"{filename} is parameterized and cannot configure materialization"
        )

    target = f"{CATALOG}.{TARGET_SCHEMA}.{view_name}"
    catalog_definition = definition.replace("g3_catalog.", f"{CATALOG}.")
    print(f"[{position}/{len(DEFINITIONS)}] Creating or replacing {target}")
    spark.sql(
        f"CREATE OR REPLACE VIEW {target}\n"
        "WITH METRICS LANGUAGE YAML AS\n"
        "$$\n"
        f"{catalog_definition.rstrip()}\n"
        "$$"
    )
    spark.sql(
        f"ALTER VIEW {target} SET TBLPROPERTIES "
        "('access_classification' = 'customer_facing')"
    )
    spark.sql(f"GRANT SELECT ON VIEW {target} TO {QUOTED_PRINCIPAL}")

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {QUOTED_PRINCIPAL}")
spark.sql(
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{TARGET_SCHEMA} "
    f"TO {QUOTED_PRINCIPAL}"
)
spark.sql(
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO {QUOTED_PRINCIPAL}"
)
print(
    f"Created {len(DEFINITIONS)} customer_facing metric views in "
    f"{CATALOG}.{TARGET_SCHEMA} for {SERVICE_PRINCIPAL}"
)
