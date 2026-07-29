# Databricks notebook source
"""Create or replace the consolidated Unity Catalog metric views."""

from pathlib import Path

import yaml
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from pipeline.gold.gold_common import catalog_widget


DEFINITIONS = [
    ("01_case_metrics.yaml", "mv_case_metrics"),
    ("02_transaction_metrics.yaml", "mv_transaction_metrics"),
    ("03_authorization_metrics.yaml", "mv_authorization_metrics"),
    ("04_dispute_metrics.yaml", "mv_dispute_metrics"),
    ("05_chargeback_metrics.yaml", "mv_chargeback_metrics"),
    ("06_fraud_alert_metrics.yaml", "mv_fraud_alert_metrics"),
    ("07_safe_note_metrics.yaml", "mv_safe_note_metrics"),
    ("08_party_metrics.yaml", "mv_party_metrics"),
]


def find_definition_dir() -> Path:
    """Find the pulled repository's metric_view directory."""
    current = Path.cwd().resolve()
    for root in (current, *current.parents):
        candidate = root / "metrics_view"
        if all((candidate / filename).is_file() for filename, _ in DEFINITIONS):
            return candidate
    raise FileNotFoundError(f"Cannot find metric_view definitions from {current}")


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
TARGET_SCHEMA = "metrics"
DEFINITION_DIR = find_definition_dir()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

for position, (filename, view_name) in enumerate(DEFINITIONS, start=1):
    path = DEFINITION_DIR / filename
    definition = path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(definition)
    if parsed.get("version") != 1.1:
        raise ValueError(f"{filename} must use metric-view YAML version 1.1")
    if not parsed.get("source", "").startswith("g3_catalog.gold."):
        raise ValueError(f"{filename} must declare a g3_catalog.gold source")

    catalog_definition = definition.replace("g3_catalog.", f"{CATALOG}.")
    target = f"{CATALOG}.{TARGET_SCHEMA}.{view_name}"
    print(f"[{position}/{len(DEFINITIONS)}] Creating or replacing {target}")
    statement = (
        f"CREATE OR REPLACE VIEW {target}\n"
        "WITH METRICS LANGUAGE YAML AS\n"
        "$$\n"
        f"{catalog_definition.rstrip()}\n"
        "$$"
    )
    spark.sql(statement)

print(f"Created or replaced {len(DEFINITIONS)} metric views in {CATALOG}.{TARGET_SCHEMA}")
