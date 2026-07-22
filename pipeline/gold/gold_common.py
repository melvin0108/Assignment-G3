"""Shared, testable contracts for the Gold dimensional mart.

PySpark imports stay inside Spark-facing functions so local contract tests can
run without a Databricks runtime.
"""

from __future__ import annotations

from pathlib import Path


GOLD_MODELS = {
    "dim_date",
    "dim_case",
    "dim_merchant",
    "dim_channel",
    "dim_dispute_reason",
    "dim_currency",
    "fact_case_transaction",
    "fact_authorization_attempt",
    "fact_dispute",
    "fact_chargeback",
    "fact_fraud_alert",
    "fact_investigation_note",
    "fact_case_party_summary",
    "investigation_context",
}

STANDARD_METADATA_COLUMNS = {
    "pipeline_run_id",
    "batch_id",
    "last_refreshed_at",
    "quality_status",
    "warning_flags",
    "source_references",
    "usage_restrictions",
}

USAGE_RESTRICTIONS = "internal_only"
AI_ALLOWED_RESTRICTIONS = "ai_allowed"
FORBIDDEN_AI_COLUMNS = {
    "customer_id", "employee_id", "owner_employee_id", "author_employee_id",
    "account_id", "card_id", "party_id", "device_id", "ip_address", "pan",
    "phone", "email", "address", "tax_id", "contact_log_id",
}

SILVER_INPUTS = (
    "date_dim", "investigation_cases", "merchants", "merchant_categories",
    "channels", "dispute_reason_codes", "currencies", "transactions",
    "auth_attempts", "disputes", "chargebacks", "fraud_alerts",
    "investigation_notes", "case_transactions", "case_parties",
)


def gold_model_dir(start: Path | None = None) -> Path:
    """Find the co-versioned Gold YAML contracts from a notebook directory."""
    current = (start or Path.cwd()).resolve()
    for root in (current, *current.parents):
        candidate = root / "docs" / "models" / "gold"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(f"Cannot find docs/models/gold from {current}")


def assert_no_forbidden_columns(columns: list[str] | set[str] | tuple[str, ...]) -> None:
    """Reject a Gold projection that would expose a forbidden AI attribute."""
    leaked = sorted(set(columns) & FORBIDDEN_AI_COLUMNS)
    if leaked:
        raise ValueError("Gold projection contains forbidden AI columns: " + ", ".join(leaked))


def catalog_widget(dbutils) -> str:
    """Create or reuse the standard catalog widget."""
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


def matching_silver_snapshot(spark, catalog: str):
    """Return the single batch/run identity shared by all Gold Silver inputs."""
    identities = {}
    for table_name in SILVER_INPUTS:
        rows = (spark.read.table(f"{catalog}.silver.{table_name}")
            .select("_batch_id", "_run_id").distinct().limit(2).collect())
        if len(rows) != 1 or rows[0]["_batch_id"] is None or rows[0]["_run_id"] is None:
            raise ValueError(f"Silver input {table_name} must contain exactly one batch/run identity")
        identities[table_name] = (rows[0]["_batch_id"], rows[0]["_run_id"])
    if len(set(identities.values())) != 1:
        details = ", ".join(f"{name}={identity}" for name, identity in sorted(identities.items()))
        raise ValueError("Gold requires one matching Silver snapshot: " + details)
    return next(iter(identities.values()))


def add_standard_metadata(df, pipeline_run_id: str, batch_id: int, quality_status_col, warning_flags_col, usage_restrictions: str = USAGE_RESTRICTIONS):
    """Attach the uniform Gold metadata contract to a Spark DataFrame."""
    from pyspark.sql import functions as F

    return (df
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("last_refreshed_at", F.current_timestamp())
        .withColumn("quality_status", quality_status_col)
        .withColumn("warning_flags", warning_flags_col)
        .withColumn("usage_restrictions", F.lit(usage_restrictions)))


def write_gold_table(df, catalog: str, table_name: str) -> None:
    """Safely overwrite one documented Gold Delta model."""
    if table_name not in GOLD_MODELS:
        raise ValueError(f"Undeclared Gold model: {table_name}")
    assert_no_forbidden_columns(df.columns)
    required = STANDARD_METADATA_COLUMNS - set(df.columns)
    if required:
        raise ValueError(f"{table_name} is missing Gold metadata columns: {sorted(required)}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.gold.{table_name}"
    )
