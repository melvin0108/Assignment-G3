"""Shared Databricks Auto Loader implementation for Bronze table notebooks."""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.dbutils import DBUtils


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


TABLE_CONFIG = {
    "accounts": (["account_id", "customer_id", "product_type", "open_date", "status", "currency"], ["account_id"]),
    "auth_attempts": (["attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"], ["attempt_id"]),
    "branches": (["branch_code", "name", "country", "region", "status"], ["branch_code"]),
    "cards": (["card_id", "account_id", "card_type", "pan", "expiry", "status", "effective_at"], ["card_id"]),
    "case_parties": (["case_id", "party_type", "party_id", "role"], ["case_id", "party_type", "party_id"]),
    "case_status_types": (["status_code", "description"], ["status_code"]),
    "case_transactions": (["case_id", "transaction_id", "linked_at"], ["case_id", "transaction_id"]),
    "channels": (["channel_code", "channel_name"], ["channel_code"]),
    "chargebacks": (["chargeback_id", "dispute_id", "scheme", "amount", "stage", "processed_at"], ["chargeback_id"]),
    "countries": (["iso_code", "name", "region"], ["iso_code"]),
    "currencies": (["currency_code", "name", "decimals"], ["currency_code"]),
    "customer_contact_logs": (["contact_id", "customer_id", "direction", "contact_method", "do_not_contact", "contacted_at", "employee_id", "note"], ["contact_id"]),
    "customers": (["customer_id", "first_name", "last_name", "dob", "email", "phone", "address", "tax_id", "created_at", "effective_at"], ["customer_id"]),
    "date_dim": (["date_id", "year", "month", "quarter", "is_weekend"], ["date_id"]),
    "defects_manifest": (["source_table", "record_key", "rule_id", "rule_name", "failure_reason", "severity"], ["source_table", "record_key", "rule_id"]),
    "dispute_reason_codes": (["reason_code", "description"], ["reason_code"]),
    "disputes": (["dispute_id", "transaction_id", "reason_code", "amount", "status", "raised_at"], ["dispute_id"]),
    "employees": (["employee_id", "full_name", "email", "team", "role"], ["employee_id"]),
    "fraud_alerts": (["alert_id", "transaction_id", "rule_name", "score", "triggered_at", "disposition"], ["alert_id"]),
    "fraud_types": (["fraud_type_code", "description", "severity"], ["fraud_type_code"]),
    "investigation_cases": (["case_id", "priority", "status_code", "fraud_type_code", "owner_employee_id", "opened_at", "closed_at", "legal_hold"], ["case_id"]),
    "investigation_notes": (["note_id", "case_id", "author_employee_id", "note_text", "created_at"], ["note_id"]),
    "merchant_categories": (["mcc", "category_name", "category_group"], ["mcc"]),
    "merchants": (["merchant_id", "name", "mcc", "country", "risk_rating", "status", "effective_at"], ["merchant_id"]),
    "scd_changes_manifest": (["source_table", "natural_key", "snapshot", "changed_attribute", "old_value", "new_value", "prior_effective_at", "effective_at"], ["source_table", "natural_key", "snapshot", "changed_attribute"]),
    "transaction_devices": (["device_id", "transaction_id", "device_type", "ip", "geo_country"], ["device_id"]),
    "transactions": (["transaction_id", "account_id", "card_id", "merchant_id", "channel", "amount", "currency", "txn_ts", "status"], ["transaction_id"]),
}


def _catalog_widget():
    """Create the team-standard catalog widget and return its validated value."""
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


def ingest_table(table_name):
    """Ingest all unseen CSV files for one table, then stop (AvailableNow)."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unknown Bronze table: {table_name}")

    catalog = _catalog_widget()
    source_cols, record_id_cols = TABLE_CONFIG[table_name]
    source_path = f"/Volumes/{catalog}/bronze/raw_data/{table_name}"
    state_root = f"/Volumes/{catalog}/bronze/autoloader_state/{table_name}"
    target = f"{catalog}.bronze.{table_name}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.autoloader_state")

    source_schema = StructType([StructField(c, StringType(), True) for c in source_cols])
    raw_df = (
        spark.readStream.format("cloudFiles")
        .schema(source_schema)
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{state_root}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .load(source_path)
    )

    batch_id = F.regexp_extract(F.col("_metadata.file_name"), r"(\d+)\.csv$", 1).cast("long")
    record_id = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in record_id_cols])
    raw_json = F.to_json(F.struct(*[F.col(c) for c in source_cols]), {"ignoreNullFields": "false"})
    bronze_df = raw_df.select(
        *[F.col(c).cast("string").alias(c) for c in source_cols],
        F.col("_metadata.file_name").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_mod_time"),
        F.current_timestamp().alias("_ingest_ts"),
        F.concat(F.lit("RUN-"), F.lpad(batch_id.cast("string"), 2, "0")).alias("_run_id"),
        batch_id.alias("_batch_id"),
        record_id.alias("_source_record_id"),
        F.sha2(raw_json, 256).alias("_record_hash"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
    )

    query = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", f"{state_root}/checkpoint")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    print(f"Auto Loader completed for {target}")
    spark.sql(
        f"SELECT _batch_id, COUNT(*) AS rows FROM {target} "
        "GROUP BY _batch_id ORDER BY _batch_id"
    ).show(truncate=False)
