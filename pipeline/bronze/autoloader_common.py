"""Shared Databricks Auto Loader implementation for Bronze table notebooks."""

import json
import logging
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from pipeline.bronze.schema_evolution import (
    compare_headers,
    evolve_known_columns,
    parse_csv_header,
    run_with_schema_retry,
)


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
LOGGER = logging.getLogger("g3.schema_evolution")


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

BRONZE_METADATA_COLUMNS = {
    "_source_file",
    "_source_file_mod_time",
    "_ingest_ts",
    "_run_id",
    "_batch_id",
    "_source_record_id",
    "_record_hash",
    "_rescued_data",
    "_corrupt_record",
}


def _warning(event_type, table_name, **context):
    """Emit one machine-readable schema evolution warning."""
    payload = {
        "event": "SCHEMA_EVOLUTION",
        "event_type": event_type,
        "table": table_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **context,
    }
    LOGGER.warning(json.dumps(payload, sort_keys=True, default=str))


def _batch_id(file_name):
    match = re.search(r"(\d+)\.csv$", file_name)
    return int(match.group(1)) if match else None


def _new_csv_files(source_path, target):
    """List source CSVs not yet represented in the Bronze target."""
    processed = set()
    if spark.catalog.tableExists(target):
        processed = {
            row["_source_file"]
            for row in spark.read.table(target).select("_source_file").distinct().collect()
        }

    files = []
    for file_info in dbutils.fs.ls(source_path):
        file_name = file_info.name.rstrip("/")
        if file_info.path.lower().endswith(".csv") and file_name not in processed:
            files.append({"name": file_name, "path": file_info.path})
    return sorted(files, key=lambda item: (_batch_id(item["name"]) or -1, item["path"]))


def _known_bronze_columns(target, contract_columns):
    if not spark.catalog.tableExists(target):
        return list(contract_columns)
    return [
        field.name
        for field in spark.table(target).schema.fields
        if field.name not in BRONZE_METADATA_COLUMNS
    ]


def _inspect_headers(table_name, target, contract_columns, pending_files):
    """Log additive and missing fields for each new CSV before ingestion."""
    known_columns = _known_bronze_columns(target, contract_columns)
    inspected = []
    for source_file in pending_files:
        header = parse_csv_header(dbutils.fs.head(source_file["path"], 65536))
        comparison = compare_headers(known_columns, header)
        context = {
            "batch_id": _batch_id(source_file["name"]),
            "file_path": source_file["path"],
        }
        if comparison.added_columns:
            _warning(
                "added_columns",
                table_name,
                columns=list(comparison.added_columns),
                **context,
            )
        if comparison.missing_columns:
            _warning(
                "missing_columns",
                table_name,
                columns=list(comparison.missing_columns),
                **context,
            )
        inspected.append({**source_file, "header": header})
        known_columns = evolve_known_columns(known_columns, header)
    return inspected


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

    pending_files = _new_csv_files(source_path, target)
    inspected_files = _inspect_headers(
        table_name, target, source_cols, pending_files
    )
    hinted_columns = [
        *(f"`{column}` STRING" for column in source_cols),
        "`_corrupt_record` STRING",
    ]
    schema_hints = ", ".join(hinted_columns)

    def run_stream_once():
        raw_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{state_root}/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("cloudFiles.inferColumnTypes", "false")
            .option("rescuedDataColumn", "_rescued_data")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("mode", "PERMISSIVE")
            .option("header", "true")
            .option("multiLine", "true")
            .load(source_path)
        )

        parser_metadata_columns = {"_rescued_data", "_corrupt_record"}
        data_columns = [
            column for column in raw_df.columns
            if column not in parser_metadata_columns
        ]
        batch_id = F.regexp_extract(
            F.col("_metadata.file_name"), r"(\d+)\.csv$", 1
        ).cast("long")
        record_id = F.concat_ws(
            "|",
            *[F.coalesce(F.col(column), F.lit("<NULL>")) for column in record_id_cols],
        )
        raw_json = F.to_json(
            F.struct(
                *[F.col(column) for column in data_columns],
                F.col("_corrupt_record"),
            ),
            {"ignoreNullFields": "false"},
        )
        bronze_df = raw_df.select(
            *[F.col(column).cast("string").alias(column) for column in data_columns],
            F.col("_metadata.file_name").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_mod_time"),
            F.current_timestamp().alias("_ingest_ts"),
            F.concat(F.lit("RUN-"), F.lpad(batch_id.cast("string"), 2, "0")).alias("_run_id"),
            batch_id.alias("_batch_id"),
            record_id.alias("_source_record_id"),
            F.sha2(raw_json, 256).alias("_record_hash"),
            F.col("_rescued_data").cast("string").alias("_rescued_data"),
            F.col("_corrupt_record").cast("string").alias("_corrupt_record"),
        )

        query = (
            bronze_df.writeStream.format("delta")
            .option("checkpointLocation", f"{state_root}/checkpoint")
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()

    def log_retry(attempt, _exc):
        _warning(
            "schema_restart",
            table_name,
            attempt=attempt,
            reason="Auto Loader updated its schema log after detecting new columns",
        )

    run_with_schema_retry(
        run_stream_once,
        max_attempts=max(2, len(inspected_files) + 1),
        on_retry=log_retry,
    )

    if inspected_files and spark.catalog.tableExists(target):
        paths_by_name = {item["name"]: item["path"] for item in inspected_files}
        rescued_counts = (
            spark.read.table(target)
            .filter(F.col("_source_file").isin(list(paths_by_name)))
            .filter(F.col("_rescued_data").isNotNull())
            .groupBy("_source_file", "_batch_id")
            .count()
            .collect()
        )
        for row in rescued_counts:
            _warning(
                "rescued_data_rows",
                table_name,
                file_path=paths_by_name.get(row["_source_file"], row["_source_file"]),
                batch_id=row["_batch_id"],
                rescued_rows=row["count"],
            )
        corrupt_counts = (
            spark.read.table(target)
            .filter(F.col("_source_file").isin(list(paths_by_name)))
            .filter(F.col("_corrupt_record").isNotNull())
            .groupBy("_source_file", "_batch_id")
            .count()
            .collect()
        )
        for row in corrupt_counts:
            _warning(
                "malformed_csv_rows",
                table_name,
                file_path=paths_by_name.get(row["_source_file"], row["_source_file"]),
                batch_id=row["_batch_id"],
                corrupt_rows=row["count"],
            )

    print(f"Auto Loader completed for {target}")
    spark.sql(
        f"SELECT _batch_id, COUNT(*) AS rows FROM {target} "
        "GROUP BY _batch_id ORDER BY _batch_id"
    ).show(truncate=False)
