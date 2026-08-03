# Databricks notebook source
"""End-to-end schema-evolution evidence using production Bronze/Silver helpers.

Run this notebook only against ``g3_test``. It recreates and retains isolated
``schema_evolution_test`` artifacts so the output can be used as assignment
evidence without changing any production source table.
"""

# COMMAND ----------

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

try:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ModuleNotFoundError as exc:
    # Keep local unittest discovery healthy while making the runtime boundary
    # explicit: this module is executable only as a Databricks notebook.
    import unittest

    raise unittest.SkipTest(
        "Databricks-only schema-evolution integration test"
    ) from exc

try:
    dbutils
except NameError as exc:
    import unittest

    raise unittest.SkipTest(
        "Databricks-only schema-evolution integration test"
    ) from exc

from pipeline.bronze import autoloader_common as bronze_loader
from pipeline.silver.type_cast import (
    TypeCastRule,
    apply_type_casts,
    type_cast_quarantine_rows,
)
from tests.databricks.schema_evolution_fixtures import (
    CONTRACT_COLUMNS,
    FIXTURES,
    RECORD_ID_COLUMNS,
    TEST_TABLE,
)


# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    dbutils.widgets.dropdown("catalog", "g3_test", ["g3_test"])
    catalog = dbutils.widgets.get("catalog")

if catalog != "g3_test":
    raise ValueError(
        "Schema-evolution integration tests are isolated to catalog g3_test; "
        f"received catalog={catalog!r}"
    )

SOURCE_PATH = f"/Volumes/{catalog}/bronze/raw_data/{TEST_TABLE}"
STATE_PATH = f"/Volumes/{catalog}/bronze/autoloader_state/{TEST_TABLE}"
BRONZE_TABLE = f"{catalog}.bronze.{TEST_TABLE}"
QUARANTINE_TABLE = f"{catalog}.silver.{TEST_TABLE}_quarantine"
RESULTS_TABLE = f"{catalog}.gov.{TEST_TABLE}_results"
TEST_RUN_ID = "SCHEMA-EVOLUTION-TEST"

RESULT_SCHEMA = StructType(
    [
        StructField("scenario", StringType(), False),
        StructField("status", StringType(), False),
        StructField("expected", StringType(), False),
        StructField("actual", StringType(), False),
        StructField("tested_at", TimestampType(), False),
    ]
)


# COMMAND ----------

class WarningCapture(logging.Handler):
    """Capture machine-readable schema warnings while preserving normal logging."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages = []

    def emit(self, record):
        message = record.getMessage()
        self.messages.append(message)
        print(f"CAPTURED_SCHEMA_WARNING: {message}")

    def events(self):
        parsed = []
        for message in self.messages:
            try:
                payload = json.loads(message)
            except (TypeError, json.JSONDecodeError):
                continue
            if payload.get("event") == "SCHEMA_EVOLUTION":
                parsed.append(payload)
        return parsed


results = []
warning_capture = WarningCapture()
schema_logger = logging.getLogger("g3.schema_evolution")
previous_logger_level = schema_logger.level
schema_logger.addHandler(warning_capture)
schema_logger.setLevel(logging.WARNING)


def printable(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def record_check(scenario, passed, expected, actual):
    status = "PASS" if passed else "FAIL"
    expected_text = printable(expected)
    actual_text = printable(actual)
    results.append(
        {
            "scenario": scenario,
            "status": status,
            "expected": expected_text,
            "actual": actual_text,
            "tested_at": datetime.now(timezone.utc),
        }
    )
    print(
        f"{status}: {scenario}\n"
        f"  expected: {expected_text}\n"
        f"  actual:   {actual_text}"
    )


def warning_events(event_type=None, batch_id=None):
    events = warning_capture.events()
    if event_type is not None:
        events = [
            event for event in events
            if event.get("event_type") == event_type
        ]
    if batch_id is not None:
        events = [
            event for event in events
            if event.get("batch_id") == batch_id
        ]
    return events


def row_for(record_id):
    rows = (
        spark.read.table(BRONZE_TABLE)
        .filter(F.col("id") == record_id)
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def row_for_batch(batch_id):
    rows = (
        spark.read.table(BRONZE_TABLE)
        .filter(F.col("_batch_id") == batch_id)
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def persist_results():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gov")
    result_df = spark.createDataFrame(results, RESULT_SCHEMA)
    (
        result_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(RESULTS_TABLE)
    )
    print(f"Persisted {len(results)} checks to {RESULTS_TABLE}")
    display(result_df.orderBy("scenario"))


# COMMAND ----------

print("Resetting the isolated schema-evolution test namespace...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gov")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.autoloader_state")

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {RESULTS_TABLE}")
dbutils.fs.rm(SOURCE_PATH, recurse=True)
dbutils.fs.rm(STATE_PATH, recurse=True)
dbutils.fs.mkdirs(SOURCE_PATH)

fatal_error = None

try:
    # Each file is published immediately before ingestion so every assertion
    # observes the same incremental behavior as a real numbered source batch.
    for fixture in FIXTURES:
        fixture_path = f"{SOURCE_PATH}/{fixture.file_name}"
        print(
            f"\n=== Scenario {fixture.scenario}: "
            f"publishing {fixture.file_name} ==="
        )
        dbutils.fs.put(fixture_path, fixture.csv_text, overwrite=False)
        bronze_loader.ingest_table(
            TEST_TABLE,
            list(CONTRACT_COLUMNS),
            list(RECORD_ID_COLUMNS),
        )

        if fixture.scenario == "baseline":
            baseline = row_for("SE-001")
            record_check(
                "baseline row ingested",
                baseline is not None,
                "SE-001 exists",
                baseline["id"] if baseline is not None else None,
            )

        elif fixture.scenario == "added_column":
            schema = {
                field.name: field.dataType.simpleString().lower()
                for field in spark.read.table(BRONZE_TABLE).schema.fields
            }
            historical = row_for("SE-001")
            current = row_for("SE-002")
            added_events = warning_events("added_columns", batch_id=2)
            restart_events = warning_events("schema_restart")
            record_check(
                "added column becomes Bronze STRING",
                schema.get("risk_score") == "string",
                {"risk_score": "string"},
                {"risk_score": schema.get("risk_score")},
            )
            record_check(
                "historical row receives NULL for added column",
                historical is not None and historical["risk_score"] is None,
                None,
                historical["risk_score"] if historical is not None else "missing row",
            )
            record_check(
                "new row retains added-column value",
                current is not None and current["risk_score"] == "0.85",
                "0.85",
                current["risk_score"] if current is not None else "missing row",
            )
            record_check(
                "added-column warning contains batch and column",
                any(
                    "risk_score" in event.get("columns", [])
                    for event in added_events
                ),
                {"event_type": "added_columns", "batch_id": 2, "column": "risk_score"},
                added_events,
            )
            record_check(
                "Auto Loader schema restart is reported",
                bool(restart_events),
                "at least one schema_restart event",
                restart_events,
            )

        elif fixture.scenario == "missing_column":
            current = row_for("SE-003")
            missing_events = warning_events("missing_columns", batch_id=3)
            record_check(
                "missing column is retained as NULL",
                current is not None and current["currency"] is None,
                None,
                current["currency"] if current is not None else "missing row",
            )
            record_check(
                "missing-column warning contains batch and column",
                any(
                    "currency" in event.get("columns", [])
                    for event in missing_events
                ),
                {"event_type": "missing_columns", "batch_id": 3, "column": "currency"},
                missing_events,
            )

        elif fixture.scenario == "reordered_columns":
            current = row_for("SE-004")
            reordered_drift = [
                event
                for event in warning_capture.events()
                if event.get("batch_id") == 4
                and event.get("event_type") in {"added_columns", "missing_columns"}
            ]
            actual_values = (
                {
                    "id": current["id"],
                    "amount": current["amount"],
                    "currency": current["currency"],
                    "risk_score": current["risk_score"],
                }
                if current is not None
                else None
            )
            record_check(
                "reordered header maps values by column name",
                actual_values
                == {
                    "id": "SE-004",
                    "amount": "400.00",
                    "currency": "THB",
                    "risk_score": "0.65",
                },
                {
                    "id": "SE-004",
                    "amount": "400.00",
                    "currency": "THB",
                    "risk_score": "0.65",
                },
                actual_values,
            )
            record_check(
                "reordering does not emit schema drift",
                not reordered_drift,
                [],
                reordered_drift,
            )

        elif fixture.scenario == "malformed_csv":
            current = row_for_batch(5)
            corrupt_events = warning_events("malformed_csv_rows", batch_id=5)
            corrupt_record = (
                current["_corrupt_record"] if current is not None else None
            )
            rescued_data = (
                current["_rescued_data"] if current is not None else None
            )
            record_check(
                "malformed CSV payload uses corrupt-record channel",
                corrupt_record is not None
                and "SE-005" in corrupt_record
                and rescued_data is None,
                {
                    "_corrupt_record": "contains SE-005",
                    "_rescued_data": None,
                },
                {
                    "_corrupt_record": corrupt_record,
                    "_rescued_data": rescued_data,
                },
            )
            record_check(
                "malformed-row warning contains corrupt count",
                any(event.get("corrupt_rows", 0) >= 1 for event in corrupt_events),
                {
                    "event_type": "malformed_csv_rows",
                    "batch_id": 5,
                    "corrupt_rows": ">=1",
                },
                corrupt_events,
            )

        elif fixture.scenario == "invalid_typed_value":
            current = row_for("SE-006")
            record_check(
                "invalid typed value remains raw Bronze text",
                current is not None and current["amount"] == "not-a-number",
                "not-a-number",
                current["amount"] if current is not None else "missing row",
            )

    # Bronze-wide invariants after every schema scenario has been ingested.
    bronze_schema = spark.read.table(BRONZE_TABLE).schema
    data_types = {
        field.name: field.dataType.simpleString().lower()
        for field in bronze_schema.fields
        if field.name not in bronze_loader.BRONZE_METADATA_COLUMNS
    }
    non_string_fields = {
        name: data_type
        for name, data_type in data_types.items()
        if data_type != "string"
    }
    record_check(
        "all Bronze source and evolved columns remain STRING",
        not non_string_fields,
        "all STRING",
        data_types,
    )

    metadata_names = {field.name for field in bronze_schema.fields}
    missing_metadata = sorted(
        bronze_loader.BRONZE_METADATA_COLUMNS - metadata_names
    )
    record_check(
        "Bronze metadata columns remain present",
        not missing_metadata,
        [],
        missing_metadata,
    )

    # Use the production Silver helper to prove scalar type changes are isolated.
    amount_rule = TypeCastRule(
        "amount",
        "amount_typed",
        "DECIMAL(12,2)",
        "DQ-TXN-AMOUNT-TYPE",
    )
    invalid_source = (
        spark.read.table(BRONZE_TABLE)
        .filter(F.col("id") == "SE-006")
    )
    typed_invalid = apply_type_casts(invalid_source, [amount_rule])
    typed_row = typed_invalid.select("amount", "amount_typed").collect()[0]
    record_check(
        "Silver try_cast returns NULL for invalid amount",
        typed_row["amount"] == "not-a-number"
        and typed_row["amount_typed"] is None,
        {"amount": "not-a-number", "amount_typed": None},
        typed_row.asDict(),
    )

    quarantine_df = type_cast_quarantine_rows(
        typed_invalid,
        [amount_rule],
        TEST_TABLE,
        "id",
        TEST_RUN_ID,
    )
    quarantine_rows = quarantine_df.collect()
    quarantine_actual = (
        {
            "record_key": quarantine_rows[0]["record_key"],
            "rule_id": quarantine_rows[0]["rule_id"],
            "disposition": quarantine_rows[0]["disposition"],
            "failure_reason": quarantine_rows[0]["failure_reason"],
        }
        if quarantine_rows
        else None
    )
    record_check(
        "invalid amount creates one standard quarantine row",
        len(quarantine_rows) == 1
        and quarantine_actual["record_key"] == "SE-006"
        and quarantine_actual["rule_id"] == "DQ-TXN-AMOUNT-TYPE"
        and quarantine_actual["disposition"] == "quarantined"
        and "not-a-number" in quarantine_actual["failure_reason"],
        {
            "count": 1,
            "record_key": "SE-006",
            "rule_id": "DQ-TXN-AMOUNT-TYPE",
            "disposition": "quarantined",
            "raw_value": "not-a-number",
        },
        {"count": len(quarantine_rows), "row": quarantine_actual},
    )
    (
        quarantine_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )

    type_events = warning_events("type_cast_failed", batch_id=6)
    record_check(
        "type-cast warning retains raw value and record key",
        any(
            event.get("record_key") == "SE-006"
            and event.get("raw_value") == "not-a-number"
            for event in type_events
        ),
        {
            "event_type": "type_cast_failed",
            "batch_id": 6,
            "record_key": "SE-006",
            "raw_value": "not-a-number",
        },
        type_events,
    )

    # Re-running with no new files must be idempotent.
    count_before = spark.read.table(BRONZE_TABLE).count()
    bronze_loader.ingest_table(
        TEST_TABLE,
        list(CONTRACT_COLUMNS),
        list(RECORD_ID_COLUMNS),
    )
    count_after = spark.read.table(BRONZE_TABLE).count()
    record_check(
        "re-running ingestion without new files is idempotent",
        count_after == count_before,
        count_before,
        count_after,
    )

except Exception as exc:
    fatal_error = exc
    record_check(
        "schema-evolution test execution",
        False,
        "notebook completes without an unexpected exception",
        f"{type(exc).__name__}: {exc}",
    )

finally:
    schema_logger.removeHandler(warning_capture)
    schema_logger.setLevel(previous_logger_level)
    persist_results()


# COMMAND ----------

failed_scenarios = [
    result["scenario"]
    for result in results
    if result["status"] == "FAIL"
]

print("\n=== Schema Evolution Integration Test Summary ===")
print(f"PASS checks: {sum(result['status'] == 'PASS' for result in results)}")
print(f"FAIL checks: {len(failed_scenarios)}")
print(f"Bronze evidence: {BRONZE_TABLE}")
print(f"Quarantine evidence: {QUARANTINE_TABLE}")
print(f"Result evidence: {RESULTS_TABLE}")

if fatal_error is not None:
    raise AssertionError(
        "Schema-evolution integration test stopped unexpectedly; "
        f"see {RESULTS_TABLE}"
    ) from fatal_error

if failed_scenarios:
    raise AssertionError(
        "Schema-evolution integration test failed: "
        + ", ".join(failed_scenarios)
    )

print("PASS: all schema-evolution integration scenarios completed")
