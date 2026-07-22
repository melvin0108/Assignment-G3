# Databricks notebook source
# ============================================================================
# Validation: M2 DQ registry + quarantine
# ----------------------------------------------------------------------------
# Run this after:
#   1. Bronze ingestion notebooks, including defects_manifest
#   2. pipeline/dq/01_setup.sql or dq_01_setup.py
#   3. pipeline/dq/02_load_dq_rules.sql or dq_02_load_dq_rules.py
#   4. pipeline/dq/04_failures_all_rules.sql or dq_03_failures_all_rules.py
#   5. pipeline/silver/silver_all_tables.py
#
# A failed check raises an exception so the output can be used as executable
# validation evidence, not only a manual inspection screenshot.
# ============================================================================

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pipeline.silver.snapshot import latest_batch_snapshot, snapshot_run_id

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
catalog = dbutils.widgets.get("catalog")
manifest_snapshot = latest_batch_snapshot(
    spark.read.table(f"{catalog}.bronze.defects_manifest")
)
SNAPSHOT_BATCH_ID = manifest_snapshot.select("_batch_id").first()["_batch_id"]
SILVER_RUN_ID = snapshot_run_id(manifest_snapshot)
DQ_RUN_ID = f"{SILVER_RUN_ID}-DQ"

validation_counts = {
    "blocking_checks_passed": 0,
    "warning_checks_clear": 0,
    "warnings_raised": 0,
}


def sql(query):
    return spark.sql(query)


def fail_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        print(f"\nFAIL: {name}")
        df.show(truncate=False)
        raise Exception(f"Validation failed: {name}")
    validation_counts["blocking_checks_passed"] += 1
    print(f"PASS: {name}")


def warn_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        validation_counts["warnings_raised"] += 1
        print(f"\nWARN: {name}")
        df.show(truncate=False)
        return
    validation_counts["warning_checks_clear"] += 1
    print(f"PASS: {name}")


def fail_if_zero(name, query):
    value = sql(query).collect()[0][0]
    if value == 0:
        raise Exception(f"Validation failed: {name} returned 0")
    validation_counts["blocking_checks_passed"] += 1
    print(f"PASS: {name} = {value}")


print("=== M2 DQ/quarantine validation ===")
print(f"INFO: validating Bronze batch {SNAPSHOT_BATCH_ID} with DQ run {DQ_RUN_ID}")

fail_if_zero(
    "enabled DQ rules are loaded",
    f"SELECT COUNT(*) FROM {catalog}.gov.dq_rules WHERE enabled = true",
)

fail_if_rows(
    "manifest rule IDs exist in DQ registry",
    f"""
    WITH registry AS (
      SELECT DISTINCT rule_id FROM {catalog}.gov.dq_rules WHERE enabled = true
    ),
    manifest AS (
      SELECT DISTINCT rule_id FROM {catalog}.bronze.defects_manifest
      WHERE _batch_id = {SNAPSHOT_BATCH_ID}
    )
    SELECT rule_id FROM manifest
    EXCEPT
    SELECT rule_id FROM registry
    """,
)

warn_if_rows(
    "enabled DQ registry rules without manifest seed records",
    f"""
    WITH registry AS (
      SELECT DISTINCT rule_id FROM {catalog}.gov.dq_rules WHERE enabled = true
    ),
    manifest AS (
      SELECT DISTINCT rule_id FROM {catalog}.bronze.defects_manifest
      WHERE _batch_id = {SNAPSHOT_BATCH_ID}
    )
    SELECT rule_id FROM registry
    EXCEPT
    SELECT rule_id FROM manifest
    """,
)

fail_if_zero(
    "quarantine rows for DQ run",
    f"""
    SELECT COUNT(*)
    FROM {catalog}.silver.quarantine_records
    WHERE run_id = '{DQ_RUN_ID}'
    """,
)

fail_if_rows(
    "quarantine rows have required fields populated",
    f"""
    SELECT *
    FROM {catalog}.silver.quarantine_records
    WHERE run_id = '{DQ_RUN_ID}'
      AND (
        source_table IS NULL OR source_table = ''
        OR record_key IS NULL OR record_key = ''
        OR rule_id IS NULL OR rule_id = ''
        OR failure_reason IS NULL OR failure_reason = ''
        OR disposition IS NULL OR disposition = ''
      )
    LIMIT 20
    """,
)

fail_if_rows(
    "quarantine rule IDs exist in DQ registry",
    f"""
    SELECT DISTINCT q.rule_id
    FROM {catalog}.silver.quarantine_records q
    LEFT JOIN {catalog}.gov.dq_rules r ON q.rule_id = r.rule_id AND r.enabled = true
    WHERE q.run_id IN ('{DQ_RUN_ID}', '{SILVER_RUN_ID}')
      AND r.rule_id IS NULL
    """,
)

SILVER_DATA_TABLES = [
    "date_dim", "defects_manifest", "countries", "currencies", "branches",
    "channels", "merchant_categories", "dispute_reason_codes", "fraud_types",
    "case_status_types", "customers", "employees", "accounts", "cards",
    "merchants", "transactions", "auth_attempts", "transaction_devices",
    "disputes", "chargebacks", "fraud_alerts", "investigation_cases",
    "investigation_notes", "case_transactions", "case_parties",
    "customer_contact_logs",
]
for table_name in SILVER_DATA_TABLES:
    fail_if_rows(
        f"silver.{table_name} contains only the current Bronze snapshot",
        f"""
        SELECT _batch_id, _run_id, COUNT(*) AS row_count
        FROM {catalog}.silver.{table_name}
        WHERE _batch_id IS NULL OR _batch_id <> {SNAPSHOT_BATCH_ID}
           OR _run_id IS NULL OR _run_id <> '{SILVER_RUN_ID}'
        GROUP BY _batch_id, _run_id
        LIMIT 20
        """,
    )

QUARANTINED_SOURCE_TABLES = [
    "accounts", "auth_attempts", "cards", "case_parties", "case_transactions",
    "chargebacks", "customer_contact_logs", "customers", "disputes", "employees",
    "fraud_alerts", "investigation_cases", "investigation_notes", "merchants",
    "transaction_devices", "transactions",
]
for table_name in QUARANTINED_SOURCE_TABLES:
    fail_if_rows(
        f"silver.{table_name} excludes quarantined source rows",
        f"""
        SELECT q.rule_id, q.record_key, q.source_record_id
        FROM {catalog}.silver.quarantine_records q
        JOIN {catalog}.silver.{table_name} s
          ON q.source_record_id = s._source_record_id
        WHERE q.run_id IN ('{DQ_RUN_ID}', '{SILVER_RUN_ID}')
          AND q.source_table = '{table_name}'
        LIMIT 20
        """,
    )

TYPE_CAST_TABLES = [
    "date_dim", "currencies", "customers", "accounts", "merchants",
    "transactions", "auth_attempts", "disputes", "chargebacks",
    "fraud_alerts", "investigation_cases", "investigation_notes",
    "case_transactions", "customer_contact_logs",
]
for table_name in TYPE_CAST_TABLES:
    fail_if_rows(
        f"silver.{table_name} excludes type-cast failures",
        f"""
        SELECT q.rule_id, q.record_key, q.source_record_id
        FROM {catalog}.silver.quarantine_records q
        JOIN {catalog}.silver.{table_name} s
          ON q.source_record_id = s._source_record_id
        WHERE q.run_id = '{SILVER_RUN_ID}'
          AND q.source_table = '{table_name}'
          AND q.rule_id LIKE '%-TYPE'
        LIMIT 20
        """,
    )

print("\nManifest vs quarantine summary:")
summary = sql(
    f"""
    WITH manifest AS (
      SELECT rule_id, record_key
      FROM {catalog}.bronze.defects_manifest
      WHERE _batch_id = {SNAPSHOT_BATCH_ID}
    ),
    quarantine AS (
      SELECT rule_id, record_key
      FROM {catalog}.silver.quarantine_records
      WHERE run_id = '{DQ_RUN_ID}'
    ),
    expected AS (
      SELECT rule_id, COUNT(DISTINCT record_key) AS expected_keys
      FROM manifest
      GROUP BY rule_id
    ),
    caught AS (
      SELECT rule_id, COUNT(DISTINCT record_key) AS caught_keys
      FROM quarantine
      GROUP BY rule_id
    ),
    missed AS (
      SELECT m.rule_id, COUNT(DISTINCT m.record_key) AS missed_keys
      FROM manifest m
      LEFT ANTI JOIN quarantine q
        ON m.rule_id = q.rule_id AND m.record_key = q.record_key
      GROUP BY m.rule_id
    ),
    extra AS (
      SELECT q.rule_id, COUNT(DISTINCT q.record_key) AS extra_keys
      FROM quarantine q
      LEFT ANTI JOIN manifest m
        ON m.rule_id = q.rule_id AND m.record_key = q.record_key
      GROUP BY q.rule_id
    )
    SELECT
      e.rule_id,
      e.expected_keys,
      COALESCE(c.caught_keys, 0) AS caught_keys,
      COALESCE(m.missed_keys, 0) AS missed_keys,
      COALESCE(x.extra_keys, 0) AS extra_keys
    FROM expected e
    LEFT JOIN caught c ON e.rule_id = c.rule_id
    LEFT JOIN missed m ON e.rule_id = m.rule_id
    LEFT JOIN extra x ON e.rule_id = x.rule_id
    ORDER BY e.rule_id
    """
)
summary.show(100, truncate=False)

missed_total = summary.selectExpr("sum(missed_keys)").collect()[0][0] or 0
extra_total = summary.selectExpr("sum(extra_keys)").collect()[0][0] or 0

if missed_total > 0 or extra_total > 0:
    print(
        f"\nWARN: manifest/quarantine mismatch "
        f"(missed_keys={missed_total}, extra_keys={extra_total})"
    )
else:
    print("PASS: manifest/quarantine keys match")

print("\nDeliverable 5 rule outcomes:")
rule_outcomes = sql(
    f"""
    WITH enabled_rules AS (
      SELECT rule_id, rule_name, layer, target_table
      FROM {catalog}.gov.dq_rules
      WHERE enabled = true
    ),
    current_failures AS (
      SELECT rule_id
      FROM {catalog}.silver.quarantine_records
      WHERE run_id IN ('{DQ_RUN_ID}', '{SILVER_RUN_ID}')
    )
    SELECT
      r.rule_id,
      r.rule_name,
      r.layer,
      r.target_table,
      COUNT(f.rule_id) AS quarantined_record_count,
      CASE WHEN COUNT(f.rule_id) = 0 THEN 'PASSED' ELSE 'FAILED' END AS outcome
    FROM enabled_rules r
    LEFT JOIN current_failures f ON r.rule_id = f.rule_id
    GROUP BY r.rule_id, r.rule_name, r.layer, r.target_table
    ORDER BY outcome, r.rule_id
    """
)
rule_outcomes.show(100, truncate=False)

rule_rows = rule_outcomes.collect()
passed_rule_count = sum(row["outcome"] == "PASSED" for row in rule_rows)
failed_rule_count = sum(row["outcome"] == "FAILED" for row in rule_rows)

current_quarantine = sql(
    f"""
    SELECT run_id, source_table, source_record_id, record_key, rule_id,
           rule_name, failure_reason, severity, disposition, detected_at
    FROM {catalog}.silver.quarantine_records
    WHERE run_id IN ('{DQ_RUN_ID}', '{SILVER_RUN_ID}')
    """
)
quarantined_record_count = current_quarantine.count()
distinct_source_record_count = (
    current_quarantine
    .select("source_table", "source_record_id")
    .distinct()
    .count()
)

sample_rows = (
    current_quarantine
    .orderBy("rule_id", "source_table", "record_key")
    .limit(10)
    .collect()
)


def json_value(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


evidence = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "catalog": catalog,
    "bronze_batch_id": SNAPSHOT_BATCH_ID,
    "silver_run_id": SILVER_RUN_ID,
    "dq_run_id": DQ_RUN_ID,
    "validation_status": "PASSED",
    "validation_checks": validation_counts,
    "enabled_rule_count": len(rule_rows),
    "passed_rule_count": passed_rule_count,
    "failed_rule_count": failed_rule_count,
    "quarantined_record_count": quarantined_record_count,
    "distinct_quarantined_source_record_count": distinct_source_record_count,
    "manifest_reconciliation": {
        "missed_key_count": missed_total,
        "extra_key_count": extra_total,
        "status": "MATCHED" if missed_total == 0 and extra_total == 0 else "WARNING",
    },
    "rules_with_failures": [
        {
            "rule_id": row["rule_id"],
            "rule_name": row["rule_name"],
            "layer": row["layer"],
            "target_table": row["target_table"],
            "quarantined_record_count": row["quarantined_record_count"],
        }
        for row in rule_rows
        if row["outcome"] == "FAILED"
    ],
    "sample_failed_records": [
        {key: json_value(value) for key, value in row.asDict().items()}
        for row in sample_rows
    ],
}

print("\nD5_EVIDENCE_JSON_START")
print(json.dumps(evidence, indent=2, sort_keys=True))
print("D5_EVIDENCE_JSON_END")

print("\nPASS: M2 DQ/quarantine validation completed with no blocking failures.")

# COMMAND ----------
