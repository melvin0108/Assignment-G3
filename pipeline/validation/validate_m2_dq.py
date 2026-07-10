# Databricks notebook source
# ============================================================================
# Validation: M2 DQ registry + quarantine
# ----------------------------------------------------------------------------
# Run this after:
#   1. Bronze ingestion notebooks, including defects_manifest
#   2. pipeline/dq/01_setup.sql or dq_01_setup.py
#   3. pipeline/dq/02_load_dq_rules.sql or dq_02_load_dq_rules.py
#   4. pipeline/dq/04_failures_all_rules.sql or dq_03_failures_all_rules.py
#
# A failed check raises an exception so the output can be used as executable
# validation evidence, not only a manual inspection screenshot.
# ============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "g3_test"
DQ_RUN_ID = "RUN-20260708-DQ1"


def sql(query):
    return spark.sql(query)


def fail_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        print(f"\nFAIL: {name}")
        df.show(truncate=False)
        raise Exception(f"Validation failed: {name}")
    print(f"PASS: {name}")


def warn_if_rows(name, query):
    df = sql(query)
    rows = df.collect()
    if rows:
        print(f"\nWARN: {name}")
        df.show(truncate=False)
        return
    print(f"PASS: {name}")


def fail_if_zero(name, query):
    value = sql(query).collect()[0][0]
    if value == 0:
        raise Exception(f"Validation failed: {name} returned 0")
    print(f"PASS: {name} = {value}")


print("=== M2 DQ/quarantine validation ===")

fail_if_zero(
    "enabled DQ rules are loaded",
    f"SELECT COUNT(*) FROM {CATALOG}.gov.dq_rules WHERE enabled = true",
)

fail_if_rows(
    "manifest rule IDs exist in DQ registry",
    f"""
    WITH registry AS (
      SELECT DISTINCT rule_id FROM {CATALOG}.gov.dq_rules WHERE enabled = true
    ),
    manifest AS (
      SELECT DISTINCT rule_id FROM {CATALOG}.bronze.defects_manifest
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
      SELECT DISTINCT rule_id FROM {CATALOG}.gov.dq_rules WHERE enabled = true
    ),
    manifest AS (
      SELECT DISTINCT rule_id FROM {CATALOG}.bronze.defects_manifest
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
    FROM {CATALOG}.silver.quarantine_records
    WHERE run_id = '{DQ_RUN_ID}'
    """,
)

fail_if_rows(
    "quarantine rows have required fields populated",
    f"""
    SELECT *
    FROM {CATALOG}.silver.quarantine_records
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

print("\nManifest vs quarantine summary:")
summary = sql(
    f"""
    WITH manifest AS (
      SELECT rule_id, record_key
      FROM {CATALOG}.bronze.defects_manifest
    ),
    quarantine AS (
      SELECT rule_id, record_key
      FROM {CATALOG}.silver.quarantine_records
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
    raise Exception(
        f"Validation failed: manifest/quarantine mismatch "
        f"(missed_keys={missed_total}, extra_keys={extra_total})"
    )

print("\nPASS: M2 DQ/quarantine validation completed with no blocking failures.")
