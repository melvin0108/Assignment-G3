"""Shared Silver type-contract and cast-failure quarantine helpers."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


LOGGER = logging.getLogger("g3.schema_evolution")
QUARANTINE_SCHEMA_DDL = """
    run_id STRING, source_table STRING, source_record_id STRING, record_key STRING,
    rule_id STRING, rule_name STRING, failure_reason STRING, severity STRING,
    disposition STRING, raw_record STRING, detected_at TIMESTAMP
"""


@dataclass(frozen=True)
class TypeCastRule:
    """One explicit Bronze-string to Silver-type conversion."""

    source_column: str
    target_column: str
    target_type: str
    rule_id: str
    expression: str | None = None


def apply_type_casts(df: DataFrame, rules: Sequence[TypeCastRule]) -> DataFrame:
    """Add typed working columns without replacing the original raw values."""
    typed_df = df
    for rule in rules:
        expression = rule.expression or (
            f"try_cast(`{rule.source_column}` AS {rule.target_type})"
        )
        typed_df = typed_df.withColumn(rule.target_column, F.expr(expression))
    return typed_df


def cast_failure(rule: TypeCastRule):
    """A nonblank source value that could not be converted."""
    raw_value = F.col(rule.source_column).cast("string")
    return (
        raw_value.isNotNull()
        & (F.trim(raw_value) != "")
        & F.col(rule.target_column).isNull()
    )


def any_cast_failure(rules: Sequence[TypeCastRule]):
    condition = F.lit(False)
    for rule in rules:
        condition = condition | cast_failure(rule)
    return condition


def type_cast_quarantine_rows(
    df: DataFrame,
    rules: Sequence[TypeCastRule],
    table_name: str,
    record_key_column: str,
    run_id: str,
) -> DataFrame:
    """Create standard quarantine rows and emit traceable cast warnings."""
    raw_columns = [
        column
        for column in df.columns
        if not column.endswith("_typed") and not column.startswith("silver_")
    ]
    failures = []
    for rule in rules:
        failed = df.filter(cast_failure(rule))
        counts = failed.agg(F.count("*").alias("count")).collect()[0]["count"]
        if counts:
            samples = failed.select(
                F.col(record_key_column).cast("string").alias("record_key"),
                F.col(rule.source_column).cast("string").alias("raw_value"),
                F.col("_batch_id").alias("batch_id"),
            ).limit(100).collect()
            for sample in samples:
                LOGGER.warning(json.dumps({
                    "event": "SCHEMA_EVOLUTION",
                    "event_type": "type_cast_failed",
                    "table": table_name,
                    "column": rule.source_column,
                    "target_type": rule.target_type,
                    "record_key": sample["record_key"],
                    "raw_value": sample["raw_value"],
                    "batch_id": sample["batch_id"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, sort_keys=True, default=str))
            if counts > len(samples):
                LOGGER.warning(json.dumps({
                    "event": "SCHEMA_EVOLUTION",
                    "event_type": "type_cast_failed_summary",
                    "table": table_name,
                    "column": rule.source_column,
                    "failed_rows": counts,
                    "logged_samples": len(samples),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, sort_keys=True))

        failures.append(failed.select(
            F.lit(run_id).alias("run_id"),
            F.lit(table_name).alias("source_table"),
            F.col("_source_record_id").alias("source_record_id"),
            F.col(record_key_column).cast("string").alias("record_key"),
            F.lit(rule.rule_id).alias("rule_id"),
            F.lit(
                f"{rule.source_column} must cast to {rule.target_type}"
            ).alias("rule_name"),
            F.concat(
                F.lit(f"type cast failed for {rule.source_column}; raw_value="),
                F.coalesce(F.col(rule.source_column).cast("string"), F.lit("<NULL>")),
            ).alias("failure_reason"),
            F.lit("quarantine").alias("severity"),
            F.lit("quarantined").alias("disposition"),
            F.to_json(F.struct(*[F.col(column) for column in raw_columns])).alias("raw_record"),
            F.current_timestamp().alias("detected_at"),
        ))

    if not failures:
        raise ValueError("At least one TypeCastRule is required")
    combined = failures[0]
    for failure_df in failures[1:]:
        combined = combined.unionByName(failure_df)
    return combined


def ensure_quarantine_table(spark, catalog: str) -> str:
    """Create the shared Silver quarantine table when a notebook runs alone."""
    table_name = f"{catalog}.silver.quarantine_records"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({QUARANTINE_SCHEMA_DDL}) USING DELTA"
    )
    return table_name
