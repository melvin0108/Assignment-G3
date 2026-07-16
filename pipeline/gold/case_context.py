"""Gold case eligibility and quality-warning transformations."""

from functools import reduce

from pyspark.sql import functions as F

from pipeline.gold.common import aggregate


def current_quarantine(spark, catalog, pipeline_run_id):
    return spark.read.table(f"{catalog}.silver.quarantine_records").where(
        F.col("run_id").isin(pipeline_run_id, f"{pipeline_run_id}-DQ")
    )


def build_case_base(inputs, quarantine):
    """Return the safe case grain and its excluded-case count."""
    cases = inputs["investigation_cases"].alias("c")
    case_quarantine = quarantine.where(F.col("source_table") == "investigation_cases").select(
        F.col("record_key").alias("case_id")
    ).distinct()
    fraud_types = inputs["fraud_types"].select(
        "fraud_type_code",
        F.col("description").alias("fraud_type_description"),
        F.col("_source_record_id").alias("fraud_type_source_record_id"),
    )
    base = (
        cases.join(fraud_types, "fraud_type_code", "left")
        .join(case_quarantine, "case_id", "left_anti")
        .where(F.col("case_id").isNotNull() & (F.col("legal_hold") == F.lit(False)))
        .select(
            "c.case_id", "c.priority", "c.status_code", "c.fraud_type_code",
            "fraud_type_description", "c.opened_at", "c.closed_at",
            "c._source_record_id", "fraud_type_source_record_id",
        )
    )
    excluded = cases.select("case_id").distinct().count() - base.select("case_id").distinct().count()
    return base, excluded


def build_warning_flags(quarantine):
    warning_frames = []
    for source_table, flag in [
        ("investigation_notes", "redacted_notes"),
        ("case_transactions", "transaction_link_removed"),
        ("case_parties", "party_link_removed"),
    ]:
        warning_frames.append(
            quarantine.where(F.col("source_table") == source_table)
            .select(
                F.get_json_object("raw_record", "$.case_id").alias("case_id"),
                F.lit(flag).alias("warning_flag"),
            )
            .where(F.col("case_id").isNotNull())
        )
    warnings = reduce(lambda left, right: left.unionByName(right), warning_frames)
    return aggregate(warnings, F.col("warning_flag"), "warning_flags")
