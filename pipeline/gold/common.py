"""Shared Gold snapshot, aggregation, and output-contract helpers."""

from functools import reduce

from pyspark.sql import functions as F

from pipeline.gold.contract import CONTEXT_VERSION, REQUIRED_INPUT_TABLES, SUMMARY_TEMPLATE


def catalog_widget(dbutils):
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


def load_current_inputs(spark, catalog):
    """Read every required Silver input and reject mismatched snapshots."""
    inputs, identities = {}, {}
    for name in REQUIRED_INPUT_TABLES:
        df = spark.read.table(f"{catalog}.silver.{name}")
        identity = df.select("_batch_id", "_run_id").distinct().limit(2).collect()
        if len(identity) != 1 or identity[0]["_batch_id"] is None or identity[0]["_run_id"] is None:
            raise ValueError(f"Silver input {name} must contain exactly one complete snapshot")
        identities[name] = (identity[0]["_batch_id"], identity[0]["_run_id"])
        inputs[name] = df
    if len(set(identities.values())) != 1:
        details = ", ".join(f"{name}={batch}/{run}" for name, (batch, run) in sorted(identities.items()))
        raise ValueError(f"Gold inputs do not share one Silver snapshot: {details}")
    batch_id, run_id = next(iter(identities.values()))
    return inputs, batch_id, run_id


def aggregate(df, value, alias):
    return df.groupBy("case_id").agg(F.sort_array(F.collect_set(value)).alias(alias))


def source_rows(df, source_table):
    return df.select(
        "case_id",
        F.lit(source_table).alias("source_table"),
        F.col("_source_record_id").alias("source_record_id"),
    ).where(
        F.col("case_id").isNotNull()
        & F.col("source_record_id").isNotNull()
        & (F.trim("source_record_id") != "")
    )


def source_references(source_frames):
    return reduce(lambda left, right: left.unionByName(right), source_frames).where(
        F.col("source_record_id").isNotNull() & (F.trim("source_record_id") != "")
    ).groupBy("case_id").agg(
        F.sort_array(F.collect_set(F.struct("source_table", "source_record_id"))).alias("source_references")
    )


def empty_arrays():
    return {
        "linked_transactions": F.expr("CAST(array() AS array<struct<transaction_id:string,amount:decimal(18,2),currency:string,channel_code:string,channel_name:string,txn_ts:timestamp,status:string>>)"),
        "payment_instrument_context": F.expr("CAST(array() AS array<struct<transaction_id:string,account_product_type:string,account_status:string,card_type:string,card_last4:string,card_status:string>>)"),
        "merchant_context": F.expr("CAST(array() AS array<struct<merchant_id:string,merchant_name:string,mcc:string,category_name:string,category_group:string,country:string,risk_rating:string,merchant_status:string>>)"),
        "authorization_context": F.expr("CAST(array() AS array<struct<attempt_id:string,transaction_id:string,decision:string,decline_reason:string,auth_ts:timestamp>>)"),
        "dispute_context": F.expr("CAST(array() AS array<struct<dispute_id:string,transaction_id:string,reason_code:string,reason_description:string,amount:decimal(18,2),status:string,raised_at:timestamp,chargebacks:array<struct<chargeback_id:string,scheme:string,amount:double,stage:string,processed_at:timestamp>>>>)"),
        "fraud_alerts": F.expr("CAST(array() AS array<struct<alert_id:string,transaction_id:string,rule_name:string,score:double,triggered_at:timestamp,disposition:string>>)"),
        "party_context": F.expr("CAST(array() AS array<struct<party_type:string,role:string>>)"),
        "safe_notes": F.expr("CAST(array() AS array<struct<note_id:string,note_text:string,created_at:timestamp>>)"),
        "warning_flags": F.expr("CAST(array() AS array<string>)"),
        "source_references": F.expr("CAST(array() AS array<struct<source_table:string,source_record_id:string>>)"),
    }


def assemble_context(case_base, collections, pipeline_run_id):
    empty = empty_arrays()
    gold = case_base
    for collection in collections:
        gold = gold.join(collection, "case_id", "left")
    for field, default in empty.items():
        gold = gold.withColumn(field, F.coalesce(field, default))
    return gold.withColumn(
        "warning_flags",
        F.sort_array(F.array_distinct(F.concat(
            F.when(F.size("linked_transactions") == 0, F.array(F.lit("partial_data"))).otherwise(empty["warning_flags"]),
            F.col("warning_flags"),
        ))),
    ).select(
        "case_id",
        F.lit("transaction_investigation").alias("context_category"),
        F.struct("priority", "status_code", "fraud_type_code", "fraud_type_description", "opened_at", "closed_at").alias("case_context"),
        F.format_string(
            SUMMARY_TEMPLATE,
            F.coalesce("priority", F.lit("unknown priority")),
            F.coalesce("fraud_type_description", F.lit("unknown fraud type")),
            F.coalesce(F.date_format("opened_at", "yyyy-MM-dd"), F.lit("unknown date")),
            F.coalesce("status_code", F.lit("unknown")),
        ).alias("case_summary"),
        "linked_transactions", "payment_instrument_context", "merchant_context", "authorization_context",
        "dispute_context", "fraud_alerts", "party_context", "safe_notes",
        F.when(F.size("warning_flags") == 0, F.lit("pass")).otherwise(F.lit("partial")).alias("quality_status"),
        F.lit("masked").alias("masking_status"),
        "warning_flags", "source_references",
        F.lit("internal_only").alias("usage_restrictions"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        F.lit(CONTEXT_VERSION).alias("context_version"),
        F.current_timestamp().alias("last_refreshed_at"),
    )
