"""Gold lineage registry maintenance."""

from pyspark.sql.types import StringType, StructField, StructType


def rewrite_gold_lineage(spark, catalog):
    table = f"{catalog}.gov.metadata_lineage"
    schema = StructType([
        StructField("source_catalog", StringType(), True), StructField("source_schema", StringType(), True),
        StructField("source_table", StringType(), True), StructField("source_field", StringType(), True),
        StructField("target_catalog", StringType(), True), StructField("target_schema", StringType(), True),
        StructField("target_table", StringType(), True), StructField("target_field", StringType(), True),
        StructField("transformation_logic", StringType(), True),
    ])
    rows = [
        (catalog, "silver", "investigation_cases", "case_id", catalog, "gold", "investigation_context", "case_id", "Eligible, non-legal-hold case key"),
        (catalog, "silver", "investigation_cases", "priority,status_code,fraud_type_code,opened_at,closed_at", catalog, "gold", "investigation_context", "case_context,case_summary", "Explicit allow-list; deterministic Spark summary"),
        (catalog, "silver", "case_transactions,transactions,channels", "transaction fields", catalog, "gold", "investigation_context", "linked_transactions", "Pre-aggregated transaction evidence"),
        (catalog, "silver", "accounts,cards", "product,status,masked pan", catalog, "gold", "investigation_context", "payment_instrument_context", "IDs excluded; masked PAN reduced to final four digits"),
        (catalog, "silver", "merchants,merchant_categories", "merchant fields", catalog, "gold", "investigation_context", "merchant_context", "Pre-aggregated merchant evidence"),
        (catalog, "silver", "auth_attempts", "authorization fields", catalog, "gold", "investigation_context", "authorization_context", "Pre-aggregated authorization evidence"),
        (catalog, "silver", "disputes,chargebacks,dispute_reason_codes", "dispute fields", catalog, "gold", "investigation_context", "dispute_context", "Pre-aggregated dispute and chargeback evidence"),
        (catalog, "silver", "fraud_alerts", "alert fields", catalog, "gold", "investigation_context", "fraud_alerts", "Pre-aggregated fraud-alert evidence"),
        (catalog, "silver", "case_parties", "party_type,role", catalog, "gold", "investigation_context", "party_context", "Party identifiers excluded"),
        (catalog, "silver", "investigation_notes", "note_id,note_text,created_at", catalog, "gold", "investigation_context", "safe_notes", "Quarantined notes excluded"),
    ]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gov")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        source_catalog STRING, source_schema STRING, source_table STRING, source_field STRING,
        target_catalog STRING, target_schema STRING, target_table STRING, target_field STRING,
        transformation_logic STRING) USING DELTA""")
    spark.sql(f"DELETE FROM {table} WHERE target_schema = 'gold' AND target_table = 'investigation_context'")
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").saveAsTable(table)
