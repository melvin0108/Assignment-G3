"""Databricks builders for the Gold dimensional mart.

Each builder publishes exactly one current-state Delta model.  The runner
validates the shared Silver snapshot before these functions are called.
"""

from pyspark.sql import functions as F

from pipeline.gold.gold_common import add_standard_metadata, stable_key_value, write_gold_table


def _key(model, *columns):
    return F.sha2(F.concat_ws("|", F.lit(model), *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]), 256)


def _refs(source_table, record_id):
    return F.array(F.struct(F.lit(source_table).alias("source_table"), F.col(record_id).cast("string").alias("source_record_id")))


def _metadata(df, run_id, batch_id, source_table, record_id, quality=F.lit("pass"), warnings=F.array()):
    return add_standard_metadata(
        df.withColumn("source_references", _refs(source_table, record_id)), run_id, batch_id, quality, warnings
    )


def _unknown_from(df, values):
    """Return a typed one-row unknown member matching ``df``'s complete schema."""
    expressions = []
    for field in df.schema.fields:
        value = values.get(field.name)
        expressions.append(F.lit(value).cast(field.dataType).alias(field.name))
    return df.limit(0).select(*expressions)


def _write(df, catalog, model):
    write_gold_table(df, catalog, model)
    print(f"Published {catalog}.gold.{model}")


def build_dimensions(spark, catalog, run_id, batch_id):
    """Build the six documented Gold dimensions with deterministic unknowns."""
    date = spark.read.table(f"{catalog}.silver.date_dim")
    dim_date = _metadata(date.select(
        F.date_format("date_id", "yyyyMMdd").cast("int").alias("date_key"), "date_id", "year", "month", "quarter", "is_weekend",
        F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "date_dim", "_source_record_id")
    unknown_date = _unknown_from(dim_date, {"date_key": 0, "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_date"], "usage_restrictions": "internal_only"})
    _write(dim_date.unionByName(unknown_date), catalog, "dim_date")

    merchants = spark.read.table(f"{catalog}.silver.merchants").alias("m")
    categories = spark.read.table(f"{catalog}.silver.merchant_categories").alias("mc")
    merchant = merchants.join(categories, "mcc", "left").select(
        _key("dim_merchant", "merchant_id").alias("merchant_key"), "merchant_id", F.col("m.name").alias("merchant_name"),
        "mcc", "category_name", "category_group", F.col("m.country").alias("country"), F.col("m.risk_rating").alias("risk_rating"),
        F.col("m.status").alias("merchant_status"), F.col("m.effective_at").alias("effective_at"), F.lit(False).alias("is_unknown"),
        F.col("m._source_record_id").alias("_source_record_id"))
    dim_merchant = _metadata(merchant, run_id, batch_id, "merchants", "_source_record_id")
    _write(dim_merchant.unionByName(_unknown_from(dim_merchant, {"merchant_key": stable_key_value("dim_merchant", "UNKNOWN"), "merchant_id": "UNKNOWN", "merchant_name": "Unknown merchant", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_merchant"], "usage_restrictions": "internal_only"})), catalog, "dim_merchant")

    channels = spark.read.table(f"{catalog}.silver.channels")
    dim_channel = _metadata(channels.select(_key("dim_channel", "channel_code").alias("channel_key"), F.col("channel_code").alias("channel_code"), "channel_name", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "channels", "_source_record_id")
    _write(dim_channel.unionByName(_unknown_from(dim_channel, {"channel_key": stable_key_value("dim_channel", "UNKNOWN"), "channel_code": "UNKNOWN", "channel_name": "Unknown channel", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_channel"], "usage_restrictions": "internal_only"})), catalog, "dim_channel")

    reasons = spark.read.table(f"{catalog}.silver.dispute_reason_codes")
    dim_reason = _metadata(reasons.select(_key("dim_dispute_reason", "reason_code").alias("dispute_reason_key"), "reason_code", "description", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "dispute_reason_codes", "_source_record_id")
    _write(dim_reason.unionByName(_unknown_from(dim_reason, {"dispute_reason_key": stable_key_value("dim_dispute_reason", "UNKNOWN"), "reason_code": "UNKNOWN", "description": "Unknown dispute reason", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_dispute_reason"], "usage_restrictions": "internal_only"})), catalog, "dim_dispute_reason")

    currencies = spark.read.table(f"{catalog}.silver.currencies")
    dim_currency = _metadata(currencies.select(_key("dim_currency", "currency_code").alias("currency_key"), "currency_code", F.col("name").alias("currency_name"), "decimals", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "currencies", "_source_record_id")
    _write(dim_currency.unionByName(_unknown_from(dim_currency, {"currency_key": stable_key_value("dim_currency", "UNKNOWN"), "currency_code": "UNKNOWN", "currency_name": "Unknown currency", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_currency"], "usage_restrictions": "internal_only"})), catalog, "dim_currency")


def build_case_and_facts(spark, catalog, run_id, batch_id):
    """Build eligible case facts first, then the case dimension from their warnings."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    links = (spark.read.table(f"{catalog}.silver.case_transactions").join(cases.select("case_id"), "case_id")
        .select("case_id", "transaction_id", "linked_at", F.col("_source_record_id").alias("link_source_record_id")))
    transactions = spark.read.table(f"{catalog}.silver.transactions").alias("t")
    fact_tx = links.alias("l").join(transactions, "transaction_id", "left").select(
        _key("fact_case_transaction", "case_id", "transaction_id").alias("case_transaction_key"),
        _key("dim_case", "case_id").alias("case_key"), _key("dim_merchant", "merchant_id").alias("merchant_key"),
        _key("dim_channel", "channel").alias("channel_key"), _key("dim_currency", "currency").alias("currency_key"),
        F.coalesce(F.date_format("txn_ts", "yyyyMMdd").cast("int"), F.lit(0)).alias("transaction_date_key"),
        "transaction_id", F.col("amount").cast("decimal(18,2)").alias("amount"), "txn_ts", F.col("status").alias("transaction_status"),
        F.col("l.link_source_record_id").alias("_source_record_id"), F.col("merchant_id").isNull().alias("missing_transaction"))
    fact_tx = _metadata(fact_tx, run_id, batch_id, "case_transactions", "_source_record_id", F.when(F.col("missing_transaction"), "partial").otherwise("pass"), F.when(F.col("missing_transaction"), F.array(F.lit("missing_transaction"))).otherwise(F.array())).drop("missing_transaction")
    _write(fact_tx, catalog, "fact_case_transaction")

    fact_auth = (spark.read.table(f"{catalog}.silver.auth_attempts").alias("a").join(links.select("case_id", "transaction_id"), "transaction_id")
        .select(_key("fact_authorization_attempt", "case_id", "attempt_id").alias("authorization_attempt_key"), _key("dim_case", "case_id").alias("case_key"), "transaction_id", "attempt_id", F.coalesce(F.date_format("auth_ts", "yyyyMMdd").cast("int"), F.lit(0)).alias("authorization_date_key"), "decision", "decline_reason", "auth_ts", F.col("a._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_auth, run_id, batch_id, "auth_attempts", "_source_record_id"), catalog, "fact_authorization_attempt")

    disputes = (spark.read.table(f"{catalog}.silver.disputes").alias("d").join(links.select("case_id", "transaction_id"), "transaction_id")
        .join(transactions.select("transaction_id", "currency"), "transaction_id"))
    fact_dispute = disputes.select(_key("fact_dispute", "case_id", "dispute_id").alias("dispute_key"), _key("dim_case", "case_id").alias("case_key"), _key("dim_dispute_reason", "reason_code").alias("dispute_reason_key"), _key("dim_currency", "currency").alias("currency_key"), F.coalesce(F.date_format("raised_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("raised_date_key"), "transaction_id", "dispute_id", F.col("d.amount").cast("decimal(18,2)").alias("amount"), F.col("d.status").alias("dispute_status"), "raised_at", F.col("d._source_record_id").alias("_source_record_id"))
    _write(_metadata(fact_dispute, run_id, batch_id, "disputes", "_source_record_id"), catalog, "fact_dispute")

    fact_chargeback = (spark.read.table(f"{catalog}.silver.chargebacks").alias("c").join(fact_dispute.select("case_key", "dispute_id", "transaction_id", "currency_key"), "dispute_id")
        .select(_key("fact_chargeback", "case_key", "chargeback_id").alias("chargeback_key"), "case_key", "currency_key", F.coalesce(F.date_format("processed_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("processed_date_key"), "transaction_id", "dispute_id", "chargeback_id", "scheme", F.col("c.amount").cast("decimal(18,2)").alias("amount"), F.col("stage").alias("chargeback_stage"), "processed_at", F.col("c._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_chargeback, run_id, batch_id, "chargebacks", "_source_record_id"), catalog, "fact_chargeback")

    fact_alert = (spark.read.table(f"{catalog}.silver.fraud_alerts").alias("a").join(links.select("case_id", "transaction_id"), "transaction_id")
        .select(_key("fact_fraud_alert", "case_id", "alert_id").alias("fraud_alert_key"), _key("dim_case", "case_id").alias("case_key"), F.coalesce(F.date_format("triggered_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("triggered_date_key"), "transaction_id", "alert_id", "rule_name", "score", "disposition", "triggered_at", F.col("a._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_alert, run_id, batch_id, "fraud_alerts", "_source_record_id"), catalog, "fact_fraud_alert")

    notes = spark.read.table(f"{catalog}.silver.investigation_notes").alias("n").join(cases.select("case_id"), "case_id")
    fact_note = notes.select(_key("fact_investigation_note", "case_id", "note_id").alias("investigation_note_key"), _key("dim_case", "case_id").alias("case_key"), F.coalesce(F.date_format("created_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("created_date_key"), "note_id", F.col("note_text").alias("safe_note_text"), "created_at", F.col("n._source_record_id").alias("_source_record_id"))
    _write(_metadata(fact_note, run_id, batch_id, "investigation_notes", "_source_record_id"), catalog, "fact_investigation_note")

    parties = spark.read.table(f"{catalog}.silver.case_parties").alias("p").join(cases.select("case_id"), "case_id")
    party_counts = parties.groupBy("case_id", "party_type", "role").agg(F.count(F.lit(1)).alias("party_count"), F.min(F.col("p._source_record_id")).alias("_source_record_id"))
    fact_party = party_counts.select(_key("fact_case_party_summary", "case_id", "party_type", "role").alias("case_party_summary_key"), _key("dim_case", "case_id").alias("case_key"), "party_type", "role", "party_count", "_source_record_id")
    _write(_metadata(fact_party, run_id, batch_id, "case_parties", "_source_record_id"), catalog, "fact_case_party_summary")

    fraud_types = spark.read.table(f"{catalog}.silver.fraud_types").select("fraud_type_code", F.col("description").alias("fraud_type_description"), "severity")
    statuses = spark.read.table(f"{catalog}.silver.case_status_types").select("status_code", F.col("description").alias("status_description"))
    case_warnings = fact_tx.groupBy("case_key").agg(F.array_sort(F.array_distinct(F.flatten(F.collect_list("warning_flags")))).alias("fact_warning_flags"), F.max(F.when(F.col("quality_status") == "partial", 1).otherwise(0)).alias("has_partial"))
    dim_case = cases.join(statuses, "status_code", "left").join(fraud_types, "fraud_type_code", "left").withColumn("case_key", _key("dim_case", "case_id")).join(case_warnings, "case_key", "left").select("case_key", "case_id", "priority", "status_code", "status_description", "fraud_type_code", F.col("severity").alias("fraud_type_severity"), "opened_at", "closed_at", F.coalesce(F.date_format("opened_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("opened_date_key"), F.coalesce(F.date_format("closed_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("closed_date_key"), F.coalesce("fact_warning_flags", F.array()).alias("fact_warning_flags"), F.coalesce("has_partial", F.lit(0)).alias("has_partial"), F.col("c._source_record_id").alias("_source_record_id"))
    dim_case = _metadata(dim_case, run_id, batch_id, "investigation_cases", "_source_record_id", F.when(F.col("has_partial") == 1, "partial").otherwise("pass"), F.col("fact_warning_flags")).drop("fact_warning_flags", "has_partial")
    _write(dim_case, catalog, "dim_case")


def build_investigation_context(spark, catalog, run_id, batch_id):
    """Materialize one AI-safe, typed context document per Gold case."""
    cases = spark.read.table(f"{catalog}.gold.dim_case")
    tx = spark.read.table(f"{catalog}.gold.fact_case_transaction")
    merchants = spark.read.table(f"{catalog}.gold.dim_merchant")
    channels = spark.read.table(f"{catalog}.gold.dim_channel")
    disputes = spark.read.table(f"{catalog}.gold.fact_dispute")
    chargebacks = spark.read.table(f"{catalog}.gold.fact_chargeback")
    alerts = spark.read.table(f"{catalog}.gold.fact_fraud_alert")
    authorizations = spark.read.table(f"{catalog}.gold.fact_authorization_attempt")
    notes = spark.read.table(f"{catalog}.gold.fact_investigation_note")
    parties = spark.read.table(f"{catalog}.gold.fact_case_party_summary")
    tx_items = tx.join(merchants.select("merchant_key", "merchant_name", "category_name", "category_group"), "merchant_key", "left").join(channels.select("channel_key", "channel_name"), "channel_key", "left").groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("transaction_id", "amount", "txn_ts", "transaction_status", "merchant_name", "category_name", "category_group", "channel_name"))).alias("transactions"))
    cb_items = chargebacks.groupBy("case_key", "dispute_id").agg(F.array_sort(F.collect_list(F.struct("chargeback_id", "scheme", "amount", "chargeback_stage", "processed_at"))).alias("chargebacks"))
    dispute_items = disputes.join(cb_items, ["case_key", "dispute_id"], "left").groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("dispute_id", "transaction_id", "amount", "dispute_status", "raised_at", "chargebacks"))).alias("disputes"))
    alert_items = alerts.groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("alert_id", "transaction_id", "rule_name", "score", "disposition", "triggered_at"))).alias("fraud_alerts"))
    authorization_items = authorizations.groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"))).alias("authorization_attempts"))
    note_items = notes.groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("note_id", "safe_note_text", "created_at"))).alias("safe_notes"))
    party_items = parties.groupBy("case_key").agg(F.array_sort(F.collect_list(F.struct("party_type", "role", "party_count"))).alias("party_summaries"))
    context = cases.join(tx_items, "case_key", "left").join(dispute_items, "case_key", "left").join(alert_items, "case_key", "left").join(authorization_items, "case_key", "left").join(note_items, "case_key", "left").join(party_items, "case_key", "left").select("case_key", "case_id", F.lit("investigation_case").alias("context_category"), F.struct("priority", "status_code", "status_description", "fraud_type_code", "fraud_type_severity", "opened_at", "closed_at").alias("case_detail"), F.concat_ws(" ", F.lit("Case"), F.col("case_id"), F.lit("is"), F.col("status_code"), F.lit("with priority"), F.col("priority"), F.lit("and fraud type"), F.col("fraud_type_code")).alias("case_summary"), "transactions", "disputes", "fraud_alerts", "authorization_attempts", "safe_notes", "party_summaries", "quality_status", "warning_flags", "source_references", "usage_restrictions")
    context = context.withColumn("masking_status", F.lit("masked")).withColumn("context_version", F.lit("2.0.0"))
    context = context.withColumn("transactions", F.coalesce("transactions", F.array())).withColumn("disputes", F.coalesce("disputes", F.array())).withColumn("fraud_alerts", F.coalesce("fraud_alerts", F.array())).withColumn("authorization_attempts", F.coalesce("authorization_attempts", F.array())).withColumn("safe_notes", F.coalesce("safe_notes", F.array())).withColumn("party_summaries", F.coalesce("party_summaries", F.array()))
    context = _metadata(context.drop("pipeline_run_id", "batch_id", "last_refreshed_at"), run_id, batch_id, "investigation_cases", "case_id", F.col("quality_status"), F.col("warning_flags"))
    _write(context, catalog, "investigation_context")
