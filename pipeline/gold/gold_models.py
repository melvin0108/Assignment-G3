"""Databricks builders for the Gold dimensional mart.

Each builder publishes exactly one current-state Delta model.  The runner
validates the shared Silver snapshot before these functions are called.
"""

from pyspark.sql import functions as F

from pipeline.gold.gold_common import AI_ALLOWED_RESTRICTIONS, USAGE_RESTRICTIONS, add_standard_metadata, write_gold_table


def _refs(source_table, record_id):
    return F.array(F.struct(F.lit(source_table).alias("source_table"), F.col(record_id).cast("string").alias("source_record_id")))


def _empty_string_array():
    return F.slice(F.array(F.lit("")), 1, 0)


def _metadata(df, run_id, batch_id, source_table, record_id, quality=F.lit("pass"), warnings=None, usage_restrictions=USAGE_RESTRICTIONS):
    if warnings is None:
        warnings = _empty_string_array()
    return add_standard_metadata(
        df.withColumn("source_references", _refs(source_table, record_id)).drop("_source_record_id"),
        run_id, batch_id, quality, warnings, usage_restrictions,
    )


def _unknown_from(df, values):
    """Return a typed one-row unknown member matching ``df``'s complete schema."""
    expressions = []
    for field in df.schema.fields:
        if field.name not in values:
            expression = F.col(field.name) if field.name in {"pipeline_run_id", "batch_id", "last_refreshed_at", "source_references"} else F.lit(None)
        elif values[field.name] is None:
            expression = F.lit(None)
        elif field.dataType.typeName() == "array":
            expression = F.array(*[F.lit(item) for item in values[field.name]])
        else:
            expression = F.lit(values[field.name]).cast(field.dataType)
        expressions.append(expression.alias(field.name))
    return df.limit(1).select(*expressions)


def _write(df, catalog, model):
    write_gold_table(df, catalog, model)
    print(f"Published {catalog}.gold.{model}")


def _masked_reference(label, column, suffix_length=4):
    """Return a display reference that never contains the complete source key."""
    return F.concat(
        F.lit(f"{label} ••••"),
        F.substring(F.col(column), -suffix_length, suffix_length),
    )


def build_dim_date(spark, catalog, run_id, batch_id):
    """Build dim_date table."""
    date = spark.read.table(f"{catalog}.silver.date_dim")
    dim_date = _metadata(date.select(
        F.date_format("date_id", "yyyyMMdd").cast("int").alias("date_key"), "date_id", "year", "month", "quarter", "is_weekend",
        F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "date_dim", "_source_record_id")
    unknown_date = _unknown_from(dim_date, {"date_key": 0, "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_date"], "usage_restrictions": USAGE_RESTRICTIONS})
    _write(dim_date.unionByName(unknown_date), catalog, "dim_date")


def build_dim_merchant(spark, catalog, run_id, batch_id):
    """Build dim_merchant table."""
    merchants = spark.read.table(f"{catalog}.silver.merchants").alias("m")
    categories = spark.read.table(f"{catalog}.silver.merchant_categories").alias("mc")
    merchant = merchants.join(categories, "mcc", "left").select(
        "merchant_id", F.col("m.name").alias("merchant_name"),
        "mcc", "category_name", "category_group", F.col("m.country").alias("country"), F.col("m.risk_rating").alias("risk_rating"),
        F.col("m.status").alias("merchant_status"), F.col("m.effective_at").alias("effective_at"), F.lit(False).alias("is_unknown"),
        F.col("m._source_record_id").alias("_source_record_id"))
    dim_merchant = _metadata(merchant, run_id, batch_id, "merchants", "_source_record_id")
    _write(dim_merchant.unionByName(_unknown_from(dim_merchant, {"merchant_id": "UNKNOWN", "merchant_name": "Unknown merchant", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_merchant"], "usage_restrictions": USAGE_RESTRICTIONS})), catalog, "dim_merchant")


def build_dim_channel(spark, catalog, run_id, batch_id):
    """Build dim_channel table."""
    channels = spark.read.table(f"{catalog}.silver.channels")
    dim_channel = _metadata(channels.select("channel_code", "channel_name", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "channels", "_source_record_id")
    _write(dim_channel.unionByName(_unknown_from(dim_channel, {"channel_code": "UNKNOWN", "channel_name": "Unknown channel", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_channel"], "usage_restrictions": USAGE_RESTRICTIONS})), catalog, "dim_channel")


def build_dim_dispute_reason(spark, catalog, run_id, batch_id):
    """Build dim_dispute_reason table."""
    reasons = spark.read.table(f"{catalog}.silver.dispute_reason_codes")
    dim_reason = _metadata(reasons.select("reason_code", "description", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "dispute_reason_codes", "_source_record_id")
    _write(dim_reason.unionByName(_unknown_from(dim_reason, {"reason_code": "UNKNOWN", "description": "Unknown dispute reason", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_dispute_reason"], "usage_restrictions": USAGE_RESTRICTIONS})), catalog, "dim_dispute_reason")


def build_dim_currency(spark, catalog, run_id, batch_id):
    """Build dim_currency table."""
    currencies = spark.read.table(f"{catalog}.silver.currencies")
    dim_currency = _metadata(currencies.select("currency_code", F.col("name").alias("currency_name"), "decimals", F.lit(False).alias("is_unknown"), "_source_record_id"), run_id, batch_id, "currencies", "_source_record_id")
    _write(dim_currency.unionByName(_unknown_from(dim_currency, {"currency_code": "UNKNOWN", "currency_name": "Unknown currency", "is_unknown": True, "quality_status": "partial", "warning_flags": ["unknown_currency"], "usage_restrictions": USAGE_RESTRICTIONS})), catalog, "dim_currency")


def build_dim_consumer_account(spark, catalog, run_id, batch_id):
    """Build the protected customer/account ownership broker."""
    customers = spark.read.table(f"{catalog}.silver.customers").select(
        "customer_id"
    ).distinct()
    accounts = spark.read.table(f"{catalog}.silver.accounts").alias("a")
    consumer_accounts = accounts.join(
        customers.alias("c"), "customer_id", "inner"
    ).select(
        "customer_id",
        "account_id",
        _masked_reference("Account", "account_id").alias("account_reference"),
        "product_type",
        F.col("status").alias("account_status"),
        F.upper("currency").alias("currency_code"),
        "open_date",
        F.col("a._source_record_id").alias("_source_record_id"),
    )
    _write(
        _metadata(
            consumer_accounts,
            run_id,
            batch_id,
            "accounts",
            "_source_record_id",
        ),
        catalog,
        "dim_consumer_account",
    )


def build_dim_consumer_card(spark, catalog, run_id, batch_id):
    """Build the protected customer/card ownership broker."""
    accounts = spark.read.table(
        f"{catalog}.gold.dim_consumer_account"
    ).alias("a")
    cards = spark.read.table(f"{catalog}.silver.cards").alias("c")
    card_last_four = F.substring(F.col("c.pan"), -4, 4)
    consumer_cards = cards.join(accounts, "account_id", "inner").select(
        "customer_id",
        "account_id",
        "card_id",
        "account_reference",
        F.concat(F.lit("Card ••••"), card_last_four).alias("card_reference"),
        card_last_four.alias("card_last_four"),
        "card_type",
        F.to_date(
            F.concat(F.col("expiry"), F.lit("-01")), "yyyy-MM-dd"
        ).alias("expiry_month"),
        F.col("c.status").alias("card_status"),
        F.col("c._source_record_id").alias("_source_record_id"),
    )
    _write(
        _metadata(
            consumer_cards,
            run_id,
            batch_id,
            "cards",
            "_source_record_id",
        ),
        catalog,
        "dim_consumer_card",
    )


def build_fact_consumer_transaction(spark, catalog, run_id, batch_id):
    """Build customer-scoped transaction details without raw business IDs."""
    accounts = spark.read.table(
        f"{catalog}.gold.dim_consumer_account"
    ).alias("a")
    cards = spark.read.table(f"{catalog}.gold.dim_consumer_card").alias("c")
    transactions = spark.read.table(
        f"{catalog}.silver.transactions"
    ).alias("t")
    merchants = spark.read.table(f"{catalog}.silver.merchants").alias("m")
    categories = spark.read.table(
        f"{catalog}.silver.merchant_categories"
    ).alias("mc")
    card_join = (
        (F.col("t.card_id") == F.col("c.card_id"))
        & (F.col("t.account_id") == F.col("c.account_id"))
        & (F.col("a.customer_id") == F.col("c.customer_id"))
    )
    consumer_transactions = (
        transactions.join(accounts, "account_id", "inner")
        .join(cards, card_join, "left")
        .join(merchants, "merchant_id", "inner")
        .join(categories, "mcc", "left")
        .filter(F.col("t.card_id").isNull() | F.col("c.card_id").isNotNull())
        .select(
            F.col("a.customer_id").alias("customer_id"),
            F.col("t.account_id").alias("account_id"),
            F.col("t.card_id").alias("card_id"),
            _masked_reference(
                "Transaction", "t.transaction_id", 6
            ).alias("transaction_reference"),
            F.col("a.account_reference").alias("account_reference"),
            F.col("c.card_reference").alias("card_reference"),
            F.col("m.name").alias("merchant_name"),
            F.col("mc.category_name").alias("merchant_category"),
            F.col("m.country").alias("merchant_country"),
            F.col("t.channel").alias("channel"),
            F.col("t.currency").alias("currency_code"),
            F.col("t.txn_ts").alias("transaction_at"),
            F.col("t.status").alias("transaction_status"),
            F.col("t.amount").cast("decimal(18,2)").alias("amount"),
            F.col("t._source_record_id").alias("_source_record_id"),
            F.col("mc.category_name").isNull().alias(
                "missing_merchant_category"
            ),
        )
    )
    consumer_transactions = _metadata(
        consumer_transactions,
        run_id,
        batch_id,
        "transactions",
        "_source_record_id",
        F.when(F.col("missing_merchant_category"), "partial").otherwise("pass"),
        F.when(
            F.col("missing_merchant_category"),
            F.array(F.lit("missing_merchant_category")),
        ).otherwise(_empty_string_array()),
    ).drop("missing_merchant_category")
    _write(
        consumer_transactions,
        catalog,
        "fact_consumer_transaction",
    )


def build_fact_consumer_dispute(spark, catalog, run_id, batch_id):
    """Build customer-scoped disputes without investigation-only attributes."""
    accounts = spark.read.table(
        f"{catalog}.gold.dim_consumer_account"
    ).alias("a")
    cards = spark.read.table(f"{catalog}.gold.dim_consumer_card").alias("c")
    transactions = spark.read.table(
        f"{catalog}.silver.transactions"
    ).alias("t")
    disputes = spark.read.table(f"{catalog}.silver.disputes").alias("d")
    reasons = spark.read.table(
        f"{catalog}.silver.dispute_reason_codes"
    ).alias("r")
    card_join = (
        (F.col("t.card_id") == F.col("c.card_id"))
        & (F.col("t.account_id") == F.col("c.account_id"))
        & (F.col("a.customer_id") == F.col("c.customer_id"))
    )
    consumer_disputes = (
        disputes.join(transactions, "transaction_id", "inner")
        .join(accounts, "account_id", "inner")
        .join(cards, card_join, "left")
        .join(reasons, "reason_code", "left")
        .filter(F.col("t.card_id").isNull() | F.col("c.card_id").isNotNull())
        .select(
            F.col("a.customer_id").alias("customer_id"),
            F.col("t.account_id").alias("account_id"),
            F.col("t.card_id").alias("card_id"),
            _masked_reference("Dispute", "d.dispute_id", 6).alias(
                "dispute_reference"
            ),
            _masked_reference(
                "Transaction", "t.transaction_id", 6
            ).alias("transaction_reference"),
            F.col("a.account_reference").alias("account_reference"),
            F.col("c.card_reference").alias("card_reference"),
            F.col("r.description").alias("reason_description"),
            F.col("t.currency").alias("currency_code"),
            F.col("d.raised_at").alias("raised_at"),
            F.col("d.status").alias("dispute_status"),
            F.col("d.amount").cast("decimal(18,2)").alias("amount"),
            F.col("d._source_record_id").alias("_source_record_id"),
            F.col("r.description").isNull().alias(
                "missing_dispute_reason"
            ),
        )
    )
    consumer_disputes = _metadata(
        consumer_disputes,
        run_id,
        batch_id,
        "disputes",
        "_source_record_id",
        F.when(F.col("missing_dispute_reason"), "partial").otherwise("pass"),
        F.when(
            F.col("missing_dispute_reason"),
            F.array(F.lit("missing_dispute_reason")),
        ).otherwise(_empty_string_array()),
    ).drop("missing_dispute_reason")
    _write(
        consumer_disputes,
        catalog,
        "fact_consumer_dispute",
    )


def build_fact_case_transaction(spark, catalog, run_id, batch_id):
    """Build fact_case_transaction table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    links = (spark.read.table(f"{catalog}.silver.case_transactions").join(cases.select("case_id"), "case_id")
        .select("case_id", "transaction_id", "linked_at", F.col("_source_record_id").alias("link_source_record_id")))
    transactions = spark.read.table(f"{catalog}.silver.transactions").alias("t")
    fact_tx = links.alias("l").join(transactions, "transaction_id", "left").select(
        "case_id", "transaction_id",
        F.coalesce("merchant_id", F.lit("UNKNOWN")).alias("merchant_id"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_code"),
        F.coalesce("currency", F.lit("UNKNOWN")).alias("currency_code"),
        F.coalesce(F.date_format("txn_ts", "yyyyMMdd").cast("int"), F.lit(0)).alias("transaction_date_key"),
        F.col("amount").cast("decimal(18,2)").alias("amount"), "txn_ts", F.col("status").alias("transaction_status"),
        F.col("l.link_source_record_id").alias("_source_record_id"), F.col("t._source_record_id").isNull().alias("missing_transaction"))
    fact_tx = _metadata(fact_tx, run_id, batch_id, "case_transactions", "_source_record_id", F.when(F.col("missing_transaction"), "partial").otherwise("pass"), F.when(F.col("missing_transaction"), F.array(F.lit("missing_transaction"))).otherwise(F.array())).drop("missing_transaction")
    _write(fact_tx, catalog, "fact_case_transaction")


def build_fact_authorization_attempt(spark, catalog, run_id, batch_id):
    """Build fact_authorization_attempt table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    links = (spark.read.table(f"{catalog}.silver.case_transactions").join(cases.select("case_id"), "case_id")
        .select("case_id", "transaction_id", "linked_at", F.col("_source_record_id").alias("link_source_record_id")))
    fact_auth = (spark.read.table(f"{catalog}.silver.auth_attempts").alias("a").join(links.select("case_id", "transaction_id"), "transaction_id")
        .select("case_id", "transaction_id", "attempt_id", F.coalesce(F.date_format("auth_ts", "yyyyMMdd").cast("int"), F.lit(0)).alias("authorization_date_key"), "decision", "decline_reason", "auth_ts", F.col("a._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_auth, run_id, batch_id, "auth_attempts", "_source_record_id"), catalog, "fact_authorization_attempt")


def build_fact_dispute(spark, catalog, run_id, batch_id):
    """Build fact_dispute table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    links = (spark.read.table(f"{catalog}.silver.case_transactions").join(cases.select("case_id"), "case_id")
        .select("case_id", "transaction_id", "linked_at", F.col("_source_record_id").alias("link_source_record_id")))
    transactions = spark.read.table(f"{catalog}.silver.transactions").alias("t")
    disputes = (spark.read.table(f"{catalog}.silver.disputes").alias("d").join(links.select("case_id", "transaction_id"), "transaction_id")
        .join(transactions.select("transaction_id", "currency"), "transaction_id", "left"))
    fact_dispute = disputes.select("case_id", "transaction_id", "dispute_id", F.coalesce("reason_code", F.lit("UNKNOWN")).alias("reason_code"), F.coalesce("currency", F.lit("UNKNOWN")).alias("currency_code"), F.coalesce(F.date_format("raised_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("raised_date_key"), F.col("d.amount").cast("decimal(18,2)").alias("amount"), F.col("d.status").alias("dispute_status"), "raised_at", F.col("d._source_record_id").alias("_source_record_id"))
    _write(_metadata(fact_dispute, run_id, batch_id, "disputes", "_source_record_id"), catalog, "fact_dispute")


def build_fact_chargeback(spark, catalog, run_id, batch_id):
    """Build fact_chargeback table."""
    fact_dispute = spark.read.table(f"{catalog}.gold.fact_dispute")
    fact_chargeback = (spark.read.table(f"{catalog}.silver.chargebacks").alias("c").join(fact_dispute.select("case_id", "dispute_id", "transaction_id", "currency_code"), "dispute_id")
        .select("case_id", "transaction_id", "dispute_id", "chargeback_id", "currency_code", F.coalesce(F.date_format("processed_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("processed_date_key"), "scheme", F.col("c.amount").cast("decimal(18,2)").alias("amount"), F.col("stage").alias("chargeback_stage"), "processed_at", F.col("c._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_chargeback, run_id, batch_id, "chargebacks", "_source_record_id"), catalog, "fact_chargeback")


def build_fact_fraud_alert(spark, catalog, run_id, batch_id):
    """Build fact_fraud_alert table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    links = (spark.read.table(f"{catalog}.silver.case_transactions").join(cases.select("case_id"), "case_id")
        .select("case_id", "transaction_id", "linked_at", F.col("_source_record_id").alias("link_source_record_id")))
    fact_alert = (spark.read.table(f"{catalog}.silver.fraud_alerts").alias("a").join(links.select("case_id", "transaction_id"), "transaction_id")
        .select("case_id", "transaction_id", "alert_id", F.coalesce(F.date_format("triggered_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("triggered_date_key"), "rule_name", "score", "disposition", "triggered_at", F.col("a._source_record_id").alias("_source_record_id")))
    _write(_metadata(fact_alert, run_id, batch_id, "fraud_alerts", "_source_record_id"), catalog, "fact_fraud_alert")


def build_fact_investigation_note(spark, catalog, run_id, batch_id):
    """Build fact_investigation_note table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    notes = spark.read.table(f"{catalog}.silver.investigation_notes").alias("n").join(cases.select("case_id"), "case_id")
    fact_note = notes.select("case_id", "note_id", F.coalesce(F.date_format("created_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("created_date_key"), F.col("note_text").alias("safe_note_text"), "created_at", F.col("n._source_record_id").alias("_source_record_id"))
    _write(_metadata(fact_note, run_id, batch_id, "investigation_notes", "_source_record_id"), catalog, "fact_investigation_note")


def build_fact_case_party_summary(spark, catalog, run_id, batch_id):
    """Build fact_case_party_summary table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    parties = spark.read.table(f"{catalog}.silver.case_parties").alias("p").join(cases.select("case_id"), "case_id")
    party_counts = parties.groupBy("case_id", "party_type", "role").agg(F.count(F.lit(1)).alias("party_count"), F.min(F.col("p._source_record_id")).alias("_source_record_id"))
    fact_party = party_counts.select("case_id", "party_type", "role", "party_count", "_source_record_id")
    _write(_metadata(fact_party, run_id, batch_id, "case_parties", "_source_record_id"), catalog, "fact_case_party_summary")


def build_dim_case(spark, catalog, run_id, batch_id):
    """Build dim_case table."""
    cases = spark.read.table(f"{catalog}.silver.investigation_cases").filter(~F.col("legal_hold")).alias("c")
    fraud_types = spark.read.table(f"{catalog}.silver.fraud_types").select("fraud_type_code", F.col("description").alias("fraud_type_description"), "severity")
    statuses = spark.read.table(f"{catalog}.silver.case_status_types").select("status_code", F.col("description").alias("status_description"))
    fact_tx = spark.read.table(f"{catalog}.gold.fact_case_transaction")
    case_warnings = fact_tx.groupBy("case_id").agg(F.array_sort(F.array_distinct(F.flatten(F.collect_list("warning_flags")))).alias("fact_warning_flags"), F.max(F.when(F.col("quality_status") == "partial", 1).otherwise(0)).alias("has_partial"))
    dim_case = cases.join(statuses, "status_code", "left").join(fraud_types, "fraud_type_code", "left").join(case_warnings, "case_id", "left").select("case_id", "priority", "status_code", "status_description", "fraud_type_code", F.col("severity").alias("fraud_type_severity"), "opened_at", "closed_at", F.coalesce(F.date_format("opened_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("opened_date_key"), F.coalesce(F.date_format("closed_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("closed_date_key"), F.coalesce("fact_warning_flags", F.array()).alias("fact_warning_flags"), F.coalesce("has_partial", F.lit(0)).alias("has_partial"), F.col("c._source_record_id").alias("_source_record_id"))
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
    tx_items = tx.join(merchants.select("merchant_id", "merchant_name", "category_name", "category_group"), "merchant_id", "left").join(channels.select("channel_code", "channel_name"), "channel_code", "left").groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("transaction_id", "amount", "currency_code", "txn_ts", "transaction_status", "merchant_name", "category_name", "category_group", "channel_name"))).alias("transactions"))
    cb_items = chargebacks.groupBy("case_id", "dispute_id").agg(F.array_sort(F.collect_list(F.struct("chargeback_id", "scheme", "amount", "chargeback_stage", "processed_at"))).alias("chargebacks"))
    dispute_items = disputes.join(cb_items, ["case_id", "dispute_id"], "left").groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("dispute_id", "transaction_id", "amount", "currency_code", "dispute_status", "raised_at", "chargebacks"))).alias("disputes"))
    alert_items = alerts.groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("alert_id", "transaction_id", "rule_name", "score", "disposition", "triggered_at"))).alias("fraud_alerts"))
    authorization_items = authorizations.groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"))).alias("authorization_attempts"))
    note_items = notes.groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("note_id", "safe_note_text", "created_at"))).alias("safe_notes"))
    party_items = parties.groupBy("case_id").agg(F.array_sort(F.collect_list(F.struct("party_type", "role", "party_count"))).alias("party_summaries"))
    context = cases.join(tx_items, "case_id", "left").join(dispute_items, "case_id", "left").join(alert_items, "case_id", "left").join(authorization_items, "case_id", "left").join(note_items, "case_id", "left").join(party_items, "case_id", "left").select("case_id", F.lit("investigation_case").alias("context_category"), F.struct("priority", "status_code", "status_description", "fraud_type_code", "fraud_type_severity", "opened_at", "closed_at").alias("case_detail"), F.concat_ws(" ", F.lit("Case"), F.col("case_id"), F.lit("is"), F.col("status_code"), F.lit("with priority"), F.col("priority"), F.lit("and fraud type"), F.col("fraud_type_code")).alias("case_summary"), "transactions", "disputes", "fraud_alerts", "authorization_attempts", "safe_notes", "party_summaries", "quality_status", "warning_flags", "source_references", "usage_restrictions")
    context = context.withColumn("masking_status", F.lit("masked")).withColumn("context_version", F.lit("2.0.0"))
    context = context.withColumn("transactions", F.coalesce("transactions", F.array())).withColumn("disputes", F.coalesce("disputes", F.array())).withColumn("fraud_alerts", F.coalesce("fraud_alerts", F.array())).withColumn("authorization_attempts", F.coalesce("authorization_attempts", F.array())).withColumn("safe_notes", F.coalesce("safe_notes", F.array())).withColumn("party_summaries", F.coalesce("party_summaries", F.array()))
    context = _metadata(context.drop("pipeline_run_id", "batch_id", "last_refreshed_at"), run_id, batch_id, "investigation_cases", "case_id", F.col("quality_status"), F.col("warning_flags"), usage_restrictions=AI_ALLOWED_RESTRICTIONS)
    _write(context, catalog, "investigation_context")


def build_dimensions(spark, catalog, run_id, batch_id):
    """Build the six documented Gold dimensions with deterministic unknowns."""
    build_dim_date(spark, catalog, run_id, batch_id)
    build_dim_merchant(spark, catalog, run_id, batch_id)
    build_dim_channel(spark, catalog, run_id, batch_id)
    build_dim_dispute_reason(spark, catalog, run_id, batch_id)
    build_dim_currency(spark, catalog, run_id, batch_id)


def build_case_and_facts(spark, catalog, run_id, batch_id):
    """Build eligible case facts first, then the case dimension from their warnings."""
    build_fact_case_transaction(spark, catalog, run_id, batch_id)
    build_fact_authorization_attempt(spark, catalog, run_id, batch_id)
    build_fact_dispute(spark, catalog, run_id, batch_id)
    build_fact_chargeback(spark, catalog, run_id, batch_id)
    build_fact_fraud_alert(spark, catalog, run_id, batch_id)
    build_fact_investigation_note(spark, catalog, run_id, batch_id)
    build_fact_case_party_summary(spark, catalog, run_id, batch_id)
    build_dim_case(spark, catalog, run_id, batch_id)
