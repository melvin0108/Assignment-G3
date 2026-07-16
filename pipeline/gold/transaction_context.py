"""Pre-aggregated transaction evidence for Gold investigation contexts."""

from pyspark.sql import functions as F

from pipeline.gold.common import aggregate, source_rows


def build_transaction_context(inputs):
    case_transactions = inputs["case_transactions"].select("case_id", "transaction_id", "_source_record_id").alias("ct")
    transactions = inputs["transactions"].alias("t")
    detail = case_transactions.join(
        transactions, F.col("ct.transaction_id") == F.col("t.transaction_id")
    ).select(
        F.col("ct.case_id").alias("case_id"),
        F.col("ct.transaction_id").alias("transaction_id"),
        F.col("ct._source_record_id").alias("case_transaction_source_record_id"),
        F.col("t.account_id"), F.col("t.card_id"), F.col("t.merchant_id"),
        F.col("t.channel").alias("channel_code"), F.col("t.amount").cast("decimal(18,2)").alias("amount"),
        F.col("t.currency"), F.col("t.txn_ts"), F.col("t.status"),
        F.col("t._source_record_id").alias("transaction_source_record_id"),
    )
    channels = inputs["channels"].select("channel_code", "channel_name", F.col("_source_record_id").alias("channel_source_record_id"))
    linked_transactions = aggregate(
        detail.join(channels, "channel_code", "left"),
        F.struct("transaction_id", "amount", "currency", "channel_code", "channel_name", "txn_ts", "status"),
        "linked_transactions",
    )

    accounts = inputs["accounts"].select("account_id", F.col("product_type").alias("account_product_type"), F.col("status").alias("account_status"), "_source_record_id").alias("a")
    cards = inputs["cards"].select("card_id", "card_type", F.col("pan").alias("masked_pan"), F.col("status").alias("card_status"), "_source_record_id").alias("c")
    payment = detail.alias("t").join(accounts, F.col("t.account_id") == F.col("a.account_id"), "left").join(cards, F.col("t.card_id") == F.col("c.card_id"), "left").select(
        F.col("t.case_id").alias("case_id"), F.col("t.transaction_id").alias("transaction_id"),
        "account_product_type", "account_status", "card_type",
        F.regexp_extract("masked_pan", r"(\d{4})$", 1).alias("card_last4"), "card_status",
        F.col("a._source_record_id").alias("account_source_record_id"), F.col("c._source_record_id").alias("card_source_record_id"),
    )
    payment_context = aggregate(payment, F.struct("transaction_id", "account_product_type", "account_status", "card_type", "card_last4", "card_status"), "payment_instrument_context")

    merchants = inputs["merchants"].select("merchant_id", F.col("name").alias("merchant_name"), "mcc", "country", "risk_rating", F.col("status").alias("merchant_status"), "_source_record_id").alias("m")
    categories = inputs["merchant_categories"].select("mcc", "category_name", "category_group", F.col("_source_record_id").alias("category_source_record_id"))
    merchant = detail.alias("t").join(merchants, F.col("t.merchant_id") == F.col("m.merchant_id"), "left").join(categories, "mcc", "left").select(
        F.col("t.case_id").alias("case_id"), F.col("m.merchant_id").alias("merchant_id"),
        "merchant_name", "mcc", "category_name", "category_group", "country", "risk_rating", "merchant_status",
        F.col("m._source_record_id").alias("merchant_source_record_id"), "category_source_record_id",
    )
    merchant_context = aggregate(merchant, F.struct("merchant_id", "merchant_name", "mcc", "category_name", "category_group", "country", "risk_rating", "merchant_status"), "merchant_context")

    auth = detail.select("case_id", "transaction_id").join(inputs["auth_attempts"], "transaction_id", "left").select("case_id", "attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts", "_source_record_id")
    authorization_context = aggregate(auth.where(F.col("attempt_id").isNotNull()), F.struct("attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"), "authorization_context")

    chargebacks = inputs["chargebacks"].select("chargeback_id", "dispute_id", "scheme", F.col("amount").cast("double").alias("amount"), "stage", "processed_at", "_source_record_id")
    chargebacks_by_dispute = chargebacks.groupBy("dispute_id").agg(
        F.sort_array(F.collect_set(F.struct("chargeback_id", "scheme", "amount", "stage", "processed_at"))).alias("chargebacks"),
        F.sort_array(F.collect_set("_source_record_id")).alias("chargeback_source_record_ids"),
    )
    reasons = inputs["dispute_reason_codes"].select("reason_code", F.col("description").alias("reason_description"), F.col("_source_record_id").alias("reason_source_record_id"))
    disputes = detail.select("case_id", "transaction_id").join(inputs["disputes"], "transaction_id", "left").join(reasons, "reason_code", "left").join(chargebacks_by_dispute, "dispute_id", "left").select(
        "case_id", "dispute_id", "transaction_id", "reason_code", "reason_description", F.col("amount").cast("decimal(18,2)").alias("amount"), "status", "raised_at", "chargebacks", "_source_record_id", "reason_source_record_id", "chargeback_source_record_ids",
    )
    disputes = disputes.where(F.col("dispute_id").isNotNull()).withColumn("chargebacks", F.coalesce("chargebacks", F.expr("CAST(array() AS array<struct<chargeback_id:string,scheme:string,amount:double,stage:string,processed_at:timestamp>>)")))
    dispute_context = aggregate(disputes, F.struct("dispute_id", "transaction_id", "reason_code", "reason_description", "amount", "status", "raised_at", "chargebacks"), "dispute_context")

    alerts = detail.select("case_id", "transaction_id").join(inputs["fraud_alerts"], "transaction_id", "left").select("case_id", "alert_id", "transaction_id", "rule_name", F.col("score").cast("double").alias("score"), "triggered_at", "disposition", "_source_record_id")
    fraud_alerts = aggregate(alerts.where(F.col("alert_id").isNotNull()), F.struct("alert_id", "transaction_id", "rule_name", "score", "triggered_at", "disposition"), "fraud_alerts")

    sources = [
        detail.select("case_id", F.lit("case_transactions").alias("source_table"), F.col("case_transaction_source_record_id").alias("source_record_id")),
        detail.select("case_id", F.lit("transactions").alias("source_table"), F.col("transaction_source_record_id").alias("source_record_id")),
        detail.join(channels, "channel_code", "left").select("case_id", F.lit("channels").alias("source_table"), F.col("channel_source_record_id").alias("source_record_id")),
        payment.select("case_id", F.lit("accounts").alias("source_table"), F.col("account_source_record_id").alias("source_record_id")),
        payment.select("case_id", F.lit("cards").alias("source_table"), F.col("card_source_record_id").alias("source_record_id")),
        merchant.select("case_id", F.lit("merchants").alias("source_table"), F.col("merchant_source_record_id").alias("source_record_id")),
        merchant.select("case_id", F.lit("merchant_categories").alias("source_table"), F.col("category_source_record_id").alias("source_record_id")),
        source_rows(auth.where(F.col("attempt_id").isNotNull()), "auth_attempts"),
        source_rows(disputes, "disputes"),
        disputes.select("case_id", F.lit("dispute_reason_codes").alias("source_table"), F.col("reason_source_record_id").alias("source_record_id")),
        disputes.select("case_id", F.lit("chargebacks").alias("source_table"), F.explode_outer("chargeback_source_record_ids").alias("source_record_id")),
        source_rows(alerts.where(F.col("alert_id").isNotNull()), "fraud_alerts"),
    ]
    return {
        "collections": [linked_transactions, payment_context, merchant_context, authorization_context, dispute_context, fraud_alerts],
        "sources": sources,
    }
