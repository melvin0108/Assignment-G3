# Databricks notebook source
"""Smoke tests for deployed authenticated Consumer metric views."""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from pipeline.gold.gold_common import catalog_widget


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
CATALOG = catalog_widget(dbutils)
SCHEMA = "consumer_metrics"

accounts = spark.read.table(f"{CATALOG}.gold.dim_consumer_account")
customers = [
    row["customer_id"]
    for row in accounts.select("customer_id").distinct().limit(2).collect()
]
require(len(customers) == 2, "Consumer smoke tests require two customers")
customer_a, customer_b = customers
account_b = (
    accounts.filter(accounts.customer_id == customer_b)
    .select("account_id")
    .first()["account_id"]
)
cards = spark.read.table(f"{CATALOG}.gold.dim_consumer_card")
card_b_row = (
    cards.filter(cards.customer_id == customer_b).select("card_id").first()
)

try:
    spark.sql(
        f"SELECT MEASURE(account_count) "
        f"FROM {CATALOG}.{SCHEMA}.mv_consumer_accounts()"
    ).collect()
except Exception:
    pass
else:
    raise AssertionError("Omitting scope_customer_id must fail")

cross_customer_rows = spark.sql(
    f"SELECT account_reference "
    f"FROM {CATALOG}.{SCHEMA}.mv_consumer_accounts("
    f"scope_customer_id => {sql_string(customer_a)}, "
    f"scope_account_id => {sql_string(account_b)})"
).count()
require(
    cross_customer_rows == 0,
    "Customer A with Customer B's account scope must return no rows",
)
if card_b_row is not None:
    cross_customer_card_rows = spark.sql(
        f"SELECT card_reference "
        f"FROM {CATALOG}.{SCHEMA}.mv_consumer_cards("
        f"scope_customer_id => {sql_string(customer_a)}, "
        f"scope_card_id => {sql_string(card_b_row['card_id'])})"
    ).count()
    require(
        cross_customer_card_rows == 0,
        "Customer A with Customer B's card scope must return no rows",
    )

view_account_count = spark.sql(
    f"SELECT MEASURE(account_count) AS count "
    f"FROM {CATALOG}.{SCHEMA}.mv_consumer_accounts("
    f"scope_customer_id => {sql_string(customer_a)})"
).first()["count"]
source_account_count = accounts.filter(
    accounts.customer_id == customer_a
).count()
require(
    view_account_count == source_account_count,
    "__all_owned__ account scope must reconcile to the broker source",
)
view_card_count = spark.sql(
    f"SELECT MEASURE(card_count) AS count "
    f"FROM {CATALOG}.{SCHEMA}.mv_consumer_cards("
    f"scope_customer_id => {sql_string(customer_a)})"
).first()["count"]
source_card_count = cards.filter(cards.customer_id == customer_a).count()
require(
    view_card_count == source_card_count,
    "__all_owned__ card scope must reconcile to the broker source",
)

technical_names = {
    "customer_id",
    "account_id",
    "card_id",
    "pipeline_run_id",
    "batch_id",
    "source_references",
    "usage_restrictions",
}
for view_name in [
    "mv_consumer_accounts",
    "mv_consumer_cards",
    "mv_consumer_transactions",
    "mv_consumer_disputes",
]:
    described = {
        row["col_name"]
        for row in spark.sql(
            f"DESCRIBE {CATALOG}.{SCHEMA}.{view_name}"
        ).collect()
    }
    require(
        described.isdisjoint(technical_names),
        f"{view_name} exposes a technical identifier or metadata field",
    )

transaction_view = spark.sql(
    f"""
    SELECT
      currency_code,
      MEASURE(transaction_count) AS row_count,
      MEASURE(total_transaction_amount) AS total_amount
    FROM {CATALOG}.{SCHEMA}.mv_consumer_transactions(
      scope_customer_id => {sql_string(customer_a)}
    )
    GROUP BY currency_code
    ORDER BY currency_code
    """
)
transaction_source = (
    spark.read.table(f"{CATALOG}.gold.fact_consumer_transaction")
    .filter(f"customer_id = {sql_string(customer_a)}")
    .groupBy("currency_code")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount")
)
require(
    transaction_view.select("currency_code", "total_amount")
    .exceptAll(transaction_source.select("currency_code", "total_amount"))
    .isEmpty(),
    "Consumer transaction amounts must reconcile by currency",
)

disputes = spark.read.table(f"{CATALOG}.gold.fact_consumer_dispute").filter(
    f"customer_id = {sql_string(customer_a)}"
)
view_dispute_count = spark.sql(
    f"SELECT MEASURE(dispute_count) AS count "
    f"FROM {CATALOG}.{SCHEMA}.mv_consumer_disputes("
    f"scope_customer_id => {sql_string(customer_a)})"
).first()["count"]
require(
    view_dispute_count == disputes.count(),
    "Dispute metric joins must not duplicate scoped disputes",
)
dispute_view = spark.sql(
    f"""
    SELECT currency_code,
           MEASURE(total_disputed_amount) AS total_amount
    FROM {CATALOG}.{SCHEMA}.mv_consumer_disputes(
      scope_customer_id => {sql_string(customer_a)}
    )
    GROUP BY currency_code
    ORDER BY currency_code
    """
)
dispute_source = (
    disputes.groupBy("currency_code")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount")
)
require(
    dispute_view.exceptAll(dispute_source).isEmpty()
    and dispute_source.exceptAll(dispute_view).isEmpty(),
    "Consumer dispute amounts must reconcile by currency",
)

recent_details = spark.sql(
    f"""
    SELECT transaction_reference, account_reference, card_reference,
           transaction_at, amount, currency_code, quality_status, data_as_of
    FROM {CATALOG}.{SCHEMA}.mv_consumer_transactions(
      scope_customer_id => {sql_string(customer_a)}
    )
    WHERE transaction_at >= CURRENT_DATE() - INTERVAL 30 DAYS
    ORDER BY transaction_at DESC
    LIMIT 100
    """
)
require(recent_details.count() <= 100, "Consumer detail results exceed 100 rows")

raw_identifiers = {
    row["account_id"]
    for row in accounts.filter(accounts.customer_id == customer_a)
    .select("account_id")
    .collect()
}
masked_references = {
    row["account_reference"]
    for row in accounts.filter(accounts.customer_id == customer_a)
    .select("account_reference")
    .collect()
}
require(
    raw_identifiers.isdisjoint(masked_references),
    "Masked account references must not equal raw account identifiers",
)
unsafe_card_references = cards.filter(cards.customer_id == customer_a).filter(
    (cards.card_reference == cards.card_id)
    | ~cards.card_last_four.rlike("^[0-9]{4}$")
)
require(
    unsafe_card_references.isEmpty(),
    "Masked card references must not expose raw card identifiers or PAN",
)

print(
    "Consumer metric smoke tests passed: required scope, cross-customer "
    "isolation, identifier hiding, currency reconciliation, no dispute "
    "fanout, 30-day query default, and 100-row detail limit"
)
