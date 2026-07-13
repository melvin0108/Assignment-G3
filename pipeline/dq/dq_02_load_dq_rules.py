# Databricks notebook source
# ============================================================================
# GOVERNANCE PIPELINE: dq_rules  (the DQ rule registry)
# ----------------------------------------------------------------------------
# Defines and populates the rule registry under gov.dq_rules — one row per
# executable data-quality rule (the 35 rule_ids the mock generator injects).
# Mirrors 05_silver_masking_policies.py / 06_silver_metadata_lineage.py: build a
# DataFrame from an explicit schema + a Python list, then saveAsTable(overwrite).
# The 35 rows below are parsed verbatim from pipeline/dq/02_load_dq_rules.sql;
# The target catalog is selected through the team-standard catalog widget.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# In a Databricks environment, `spark` is pre-initialized.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
def _catalog_widget():
    """Create the team-standard catalog widget and return its validated value.

    Mirrors pipeline/bronze/autoloader_common.py: idempotent (reuses an existing
    widget if a parent notebook or job parameter already set one) and validated
    against the team's dev/test/prod catalogs (g3_dev / g3_test / g3_catalog).
    """
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


catalog = _catalog_widget()
SCHEMA = "gov"
TABLE_NAME = "dq_rules"
FULL_TABLE_NAME = f"{catalog}.{SCHEMA}.{TABLE_NAME}"

# Schema matches gov.dq_rules DDL in 01_setup.sql (rule_id .. enabled).
schema = StructType([
    StructField("rule_id", StringType(), nullable=True),
    StructField("rule_name", StringType(), nullable=True),
    StructField("layer", StringType(), nullable=True),
    StructField("target_table", StringType(), nullable=True),
    StructField("target_key", StringType(), nullable=True),
    StructField("pattern", StringType(), nullable=True),
    StructField("severity", StringType(), nullable=True),
    StructField("expression", StringType(), nullable=True),
    StructField("enabled", BooleanType(), nullable=True),
])

# The 36 DQ rules (single_row=13, duplicate=6, fk_anti_join=13, text_pii=2,
# ai_exclusion=2). severity is 'quarantine' for all runtime DQ outputs.
data = [
    ("DQ-TXN-AMT-POS", "amount must be > 0", "bronze", "transactions", "transaction_id", "single_row", "quarantine", "CAST(amount AS DOUBLE) <= 0", True),
    ("DQ-TXN-MERCH-REQ", "merchant_id is required", "bronze", "transactions", "transaction_id", "single_row", "quarantine", "merchant_id IS NULL OR merchant_id = ''", True),
    ("DQ-TXN-TS-FUTURE", "txn_ts must not be in the future", "bronze", "transactions", "transaction_id", "single_row", "quarantine", "txn_ts > RUN_DATE (2026-07-06)", True),
    ("DQ-ACC-OPENDATE-FUTURE", "open_date must not be in the future", "bronze", "accounts", "account_id", "single_row", "quarantine", "open_date > RUN_DATE (2026-07-06)", True),
    ("DQ-CUST-EMAIL-FMT", "email must match pattern if present", "bronze", "customers", "customer_id", "single_row", "quarantine", "email NOT RLIKE email-pattern (catches empty/malformed)", True),
    ("DQ-CARD-EXPIRED-ACTIVE", "active card must not have a past expiry", "bronze", "cards", "card_id", "single_row", "quarantine", "status = 'active' AND expiry < RUN_DATE", True),
    ("DQ-MERCH-RISK-CASING", "risk_rating must be in {low,medium,high}", "bronze", "merchants", "merchant_id", "single_row", "quarantine", "risk_rating NOT IN ('low','medium','high')", True),
    ("DQ-DISP-STATUS-ENUM", "status must be a lowercase dispute enum", "bronze", "disputes", "dispute_id", "single_row", "quarantine", "status NOT IN dispute_status enum", True),
    ("DQ-DISP-REASON-REQ", "reason_code is required", "bronze", "disputes", "dispute_id", "single_row", "quarantine", "reason_code IS NULL OR reason_code = ''", True),
    ("DQ-ALT-SCORE-RANGE", "score must be within [0,1]", "bronze", "fraud_alerts", "alert_id", "single_row", "quarantine", "CAST(score AS DOUBLE) NOT BETWEEN 0 AND 1", True),
    ("DQ-DEV-TYPE-REQ", "device_type is required", "bronze", "transaction_devices", "device_id", "single_row", "quarantine", "device_type IS NULL OR device_type = ''", True),
    ("DQ-CASE-STATUS-ENUM", "status_code must be in case_status enum", "bronze", "investigation_cases", "case_id", "single_row", "quarantine", "status_code NOT IN case_status enum", True),
    ("DQ-CASE-STALE", "open cases older than 180 days are stale", "bronze", "investigation_cases", "case_id", "single_row", "quarantine", "status_code = 'open' AND opened_at < RUN_DATE - 180 days", True),
    ("DQ-CUST-ID-DUP", "(customer_id, effective_at) must be unique", "bronze", "customers", "customer_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY customer_id, effective_at) > 1  (SCD2: same customer_id + later effective_at is a new version, not a dup)", True),
    ("DQ-TXN-ID-DUP", "transaction_id must be unique", "bronze", "transactions", "transaction_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY transaction_id) > 1", True),
    ("DQ-CARD-DUP", "(card_id, effective_at) must be unique", "bronze", "cards", "card_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY card_id, effective_at) > 1  (SCD2: same card_id + later effective_at is a new version, not a dup)", True),
    ("DQ-EMP-EMAIL-UNIQ", "email must be unique", "bronze", "employees", "employee_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY email) > 1", True),
    ("DQ-CUST-NEAR-DUP", "no two customers share name+dob+address+tax_id", "bronze", "customers", "customer_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY first_name,last_name,dob,address,tax_id) > 1  (exact first pass; fuzzy TODO)", True),
    ("DQ-EMP-NAME-NEAR-DUP", "flag near-duplicate employee names", "bronze", "employees", "employee_id", "duplicate", "quarantine", "row_number() OVER (PARTITION BY full_name) > 1  (exact first pass; fuzzy TODO)", True),
    ("DQ-ACC-CUST-FK", "customer_id must exist in customers", "bronze", "accounts", "account_id", "fk_anti_join", "quarantine", "accounts.customer_id NOT IN customers.customer_id", True),
    ("DQ-TXN-ACCT-FK", "account_id must exist in accounts", "bronze", "transactions", "transaction_id", "fk_anti_join", "quarantine", "transactions.account_id NOT IN accounts.account_id", True),
    ("DQ-TXN-CARD-FK", "card_id must exist in cards", "bronze", "transactions", "transaction_id", "fk_anti_join", "quarantine", "transactions.card_id NOT IN cards.card_id", True),
    ("DQ-AUTH-TXN-FK", "transaction_id must exist in transactions", "bronze", "auth_attempts", "attempt_id", "fk_anti_join", "quarantine", "auth_attempts.transaction_id NOT IN transactions.transaction_id", True),
    ("DQ-DISP-TXN-FK", "transaction_id must exist in transactions", "bronze", "disputes", "dispute_id", "fk_anti_join", "quarantine", "disputes.transaction_id NOT IN transactions.transaction_id", True),
    ("DQ-CBK-DISP-FK", "dispute_id must exist in disputes", "bronze", "chargebacks", "chargeback_id", "fk_anti_join", "quarantine", "chargebacks.dispute_id NOT IN disputes.dispute_id", True),
    ("DQ-DEV-TXN-FK", "transaction_id must exist in transactions", "bronze", "transaction_devices", "device_id", "fk_anti_join", "quarantine", "transaction_devices.transaction_id NOT IN transactions.transaction_id", True),
    ("DQ-CASETXN-TXN-FK", "transaction_id must exist in transactions", "bronze", "case_transactions", "case_id|transaction_id", "fk_anti_join", "quarantine", "case_transactions.transaction_id NOT IN transactions.transaction_id", True),
    ("DQ-TXN-CARD-ACTIVE", "transaction must use an active card", "bronze", "transactions", "transaction_id", "fk_anti_join", "quarantine", "JOIN cards ON card_id WHERE cards.status = 'closed'", True),
    ("DQ-AUTH-TS-ORDER", "auth_ts must not be later than txn_ts", "bronze", "auth_attempts", "attempt_id", "fk_anti_join", "quarantine", "JOIN transactions: auth_ts > txn_ts", True),
    ("DQ-CASEPARTY-RESOLVE", "party_id must resolve per party_type", "bronze", "case_parties", "case_id|party_type|party_id", "fk_anti_join", "quarantine", "conditional FK: party_id resolved against customers/merchants by party_type", True),
    ("DQ-CASEPARTY-TYPE-ENUM", "party_type must be in {customer,merchant,third_party}", "bronze", "case_parties", "case_id|party_type|party_id", "fk_anti_join", "quarantine", "party_type NOT IN ('customer','merchant','third_party')", True),
    ("DQ-CTL-DNC-VIOLATION", "no outbound contact when do_not_contact=true", "bronze", "customer_contact_logs", "contact_id", "fk_anti_join", "quarantine", "direction = 'outbound' AND do_not_contact = 'true'", True),
    ("DQ-NOTE-PII-LEAK", "note_text must not contain raw PII/PAN", "bronze", "investigation_notes", "note_id", "text_pii", "quarantine", "note_text RLIKE email|phone|PAN", True),
    ("DQ-CTL-NOTE-PII", "note must not contain raw PII/PAN", "bronze", "customer_contact_logs", "contact_id", "text_pii", "quarantine", "note RLIKE email|phone|PAN", True),
    ("DQ-CASE-LEGALHOLD", "legal_hold cases excluded from AI output", "bronze", "investigation_cases", "case_id", "ai_exclusion", "quarantine", "legal_hold = 'true'", True),
    ("DQ-NOTE-LEGALHOLD", "notes on legal_hold cases must not reach AI", "bronze", "investigation_notes", "note_id", "ai_exclusion", "quarantine", "JOIN investigation_cases ON case_id WHERE legal_hold = 'true'", True),
]

# ---------------------------------------------------------------------------
# BUILD + WRITE
# ---------------------------------------------------------------------------
df = spark.createDataFrame(data, schema=schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

print(f"Writing DQ rule registry to {FULL_TABLE_NAME}")
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)
print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# VERIFY & DESCRIBE  (mirrors the VERIFY block of 02_load_dq_rules.sql)
# ---------------------------------------------------------------------------
print("\nRule count (expected 35):")
spark.sql(f"SELECT COUNT(*) AS rule_count FROM {FULL_TABLE_NAME}").show()

print("Counts per pattern (expected single_row=13, duplicate=6, fk_anti_join=12, text_pii=2, ai_exclusion=2):")
spark.sql(
    f"SELECT pattern, COUNT(*) AS n FROM {FULL_TABLE_NAME} "
    f"GROUP BY pattern ORDER BY n DESC"
).show()

print("Registry drift vs defects_manifest (expected 0 rows):")
spark.sql(
    f"SELECT r.rule_id FROM {FULL_TABLE_NAME} r "
    f"LEFT JOIN (SELECT DISTINCT rule_id FROM {catalog}.bronze.defects_manifest) m "
    f"ON r.rule_id = m.rule_id WHERE m.rule_id IS NULL"
).show()

print("\nVerifying DQ Rule Registry:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} ORDER BY pattern, rule_id").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)