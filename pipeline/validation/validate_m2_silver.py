# Databricks notebook source
# ============================================================================
# Validation: M2 Silver contract/readiness
# ----------------------------------------------------------------------------
# Run this after:
#   1. Bronze ingestion
#   2. DQ setup/rule loading/failure capture
#   3. pipeline/silver/silver_all_tables.py
#
# This validation is intentionally Silver-scoped. It does not re-test Bronze
# ingestion, DQ manifest reconciliation, or Gold model output. A failed check
# raises an exception so the output can be used as executable validation
# evidence.
# ============================================================================

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
catalog = dbutils.widgets.get("catalog")
if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
    raise ValueError(f"Unsupported catalog: {catalog}")

RUN_DATE = "2026-07-06"
PII_PATTERN_SQL = (
    r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})"
    r"|(\\+\\d{6,15})"
    r"|(\\b\\d{13,19}\\b)"
    r"|(\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b)"
)

CLEAN_SILVER_TABLES = [
    "date_dim",
    "defects_manifest",
    "countries",
    "currencies",
    "branches",
    "channels",
    "merchant_categories",
    "dispute_reason_codes",
    "fraud_types",
    "case_status_types",
    "customers",
    "employees",
    "accounts",
    "cards",
    "merchants",
    "transactions",
    "auth_attempts",
    "transaction_devices",
    "disputes",
    "chargebacks",
    "fraud_alerts",
    "investigation_cases",
    "investigation_notes",
    "case_transactions",
    "case_parties",
    "customer_contact_logs",
]

EXPECTED_SILVER_TABLES = CLEAN_SILVER_TABLES + ["quarantine_records"]
EXPECTED_GOV_TABLES = ["masking_policies", "metadata_lineage"]

GOLD_INPUT_SILVER_TABLES = [
    "date_dim",
    "investigation_cases",
    "merchants",
    "merchant_categories",
    "channels",
    "dispute_reason_codes",
    "currencies",
    "transactions",
    "auth_attempts",
    "disputes",
    "chargebacks",
    "fraud_alerts",
    "investigation_notes",
    "case_transactions",
    "case_parties",
]

SILVER_METADATA_COLUMNS = [
    "_source_file",
    "_source_file_mod_time",
    "_ingest_ts",
    "_run_id",
    "_batch_id",
    "_source_record_id",
    "_record_hash",
]

GRAIN_KEYS = {
    "date_dim": ["date_id"],
    "countries": ["iso_code"],
    "currencies": ["currency_code"],
    "branches": ["branch_code"],
    "channels": ["channel_code"],
    "merchant_categories": ["mcc"],
    "dispute_reason_codes": ["reason_code"],
    "fraud_types": ["fraud_type_code"],
    "case_status_types": ["status_code"],
    "customers": ["customer_id"],
    "employees": ["employee_id"],
    "accounts": ["account_id"],
    "cards": ["card_id"],
    "merchants": ["merchant_id"],
    "transactions": ["transaction_id"],
    "auth_attempts": ["attempt_id"],
    "transaction_devices": ["device_id"],
    "disputes": ["dispute_id"],
    "chargebacks": ["chargeback_id"],
    "fraud_alerts": ["alert_id"],
    "investigation_cases": ["case_id"],
    "investigation_notes": ["note_id"],
    "case_transactions": ["case_id", "transaction_id"],
    "case_parties": ["case_id", "party_type", "party_id"],
    "customer_contact_logs": ["contact_id"],
}

TYPE_CONTRACTS = [
    ("date_dim", "date_id", "date"),
    ("date_dim", "year", "int"),
    ("date_dim", "month", "int"),
    ("date_dim", "quarter", "int"),
    ("date_dim", "is_weekend", "boolean"),
    ("currencies", "decimals", "int"),
    ("customers", "created_at", "timestamp"),
    ("accounts", "open_date", "date"),
    ("merchants", "effective_at", "timestamp"),
    ("transactions", "amount", "decimal"),
    ("transactions", "txn_ts", "timestamp"),
    ("auth_attempts", "auth_ts", "timestamp"),
    ("disputes", "amount", "decimal"),
    ("disputes", "raised_at", "timestamp"),
    ("chargebacks", "amount", "numeric"),
    ("chargebacks", "processed_at", "timestamp"),
    ("fraud_alerts", "score", "double"),
    ("fraud_alerts", "triggered_at", "timestamp"),
    ("investigation_cases", "opened_at", "timestamp"),
    ("investigation_cases", "closed_at", "timestamp"),
    ("investigation_cases", "legal_hold", "boolean"),
    ("investigation_notes", "created_at", "timestamp"),
    ("case_transactions", "linked_at", "timestamp"),
    ("customer_contact_logs", "do_not_contact", "boolean"),
    ("customer_contact_logs", "contacted_at", "timestamp"),
]

TYPE_PREDICATES = {
    "date": "LOWER(data_type) = 'date'",
    "timestamp": "LOWER(data_type) = 'timestamp'",
    "int": "LOWER(data_type) IN ('int', 'integer')",
    "boolean": "LOWER(data_type) = 'boolean'",
    "double": "LOWER(data_type) = 'double'",
    "decimal": "LOWER(data_type) LIKE 'decimal%'",
    "numeric": "(LOWER(data_type) LIKE 'decimal%' OR LOWER(data_type) IN ('double', 'float'))",
}

MASKING_POLICY_FIELDS = [
    ("customers", "first_name"),
    ("customers", "last_name"),
    ("customers", "email"),
    ("customers", "phone"),
    ("customers", "address"),
    ("customers", "dob"),
    ("customers", "tax_id"),
    ("cards", "pan"),
    ("employees", "full_name"),
    ("employees", "email"),
    ("transaction_devices", "device_id"),
    ("transaction_devices", "ip"),
]


def sql(query):
    return spark.sql(query)


def values(items):
    return ", ".join(f"('{item}')" for item in items)


def pair_values(items):
    return ", ".join(f"('{left}', '{right}')" for left, right in items)


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


def fail_if_not_one_snapshot():
    identities = {}
    for table_name in CLEAN_SILVER_TABLES:
        rows = sql(
            f"""
            SELECT _batch_id, _run_id
            FROM {catalog}.silver.{table_name}
            GROUP BY _batch_id, _run_id
            LIMIT 2
            """
        ).collect()
        if len(rows) != 1 or rows[0]["_batch_id"] is None or rows[0]["_run_id"] is None:
            raise Exception(
                f"Validation failed: silver.{table_name} must contain exactly "
                "one non-null _batch_id/_run_id identity"
            )
        identities[table_name] = (rows[0]["_batch_id"], rows[0]["_run_id"])

    distinct_identities = set(identities.values())
    if len(distinct_identities) != 1:
        details = ", ".join(
            f"{table_name}={batch_id}/{run_id}"
            for table_name, (batch_id, run_id) in sorted(identities.items())
        )
        raise Exception("Validation failed: Silver tables have inconsistent snapshots: " + details)

    batch_id, run_id = distinct_identities.pop()
    print(f"PASS: all clean Silver tables share snapshot batch {batch_id}, run {run_id}")
    return batch_id, run_id


def required_key_condition(columns):
    return " OR ".join(
        f"{column} IS NULL OR TRIM(CAST({column} AS STRING)) = ''"
        for column in columns
    )


print("=== M2 Silver validation ===")

fail_if_rows(
    "all expected Silver tables exist",
    f"""
    WITH expected(table_name) AS (VALUES {values(EXPECTED_SILVER_TABLES)})
    SELECT e.table_name
    FROM expected e
    LEFT JOIN {catalog}.information_schema.tables t
      ON t.table_schema = 'silver'
     AND t.table_name = e.table_name
    WHERE t.table_name IS NULL
    ORDER BY e.table_name
    """,
)

fail_if_rows(
    "expected Silver governance tables exist in gov schema",
    f"""
    WITH expected(table_name) AS (VALUES {values(EXPECTED_GOV_TABLES)})
    SELECT e.table_name
    FROM expected e
    LEFT JOIN {catalog}.information_schema.tables t
      ON t.table_schema = 'gov'
     AND t.table_name = e.table_name
    WHERE t.table_name IS NULL
    ORDER BY e.table_name
    """,
)

for table_name in CLEAN_SILVER_TABLES:
    fail_if_zero(
        f"silver.{table_name} has rows",
        f"SELECT COUNT(*) FROM {catalog}.silver.{table_name}",
    )

SNAPSHOT_BATCH_ID, SILVER_RUN_ID = fail_if_not_one_snapshot()
DQ_RUN_ID = f"{SILVER_RUN_ID}-DQ"
print(f"INFO: validating Silver run {SILVER_RUN_ID} and DQ run {DQ_RUN_ID}")

expected_metadata_pairs = [
    (table_name, column_name)
    for table_name in CLEAN_SILVER_TABLES
    for column_name in SILVER_METADATA_COLUMNS
]
fail_if_rows(
    "clean Silver tables have required metadata columns",
    f"""
    WITH expected(table_name, column_name) AS (VALUES {pair_values(expected_metadata_pairs)})
    SELECT e.table_name, e.column_name AS missing_column
    FROM expected e
    LEFT JOIN {catalog}.information_schema.columns c
      ON c.table_schema = 'silver'
     AND c.table_name = e.table_name
     AND c.column_name = e.column_name
    WHERE c.column_name IS NULL
    ORDER BY e.table_name, e.column_name
    """,
)

fail_if_rows(
    "Bronze-only rescue column is not promoted to clean Silver",
    f"""
    SELECT table_name, column_name
    FROM {catalog}.information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name IN ({", ".join(f"'{table_name}'" for table_name in CLEAN_SILVER_TABLES)})
      AND column_name = '_rescued_data'
    ORDER BY table_name
    """,
)

for table_name, column_name, expected_kind in TYPE_CONTRACTS:
    predicate = TYPE_PREDICATES[expected_kind]
    fail_if_rows(
        f"silver.{table_name}.{column_name} has {expected_kind} contract type",
        f"""
        SELECT table_name, column_name, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = 'silver'
          AND table_name = '{table_name}'
          AND column_name = '{column_name}'
          AND NOT ({predicate})
        UNION ALL
        SELECT '{table_name}', '{column_name}', '<missing>'
        WHERE NOT EXISTS (
          SELECT 1
          FROM {catalog}.information_schema.columns
          WHERE table_schema = 'silver'
            AND table_name = '{table_name}'
            AND column_name = '{column_name}'
        )
        """,
    )

for table_name in CLEAN_SILVER_TABLES:
    fail_if_rows(
        f"silver.{table_name} excludes blocking quarantine records",
        f"""
        SELECT q.run_id, q.source_table, q.source_record_id, q.record_key,
               q.rule_id, q.disposition
        FROM {catalog}.silver.quarantine_records q
        JOIN {catalog}.silver.{table_name} s
          ON q.source_record_id = s._source_record_id
        WHERE q.source_table = '{table_name}'
          AND q.run_id IN (s._run_id, CONCAT(s._run_id, '-DQ'))
          AND LOWER(q.disposition) IN ('quarantined', 'rejected')
        LIMIT 20
        """,
    )

    warn_if_rows(
        f"silver.{table_name} contains non-blocking quarantine warnings",
        f"""
        SELECT q.run_id, q.source_table, q.source_record_id, q.record_key,
               q.rule_id, q.disposition
        FROM {catalog}.silver.quarantine_records q
        JOIN {catalog}.silver.{table_name} s
          ON q.source_record_id = s._source_record_id
        WHERE q.source_table = '{table_name}'
          AND q.run_id IN (s._run_id, CONCAT(s._run_id, '-DQ'))
          AND LOWER(q.disposition) IN ('allowed_with_warning', 'masked')
        LIMIT 20
        """,
    )

for table_name, columns in GRAIN_KEYS.items():
    column_list = ", ".join(columns)
    fail_if_rows(
        f"silver.{table_name} grain is unique",
        f"""
        SELECT {column_list}, COUNT(*) AS row_count
        FROM {catalog}.silver.{table_name}
        GROUP BY {column_list}
        HAVING COUNT(*) > 1
        LIMIT 20
        """,
    )
    fail_if_rows(
        f"silver.{table_name} grain keys are populated",
        f"""
        SELECT *
        FROM {catalog}.silver.{table_name}
        WHERE {required_key_condition(columns)}
        LIMIT 20
        """,
    )

fail_if_rows(
    "silver.defects_manifest required fields are populated",
    f"""
    SELECT *
    FROM {catalog}.silver.defects_manifest
    WHERE source_table IS NULL OR TRIM(source_table) = ''
       OR record_key IS NULL OR TRIM(record_key) = ''
       OR rule_id IS NULL OR TRIM(rule_id) = ''
       OR rule_name IS NULL OR TRIM(rule_name) = ''
       OR failure_reason IS NULL OR TRIM(failure_reason) = ''
       OR severity IS NULL OR TRIM(severity) = ''
    LIMIT 20
    """,
)

warn_if_rows(
    "silver.defects_manifest has repeated source_table/record_key/rule_id labels",
    f"""
    SELECT source_table, record_key, rule_id, COUNT(*) AS row_count
    FROM {catalog}.silver.defects_manifest
    GROUP BY source_table, record_key, rule_id
    HAVING COUNT(*) > 1
    LIMIT 20
    """,
)

RI_CHECKS = [
    (
        "accounts.customer_id resolves to customers",
        f"""
        SELECT a.account_id, a.customer_id
        FROM {catalog}.silver.accounts a
        LEFT JOIN {catalog}.silver.customers c ON a.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "accounts.currency resolves to currencies",
        f"""
        SELECT a.account_id, a.currency
        FROM {catalog}.silver.accounts a
        LEFT JOIN {catalog}.silver.currencies c ON a.currency = c.currency_code
        WHERE c.currency_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "cards.account_id resolves to accounts",
        f"""
        SELECT c.card_id, c.account_id
        FROM {catalog}.silver.cards c
        LEFT JOIN {catalog}.silver.accounts a ON c.account_id = a.account_id
        WHERE a.account_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "branches.country resolves to countries",
        f"""
        SELECT b.branch_code, b.country
        FROM {catalog}.silver.branches b
        LEFT JOIN {catalog}.silver.countries c ON b.country = c.iso_code
        WHERE c.iso_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "merchants.mcc resolves to merchant_categories",
        f"""
        SELECT m.merchant_id, m.mcc
        FROM {catalog}.silver.merchants m
        LEFT JOIN {catalog}.silver.merchant_categories mc ON m.mcc = mc.mcc
        WHERE mc.mcc IS NULL
        LIMIT 20
        """,
    ),
    (
        "merchants.country resolves to countries",
        f"""
        SELECT m.merchant_id, m.country
        FROM {catalog}.silver.merchants m
        LEFT JOIN {catalog}.silver.countries c ON m.country = c.iso_code
        WHERE c.iso_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "transactions.account_id resolves to accounts",
        f"""
        SELECT t.transaction_id, t.account_id
        FROM {catalog}.silver.transactions t
        LEFT JOIN {catalog}.silver.accounts a ON t.account_id = a.account_id
        WHERE a.account_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "transactions.card_id resolves to cards",
        f"""
        SELECT t.transaction_id, t.card_id
        FROM {catalog}.silver.transactions t
        LEFT JOIN {catalog}.silver.cards c ON t.card_id = c.card_id
        WHERE c.card_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "transactions.merchant_id resolves to merchants",
        f"""
        SELECT t.transaction_id, t.merchant_id
        FROM {catalog}.silver.transactions t
        LEFT JOIN {catalog}.silver.merchants m ON t.merchant_id = m.merchant_id
        WHERE m.merchant_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "transactions.channel resolves to channels",
        f"""
        SELECT t.transaction_id, t.channel
        FROM {catalog}.silver.transactions t
        LEFT JOIN {catalog}.silver.channels c ON t.channel = c.channel_code
        WHERE c.channel_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "transactions.currency resolves to currencies",
        f"""
        SELECT t.transaction_id, t.currency
        FROM {catalog}.silver.transactions t
        LEFT JOIN {catalog}.silver.currencies c ON t.currency = c.currency_code
        WHERE c.currency_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "auth_attempts.transaction_id resolves to transactions",
        f"""
        SELECT a.attempt_id, a.transaction_id
        FROM {catalog}.silver.auth_attempts a
        LEFT JOIN {catalog}.silver.transactions t ON a.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "transaction_devices.transaction_id resolves to transactions",
        f"""
        SELECT d.device_id, d.transaction_id
        FROM {catalog}.silver.transaction_devices d
        LEFT JOIN {catalog}.silver.transactions t ON d.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "transaction_devices.geo_country resolves to countries when present",
        f"""
        SELECT d.device_id, d.geo_country
        FROM {catalog}.silver.transaction_devices d
        LEFT JOIN {catalog}.silver.countries c ON d.geo_country = c.iso_code
        WHERE d.geo_country IS NOT NULL
          AND TRIM(d.geo_country) <> ''
          AND c.iso_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "disputes.transaction_id resolves to transactions",
        f"""
        SELECT d.dispute_id, d.transaction_id
        FROM {catalog}.silver.disputes d
        LEFT JOIN {catalog}.silver.transactions t ON d.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "disputes.reason_code resolves to dispute_reason_codes",
        f"""
        SELECT d.dispute_id, d.reason_code
        FROM {catalog}.silver.disputes d
        LEFT JOIN {catalog}.silver.dispute_reason_codes r ON d.reason_code = r.reason_code
        WHERE r.reason_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "chargebacks.dispute_id resolves to disputes",
        f"""
        SELECT c.chargeback_id, c.dispute_id
        FROM {catalog}.silver.chargebacks c
        LEFT JOIN {catalog}.silver.disputes d ON c.dispute_id = d.dispute_id
        WHERE d.dispute_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "fraud_alerts.transaction_id resolves to transactions",
        f"""
        SELECT a.alert_id, a.transaction_id
        FROM {catalog}.silver.fraud_alerts a
        LEFT JOIN {catalog}.silver.transactions t ON a.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "investigation_cases.status_code resolves to case_status_types",
        f"""
        SELECT c.case_id, c.status_code
        FROM {catalog}.silver.investigation_cases c
        LEFT JOIN {catalog}.silver.case_status_types s ON c.status_code = s.status_code
        WHERE s.status_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "investigation_cases.fraud_type_code resolves to fraud_types",
        f"""
        SELECT c.case_id, c.fraud_type_code
        FROM {catalog}.silver.investigation_cases c
        LEFT JOIN {catalog}.silver.fraud_types f ON c.fraud_type_code = f.fraud_type_code
        WHERE f.fraud_type_code IS NULL
        LIMIT 20
        """,
    ),
    (
        "investigation_cases.owner_employee_id resolves to employees",
        f"""
        SELECT c.case_id, c.owner_employee_id
        FROM {catalog}.silver.investigation_cases c
        LEFT JOIN {catalog}.silver.employees e ON c.owner_employee_id = e.employee_id
        WHERE e.employee_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "investigation_notes.case_id resolves to investigation_cases",
        f"""
        SELECT n.note_id, n.case_id
        FROM {catalog}.silver.investigation_notes n
        LEFT JOIN {catalog}.silver.investigation_cases c ON n.case_id = c.case_id
        WHERE c.case_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "investigation_notes.author_employee_id resolves to employees",
        f"""
        SELECT n.note_id, n.author_employee_id
        FROM {catalog}.silver.investigation_notes n
        LEFT JOIN {catalog}.silver.employees e ON n.author_employee_id = e.employee_id
        WHERE e.employee_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "case_transactions.case_id resolves to investigation_cases",
        f"""
        SELECT ct.case_id, ct.transaction_id
        FROM {catalog}.silver.case_transactions ct
        LEFT JOIN {catalog}.silver.investigation_cases c ON ct.case_id = c.case_id
        WHERE c.case_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "case_transactions.transaction_id resolves to transactions",
        f"""
        SELECT ct.case_id, ct.transaction_id
        FROM {catalog}.silver.case_transactions ct
        LEFT JOIN {catalog}.silver.transactions t ON ct.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "case_parties.case_id resolves to investigation_cases",
        f"""
        SELECT cp.case_id, cp.party_type, cp.party_id
        FROM {catalog}.silver.case_parties cp
        LEFT JOIN {catalog}.silver.investigation_cases c ON cp.case_id = c.case_id
        WHERE c.case_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "customer_contact_logs.customer_id resolves to customers",
        f"""
        SELECT cl.contact_id, cl.customer_id
        FROM {catalog}.silver.customer_contact_logs cl
        LEFT JOIN {catalog}.silver.customers c ON cl.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        LIMIT 20
        """,
    ),
    (
        "customer_contact_logs.employee_id resolves to employees",
        f"""
        SELECT cl.contact_id, cl.employee_id
        FROM {catalog}.silver.customer_contact_logs cl
        LEFT JOIN {catalog}.silver.employees e ON cl.employee_id = e.employee_id
        WHERE e.employee_id IS NULL
        LIMIT 20
        """,
    ),
]

for name, query in RI_CHECKS:
    fail_if_rows(name, query)

fail_if_rows(
    "case_parties resolve party_id by party_type",
    f"""
    SELECT cp.case_id, cp.party_type, cp.party_id
    FROM {catalog}.silver.case_parties cp
    LEFT JOIN {catalog}.silver.customers c
      ON cp.party_type = 'customer' AND cp.party_id = c.customer_id
    LEFT JOIN {catalog}.silver.merchants m
      ON cp.party_type = 'merchant' AND cp.party_id = m.merchant_id
    WHERE cp.party_type NOT IN ('customer', 'merchant', 'third_party')
       OR (cp.party_type = 'customer' AND c.customer_id IS NULL)
       OR (cp.party_type = 'merchant' AND m.merchant_id IS NULL)
       OR (cp.party_type = 'third_party' AND cp.party_id NOT RLIKE '^TP-[0-9]{{4}}$')
    LIMIT 20
    """,
)

BUSINESS_RULE_CHECKS = [
    (
        "transactions are positive and not future-dated",
        f"""
        SELECT transaction_id, amount, txn_ts
        FROM {catalog}.silver.transactions
        WHERE amount <= 0
           OR txn_ts > TIMESTAMP '{RUN_DATE} 23:59:59'
        LIMIT 20
        """,
    ),
    (
        "transactions do not use closed cards",
        f"""
        SELECT t.transaction_id, t.card_id, c.status
        FROM {catalog}.silver.transactions t
        JOIN {catalog}.silver.cards c ON t.card_id = c.card_id
        WHERE LOWER(TRIM(c.status)) = 'closed'
        LIMIT 20
        """,
    ),
    (
        "accounts are not future-opened",
        f"""
        SELECT account_id, open_date
        FROM {catalog}.silver.accounts
        WHERE open_date > DATE '{RUN_DATE}'
        LIMIT 20
        """,
    ),
    (
        "fraud alert scores are in range",
        f"""
        SELECT alert_id, score
        FROM {catalog}.silver.fraud_alerts
        WHERE score < 0 OR score > 1
        LIMIT 20
        """,
    ),
    (
        "auth attempts do not occur after their transaction",
        f"""
        SELECT a.attempt_id, a.auth_ts, t.txn_ts
        FROM {catalog}.silver.auth_attempts a
        JOIN {catalog}.silver.transactions t ON a.transaction_id = t.transaction_id
        WHERE a.auth_ts > t.txn_ts
        LIMIT 20
        """,
    ),
    (
        "disputes are positive and not before their transaction",
        f"""
        SELECT d.dispute_id, d.amount, d.raised_at, t.txn_ts
        FROM {catalog}.silver.disputes d
        JOIN {catalog}.silver.transactions t ON d.transaction_id = t.transaction_id
        WHERE d.amount <= 0 OR d.raised_at < t.txn_ts
        LIMIT 20
        """,
    ),
    (
        "investigation cases use valid status values",
        f"""
        SELECT case_id, status_code
        FROM {catalog}.silver.investigation_cases
        WHERE status_code NOT IN ('open', 'in_progress', 'suspended', 'closed')
        LIMIT 20
        """,
    ),
    (
        "customer contact logs respect do-not-contact",
        f"""
        SELECT contact_id, direction, do_not_contact
        FROM {catalog}.silver.customer_contact_logs
        WHERE direction = 'outbound' AND do_not_contact = true
        LIMIT 20
        """,
    ),
]

for name, query in BUSINESS_RULE_CHECKS:
    fail_if_rows(name, query)

warn_if_rows(
    "legal-hold cases remain in Silver and must be excluded from Gold/AI",
    f"""
    SELECT case_id, legal_hold
    FROM {catalog}.silver.investigation_cases
    WHERE legal_hold = true
    LIMIT 20
    """,
)

PII_CHECKS = [
    (
        "customers first/last names are tokenized",
        f"""
        SELECT customer_id, first_name, last_name
        FROM {catalog}.silver.customers
        WHERE first_name NOT LIKE 'TOK_%'
           OR last_name NOT LIKE 'TOK_%'
        LIMIT 20
        """,
    ),
    (
        "customers email and phone are masked",
        f"""
        SELECT customer_id, email, phone
        FROM {catalog}.silver.customers
        WHERE (email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$')
           OR (phone RLIKE '^\\\\+\\\\d{{6,15}}$')
        LIMIT 20
        """,
    ),
    (
        "customers address and tax_id are hashed when present",
        f"""
        SELECT customer_id, address, tax_id
        FROM {catalog}.silver.customers
        WHERE (address IS NOT NULL AND address NOT RLIKE '^[0-9a-f]{{64}}$')
           OR (tax_id IS NOT NULL AND tax_id NOT RLIKE '^[0-9a-f]{{64}}$')
        LIMIT 20
        """,
    ),
    (
        "cards PAN is masked",
        f"""
        SELECT card_id, pan
        FROM {catalog}.silver.cards
        WHERE pan NOT RLIKE '^XXXX-XXXX-XXXX-[0-9]{{4}}$'
        LIMIT 20
        """,
    ),
    (
        "employees direct identifiers are hashed",
        f"""
        SELECT employee_id, full_name, email
        FROM {catalog}.silver.employees
        WHERE full_name NOT RLIKE '^[0-9a-f]{{64}}$'
           OR email NOT RLIKE '^[0-9a-f]{{64}}$'
        LIMIT 20
        """,
    ),
    (
        "transaction device identifiers are protected",
        f"""
        SELECT device_id, ip
        FROM {catalog}.silver.transaction_devices
        WHERE device_id NOT RLIKE '^DEV_[0-9a-f]{{16}}$'
           OR ip RLIKE '^([0-9]{{1,3}}\\\\.){{3}}[0-9]{{1,3}}$'
        LIMIT 20
        """,
    ),
    (
        "investigation notes contain no raw PII/PAN",
        f"""
        SELECT note_id, note_text
        FROM {catalog}.silver.investigation_notes
        WHERE note_text RLIKE '{PII_PATTERN_SQL}'
        LIMIT 20
        """,
    ),
    (
        "customer contact notes contain no raw PII/PAN",
        f"""
        SELECT contact_id, note
        FROM {catalog}.silver.customer_contact_logs
        WHERE note RLIKE '{PII_PATTERN_SQL}'
        LIMIT 20
        """,
    ),
]

for name, query in PII_CHECKS:
    fail_if_rows(name, query)

fail_if_rows(
    "masking policy registry covers protected Silver fields",
    f"""
    WITH expected(table_name, field_name) AS (VALUES {pair_values(MASKING_POLICY_FIELDS)})
    SELECT e.table_name, e.field_name
    FROM expected e
    LEFT JOIN {catalog}.gov.masking_policies p
      ON e.table_name = p.table_name
     AND e.field_name = p.field_name
    WHERE p.field_name IS NULL
    ORDER BY e.table_name, e.field_name
    """,
)

fail_if_rows(
    "metadata lineage rows point to existing Silver fields",
    f"""
    SELECT l.target_table, l.target_field
    FROM {catalog}.gov.metadata_lineage l
    LEFT JOIN {catalog}.information_schema.columns c
      ON c.table_schema = 'silver'
     AND c.table_name = l.target_table
     AND c.column_name = l.target_field
    WHERE l.target_schema = 'silver'
      AND c.column_name IS NULL
    LIMIT 20
    """,
)

warn_if_rows(
    "clean Silver tables without metadata lineage rows",
    f"""
    WITH expected(table_name) AS (VALUES {values(CLEAN_SILVER_TABLES)})
    SELECT e.table_name
    FROM expected e
    LEFT JOIN (
      SELECT DISTINCT target_table
      FROM {catalog}.gov.metadata_lineage
      WHERE target_schema = 'silver'
    ) l ON e.table_name = l.target_table
    WHERE l.target_table IS NULL
    ORDER BY e.table_name
    """,
)

fail_if_rows(
    "Gold input Silver tables are present in the shared Silver snapshot",
    f"""
    WITH expected(table_name) AS (VALUES {values(GOLD_INPUT_SILVER_TABLES)})
    SELECT e.table_name
    FROM expected e
    LEFT JOIN {catalog}.information_schema.tables t
      ON t.table_schema = 'silver'
     AND t.table_name = e.table_name
    WHERE t.table_name IS NULL
    ORDER BY e.table_name
    """,
)

print("\nSilver table row counts:")
row_count_query = "\nUNION ALL\n".join(
    f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
    f"FROM {catalog}.silver.{table_name}"
    for table_name in CLEAN_SILVER_TABLES
)
sql(row_count_query + "\nORDER BY table_name").show(len(CLEAN_SILVER_TABLES), truncate=False)

print("\nPASS: M2 Silver validation completed with no blocking failures.")

# COMMAND ----------
