"""Static configuration for the mock generator.

Single source of truth for: the pinned run date, ID prefixes, enums
(data-model.md §5), reference/lookup data, table CSV column order, generation
order, and base volumes. Keep this in sync with docs/data-dictionary.md and
docs/data-model.md.
"""
from datetime import date

# --------------------------------------------------------------------------
# Pinned "current" date.
# Time-based defects (future timestamps, stale cases) are measured against this
# so generation is fully deterministic regardless of when it runs.
# (Matches the project date; update when re-baselining tests.)
# --------------------------------------------------------------------------
RUN_DATE = date(2026, 7, 6)

# --------------------------------------------------------------------------
# ID prefixes
# --------------------------------------------------------------------------
PFX = {
    "customer": "CUST-",
    "account": "ACC-",
    "card": "CARD-",
    "merchant": "MCH-",
    "employee": "EMP-",
    "transaction": "TXN-",
    "dispute": "DSP-",
    "chargeback": "CBK-",
    "case": "CASE-",
    "note": "NOTE-",
    "alert": "ALT-",
    "attempt": "AUTH-",
    "device": "DEV-",
    "contact": "CTL-",
    "third_party": "TP-",
    "branch": "BR-",
}

# --------------------------------------------------------------------------
# Enums (data-model.md §5) — valid values
# --------------------------------------------------------------------------
TRANSACTION_STATUS = ["authorized", "settled", "declined", "reversed", "refunded"]
ACCOUNT_STATUS = ["active", "dormant", "closed", "frozen"]
CARD_STATUS = ["active", "blocked", "expired", "closed"]
MERCHANT_STATUS = ["active", "suspended", "closed"]
DISPUTE_STATUS = ["open", "in_review", "resolved", "rejected", "withdrawn"]
CASE_STATUS = ["open", "in_progress", "suspended", "closed"]
CHARGEBACK_STAGE = ["representment", "pre_arbitration", "won", "lost", "reversed"]
FRAUD_ALERT_DISPOSITION = ["open", "escalated_to_case", "dismissed", "true_positive", "false_positive"]

CARD_TYPE = ["debit", "credit"]
ACCOUNT_PRODUCT = ["Everyday", "Savings", "Credit", "Debit"]
RISK_RATING = ["low", "medium", "high"]
CASE_PRIORITY = ["low", "medium", "high", "critical"]
FRAUD_TYPE = ["card_fraud", "account_takeover", "sar", "none"]
DISPUTE_REASON = ["10.4", "13.1", "13.7"]
CHANNEL = ["pos", "online", "mobile", "atm"]
CURRENCY = ["AUD", "USD", "NZD", "GBP", "SGD"]
CONTACT_METHOD = ["phone", "email", "sms", "post"]
CONTACT_DIRECTION = ["inbound", "outbound"]
PARTY_TYPE = ["customer", "merchant", "third_party"]
PARTY_ROLE = ["subject", "reporter", "witness", "merchant"]
EMP_TEAM = ["Fraud Ops", "QA", "Compliance"]
EMP_ROLE = ["investigator", "supervisor", "analyst"]
SCHEME = ["visa", "mastercard", "amex"]
DEVICE_TYPE = ["mobile_ios", "mobile_android", "web", "pos_terminal", "atm"]
AUTH_DECISION = ["approved", "declined"]
DECLINE_REASON = ["insufficient_funds", "incorrect_pin", "suspected_fraud", "expired_card", "do_not_honour"]

# Intentionally-invalid values used to inject DQ defects (data-model.md §5)
INVALID = {
    "dispute_status_casing": "Open",
    "dispute_status_unknown": "pending",
    "case_status_unknown": "on_hold",
    "risk_casing_variants": ["HIGH", "High", "H", "Low", "M"],
}
ORPHAN_CUSTOMER_ID = "CUST-9999"  # guaranteed not to exist
ORPHAN_DISPUTE_ID = "DSP-999999"  # guaranteed not to exist (dispute ids are seq_id DSP-<i>; never reach 999999)

# --------------------------------------------------------------------------
# Reference / lookup data (small, mostly clean)
# --------------------------------------------------------------------------
MERCHANT_CATEGORIES = [
    ("5499", "Cafes & Restaurants", "Food"),
    ("5732", "Electronics", "Retail"),
    ("7011", "Hotels & Lodging", "Travel"),
    ("5812", "Eating Places", "Food"),
    ("5411", "Grocery Stores", "Food"),
    ("5912", "Drug Stores", "Services"),
]
CHANNELS = [("pos", "Point of Sale"), ("online", "E-Commerce"), ("mobile", "Mobile App"), ("atm", "ATM")]
CASE_STATUS_TYPES = [("open", "Case open"), ("in_progress", "Under investigation"), ("suspended", "Temporarily halted"), ("closed", "Case closed")]
DISPUTE_REASON_CODES = [("10.4", "Fraud - Card Absent"), ("13.1", "Merchandise Not Received"), ("13.7", "Cancelled Merchandise")]
FRAUD_TYPES = [("card_fraud", "Card compromise", "high"), ("account_takeover", "Account takeover", "high"), ("sar", "Suspicious Activity Report", "critical"), ("none", "Not fraud", "low")]
COUNTRIES = [("AU", "Australia", "APAC"), ("NZ", "New Zealand", "APAC"), ("US", "United States", "AMER"), ("GB", "United Kingdom", "EMEA"), ("SG", "Singapore", "APAC")]
CURRENCIES = [("AUD", "Australian Dollar", 2), ("USD", "US Dollar", 2), ("NZD", "New Zealand Dollar", 2), ("GBP", "Pound Sterling", 2), ("SGD", "Singapore Dollar", 2)]
BRANCHES = [("BR-01", "Melbourne Flagship", "AU", "VIC", "active"), ("BR-02", "Sydney CBD", "AU", "NSW", "active"), ("BR-03", "Brisbane", "AU", "QLD", "active"), ("BR-04", "Perth", "AU", "WA", "closed")]
DATE_DIM_RANGE = (date(2023, 1, 1), RUN_DATE)  # inclusive daily calendar

# --------------------------------------------------------------------------
# Table CSV schemas — column order is the on-disk order (bronze-layer.md)
# --------------------------------------------------------------------------
TABLE_SCHEMAS = {
    "customers": ["customer_id", "first_name", "last_name", "dob", "email", "phone", "address", "tax_id", "created_at", "effective_at"],
    "accounts": ["account_id", "customer_id", "product_type", "open_date", "status", "currency"],
    "cards": ["card_id", "account_id", "card_type", "pan", "expiry", "status", "effective_at"],
    "merchants": ["merchant_id", "name", "mcc", "country", "risk_rating", "status", "effective_at"],
    "merchant_categories": ["mcc", "category_name", "category_group"],
    "channels": ["channel_code", "channel_name"],
    "case_status_types": ["status_code", "description"],
    "dispute_reason_codes": ["reason_code", "description"],
    "fraud_types": ["fraud_type_code", "description", "severity"],
    "countries": ["iso_code", "name", "region"],
    "currencies": ["currency_code", "name", "decimals"],
    "branches": ["branch_code", "name", "country", "region", "status"],
    "date_dim": ["date_id", "year", "month", "quarter", "is_weekend"],
    "employees": ["employee_id", "full_name", "email", "team", "role"],
    "transactions": ["transaction_id", "account_id", "card_id", "merchant_id", "channel", "amount", "currency", "txn_ts", "status"],
    "auth_attempts": ["attempt_id", "transaction_id", "decision", "decline_reason", "auth_ts"],
    "transaction_devices": ["device_id", "transaction_id", "device_type", "ip", "geo_country"],
    "disputes": ["dispute_id", "transaction_id", "reason_code", "amount", "status", "raised_at"],
    "chargebacks": ["chargeback_id", "dispute_id", "scheme", "amount", "stage", "processed_at"],
    "investigation_cases": ["case_id", "priority", "status_code", "fraud_type_code", "owner_employee_id", "opened_at", "closed_at", "legal_hold"],
    "investigation_notes": ["note_id", "case_id", "author_employee_id", "note_text", "created_at"],
    "fraud_alerts": ["alert_id", "transaction_id", "rule_name", "score", "triggered_at", "disposition"],
    "case_transactions": ["case_id", "transaction_id", "linked_at"],
    "case_parties": ["case_id", "party_type", "party_id", "role"],
    "customer_contact_logs": ["contact_id", "customer_id", "direction", "contact_method", "do_not_contact", "contacted_at", "employee_id", "note"],
}

# Generation order — parents before children so foreign keys resolve.
GENERATION_ORDER = [
    "merchant_categories", "channels", "case_status_types", "dispute_reason_codes",
    "fraud_types", "countries", "currencies", "branches", "date_dim",
    "customers", "employees", "accounts", "cards", "merchants",
    "transactions", "auth_attempts", "transaction_devices",
    "disputes", "chargebacks", "fraud_alerts",
    "investigation_cases", "investigation_notes", "case_transactions",
    "case_parties", "customer_contact_logs",
]

# Base volumes. 0 / missing means "derived in generate.build_counts()".
BASE_VOLUMES = {
    "customers": 5000,
    "employees": 200,
    "merchants": 2000,
    "transactions": 2000000,
}

# --------------------------------------------------------------------------
# SCD Type 2 — multi-snapshot dimension history (Approach B, mock-only)
# --------------------------------------------------------------------------
# Dimensions whose attribute changes between snapshots drive SCD2. Each entry
# names the single attribute that mutates when a key is "re-extracted" in a
# later snapshot (the rest of the row stays identical). These version changes
# are legitimate history, NOT DQ defects — they are logged to a separate
# scd_changes_manifest.csv (see mock/scd.py), never to DefectManifest.
SCD2_DIMENSIONS = ["customers", "cards", "merchants"]
SCD2_MUTATIONS = {
    "customers": "address",     # customer moves -> address re-extracted
    "cards": "status",          # card lifecycle (active -> blocked/closed)
    "merchants": "risk_rating", # merchant risk re-assessment (low -> high)
}
SCD2_REQUIRED_KEYS = {
    "customers": {"CUST-0001"},  # deterministic end-to-end SCD2 demonstration
}
SCD2_RATE_DEFAULT = 0.02        # fraction of dim keys that change per snapshot

# Snapshot as-of dates. T0 = RUN_DATE (unchanged from the single-run baseline);
# each later snapshot steps forward by SNAPSHOT_INTERVAL_DAYS, so a mutated row's
# new effective_at is strictly later than every T0 value (clean SCD2 ordering).
SNAPSHOT_BASE_DATE = RUN_DATE       # T0 as-of
SNAPSHOT_INTERVAL_DAYS = 30         # T1 as-of = RUN_DATE + 30d, T2 + 60d, ...
