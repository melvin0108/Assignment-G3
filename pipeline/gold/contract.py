"""Small, runtime-independent contract helpers for Gold context documents."""

FORBIDDEN_FIELD_TOKENS = (
    "customer",
    "employee",
    "staff",
    "account_id",
    "card_id",
    "party_id",
    "device",
    "ip",
    "pan",
)
SUMMARY_TEMPLATE = "%s priority %s investigation opened %s; current status %s."
CONTEXT_VERSION = "1.0.0"
REQUIRED_INPUT_TABLES = (
    "investigation_cases",
    "case_transactions",
    "case_parties",
    "transactions",
    "auth_attempts",
    "accounts",
    "cards",
    "merchants",
    "merchant_categories",
    "fraud_alerts",
    "disputes",
    "chargebacks",
    "investigation_notes",
    "channels",
    "fraud_types",
    "dispute_reason_codes",
)


def build_case_summary(priority, fraud_type_description, status_code, opened_at):
    """Return the deterministic, non-inferential Gold case summary."""
    priority = priority or "unknown priority"
    fraud_type_description = fraud_type_description or "unknown fraud type"
    status_code = status_code or "unknown"
    opened_at = opened_at or "unknown date"
    return SUMMARY_TEMPLATE % (priority, fraud_type_description, opened_at, status_code)


def forbidden_field_names(field_names):
    """Return sorted public fields that would violate the Gold allow-list."""
    def is_forbidden(field_name):
        name = field_name.lower()
        segments = name.replace(".", "_").split("_")
        return any(
            token in name
            for token in FORBIDDEN_FIELD_TOKENS
            if token not in {"ip", "pan"}
        ) or "ip" in segments or "pan" in segments

    return sorted(
        field_name
        for field_name in field_names
        if is_forbidden(field_name)
    )
