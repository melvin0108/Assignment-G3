"""Aggregate Bronze table configs without owning table-specific values."""

from pipeline.bronze import (
    bronze_accounts,
    bronze_auth_attempts,
    bronze_branches,
    bronze_cards,
    bronze_case_parties,
    bronze_case_status_types,
    bronze_case_transactions,
    bronze_channels,
    bronze_chargebacks,
    bronze_countries,
    bronze_currencies,
    bronze_customer_contact_logs,
    bronze_customers,
    bronze_date_dim,
    bronze_defects_manifest,
    bronze_dispute_reason_codes,
    bronze_disputes,
    bronze_employees,
    bronze_fraud_alerts,
    bronze_fraud_types,
    bronze_investigation_cases,
    bronze_investigation_notes,
    bronze_merchant_categories,
    bronze_merchants,
    bronze_transaction_devices,
    bronze_transactions,
)


_TABLE_MODULES = (
    bronze_accounts,
    bronze_auth_attempts,
    bronze_branches,
    bronze_cards,
    bronze_case_parties,
    bronze_case_status_types,
    bronze_case_transactions,
    bronze_channels,
    bronze_chargebacks,
    bronze_countries,
    bronze_currencies,
    bronze_customer_contact_logs,
    bronze_customers,
    bronze_date_dim,
    bronze_defects_manifest,
    bronze_dispute_reason_codes,
    bronze_disputes,
    bronze_employees,
    bronze_fraud_alerts,
    bronze_fraud_types,
    bronze_investigation_cases,
    bronze_investigation_notes,
    bronze_merchant_categories,
    bronze_merchants,
    bronze_transaction_devices,
    bronze_transactions,
)

ALL_TABLE_CONFIGS = {
    module.TABLE_NAME: (module.SOURCE_COLUMNS, module.RECORD_ID_COLUMNS)
    for module in _TABLE_MODULES
}

if len(ALL_TABLE_CONFIGS) != len(_TABLE_MODULES):
    raise ValueError("Bronze table names must be unique")
