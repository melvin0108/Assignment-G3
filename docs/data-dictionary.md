# Raw Source Data Dictionary

This dictionary defines the intended row counts and columns for the raw source
tables used by the Transaction Investigation Context pipeline.

Scope: the 25 source CSV tables generated into `data/raw/`. The checked-in
`data/raw` files are small script-test samples and are not the target volume.

## Row Count Baseline

`Requirements.md` requires realistic mock source datasets, data-quality
defects, and enough volume to demonstrate batch processing and stress testing.
For the assignment deliverable, use the **assignment target baseline** below:

- `transactions`: 2,000,000 rows.
- `customers`: 5,000 rows.
- This maps to the current generator default: `python -m mock.generate`.
- Smaller development samples should override `--transactions` and `--customers`
  explicitly.

Derived fact and child-table counts follow the generator ratios in
`mock/generate.py`; fixed reference tables keep their natural lookup sizes.

## Source Tables

| Table | Target rows | Grain | Columns |
|---|---:|---|---|
| `merchant_categories` | 6 | one row per merchant category code | `mcc`, `category_name`, `category_group` |
| `channels` | 4 | one row per transaction channel | `channel_code`, `channel_name` |
| `case_status_types` | 4 | one row per investigation case status | `status_code`, `description` |
| `dispute_reason_codes` | 3 | one row per dispute reason code | `reason_code`, `description` |
| `fraud_types` | 4 | one row per fraud type | `fraud_type_code`, `description`, `severity` |
| `countries` | 5 | one row per ISO country | `iso_code`, `name`, `region` |
| `currencies` | 5 | one row per ISO currency | `currency_code`, `name`, `decimals` |
| `branches` | 4 | one row per branch | `branch_code`, `name`, `country`, `region`, `status` |
| `date_dim` | 1,283 | one row per calendar day from 2023-01-01 to 2026-07-06 | `date_id`, `year`, `month`, `quarter`, `is_weekend` |
| `customers` | 5,000 | one row per customer | `customer_id`, `first_name`, `last_name`, `dob`, `email`, `phone`, `address`, `tax_id`, `created_at` |
| `employees` | 200 | one row per employee or investigator | `employee_id`, `full_name`, `email`, `team`, `role` |
| `accounts` | 7,500 | one row per account | `account_id`, `customer_id`, `product_type`, `open_date`, `status`, `currency` |
| `cards` | 9,000 | one row per payment card | `card_id`, `account_id`, `card_type`, `pan`, `expiry`, `status` |
| `merchants` | 2,000 | one row per merchant | `merchant_id`, `name`, `mcc`, `country`, `risk_rating`, `status` |
| `transactions` | 2,000,000 | one row per transaction event | `transaction_id`, `account_id`, `card_id`, `merchant_id`, `channel`, `amount`, `currency`, `txn_ts`, `status` |
| `auth_attempts` | 2,400,000 | one row per authorization attempt | `attempt_id`, `transaction_id`, `decision`, `decline_reason`, `auth_ts` |
| `transaction_devices` | 1,600,000 | one row per transaction device or session fingerprint | `device_id`, `transaction_id`, `device_type`, `ip`, `geo_country` |
| `disputes` | 40,000 | one row per customer dispute | `dispute_id`, `transaction_id`, `reason_code`, `amount`, `status`, `raised_at` |
| `chargebacks` | 8,000 | one row per scheme chargeback | `chargeback_id`, `dispute_id`, `scheme`, `amount`, `stage`, `processed_at` |
| `fraud_alerts` | 10,000 | one row per fraud rule alert | `alert_id`, `transaction_id`, `rule_name`, `score`, `triggered_at`, `disposition` |
| `investigation_cases` | 2,000 | one row per investigation case | `case_id`, `priority`, `status_code`, `fraud_type_code`, `owner_employee_id`, `opened_at`, `closed_at`, `legal_hold` |
| `investigation_notes` | 10,000 | one row per investigation note | `note_id`, `case_id`, `author_employee_id`, `note_text`, `created_at` |
| `case_transactions` | 6,000 | one row per case-to-transaction link | `case_id`, `transaction_id`, `linked_at` |
| `case_parties` | 4,000 | one row per case-to-party link | `case_id`, `party_type`, `party_id`, `role` |
| `customer_contact_logs` | 2,000 | one row per customer contact attempt | `contact_id`, `customer_id`, `direction`, `contact_method`, `do_not_contact`, `contacted_at`, `employee_id`, `note` |

## Count Derivation

| Table group | Rule |
|---|---|
| Core anchors | `customers = 5,000`; `transactions = 2,000,000` |
| Accounts | `accounts = customers * 1.5` |
| Cards | `cards = accounts * 1.2` |
| Authorization attempts | `auth_attempts = transactions * 1.2` |
| Transaction devices | `transaction_devices = transactions * 0.8` |
| Disputes | `disputes = transactions * 0.02` |
| Chargebacks | `chargebacks = disputes * 0.2` |
| Fraud alerts | `fraud_alerts = transactions * 0.005` |
| Investigation cases | `investigation_cases = transactions * 0.001` |
| Investigation notes | `investigation_notes = investigation_cases * 5` |
| Case transactions | `case_transactions = investigation_cases * 3` |
| Case parties | `case_parties = investigation_cases * 2` |
| Customer contact logs | `customer_contact_logs = investigation_cases` |
| Reference tables | Fixed lookup sizes from `mock/config.py` |

## Notes

- All raw fields are landed as strings in Bronze; Silver performs type coercion,
  validation, masking, and quarantine routing.
- `_defects_manifest.csv` is generated beside the source CSVs for validation
  evidence, but it is not a source domain table.
- Pipeline-emitted tables such as `dq_rules`, `dq_results`,
  `quarantine_records`, `pipeline_runs`, `metadata_lineage`,
  `masking_policies`, `access_policies`, and `investigation_context` do not
  have fixed source row targets. Their row counts are produced by the pipeline
  per run and are documented in `docs/data-model.md`.
