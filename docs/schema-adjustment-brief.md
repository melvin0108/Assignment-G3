# Schema Adjustment Brief Before Bronze Data Generation

## Evaluation Summary

The proposed schema is suitable for the assignment and supports the Transaction Investigation scenario well. It exceeds the minimum source dataset requirement and has enough domain coverage for ingestion, data quality checks, quarantine handling, PII masking, metadata, lineage, and AI-ready context generation.

The core Bronze tables are strong enough to start with:

- `customers`
- `accounts`
- `cards`
- `merchants`
- `merchant_categories`
- `transactions`
- `disputes`
- `investigation_cases`
- `investigation_notes`
- `employees`

However, the schema should be tightened before Bronze source data is generated. Several details are still underspecified, especially table naming, data contracts, field-level classifications, status enums, quarantine structure, and the final AI-ready context grain.

## Required CTAs

### 1. Standardize Table Naming

Use one table naming convention consistently across source files, Bronze tables, Silver tables, Gold outputs, DQ rules, and lineage.

Recommended convention:

```text
customers
accounts
cards
merchants
merchant_categories
transactions
disputes
investigation_cases
investigation_notes
employees
```

Do not mix singular names from `data-model.md` with plural names from `bronze-layer.md`.

### 2. Create a Data Contract for Every Source Table

For each source table, define:

```text
field_name
data_type
required_or_optional
accepted_values_or_pattern
example_value
primary_key_or_foreign_key
sensitive_data_classification
quality_rules
```

This is required by the assignment and should exist before mock data is generated.

### 3. Align Sample Columns With the Data Model

Resolve current mismatches before creating Bronze files:

- `customer.tax_id` appears in the model but not the Bronze sample.
- `card.masked_pan` in the model conflicts with `pan` in the Bronze sample.
- `customer_contact_log` mentions Do-Not-Contact logic but no field currently supports it.
- `case_party.party_id` needs explicit resolution rules based on `party_type`.

### 4. Define Mandatory Bronze Metadata Columns

Every Bronze table should include:

```text
_source_file
_source_file_mod_time
_ingest_ts
_run_id
_batch_id
_rescued_data
```

Recommended additions:

```text
_source_record_id
_record_hash
```

These fields support lineage, deduplication, replay, and quarantine traceability.

### 5. Define Table Grain Clearly

Document the grain of each source table before data generation.

Examples:

```text
transactions: one row per transaction event
auth_attempts: one row per authorization attempt
investigation_notes: one row per note per case
case_transactions: one row per case-transaction relationship
```

### 6. Define Status Enums and Transition Rules

Explicitly list valid values for:

```text
transaction.status
account.status
card.status
merchant.status
dispute.status
investigation_case.status_code
chargeback.stage
fraud_alert.disposition
```

Also define the intentionally invalid examples used for DQ testing.

### 7. Define PII and Masking Rules

At minimum, classify and define handling for:

```text
customer name
email
phone
address
dob
tax_id
card PAN
employee name
employee email
free-text notes
IP address
device identifiers
```

Clarify whether each field is:

```text
allowed in Bronze only
masked in Silver
redacted from Gold
tokenized
hashed
excluded from AI output
```

### 8. Define Quarantine Output Schema

Before creating invalid records, define how failed records will be stored.

Recommended fields:

```text
run_id
source_table
source_record_id
record_key
rule_id
rule_name
failure_reason
severity
disposition
raw_record
detected_at
```

### 9. Define Gold AI-Ready Context Grain

Recommended Gold output:

```text
one investigation_context record per case_id
```

Each record should include:

```text
case summary
linked transactions
merchant context
dispute context
fraud alerts
redacted notes
quality_status
masking_status
source_references
usage_restrictions
context_version
last_refreshed_at
warning_flags
```

### 10. Generate Bronze Data Only After Contracts Are Finalized

Bronze data should be raw and dirty, but not ambiguous. The defects should be intentional, traceable, and mapped to DQ rules.

## Instruction for the Bronze Data Agent

Before creating Bronze mock data, update the schema documentation so every source table has a complete data contract, consistent naming, explicit keys, clear grain, PII classification, accepted values, and planned DQ defects.

After those contracts are finalized, generate Bronze source files exactly according to the contracts. Include both valid and invalid records so the assignment can demonstrate data quality evidence, quarantine behavior, PII protection, metadata capture, and AI-safe context generation.
