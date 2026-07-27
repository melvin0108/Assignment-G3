# Deliverable 5 — Data Quality Evidence

## Evidence status

**Historical Batch 1 evidence — validation passed with non-blocking warnings.**

This document records Deliverable 5 Data Quality evidence produced by a Batch 1 Databricks validation run. The executable validation gate passed, while the manifest reconciliation reported differences that are disclosed below and must not be interpreted as perfect recall or precision. For inspectable generated inputs and outputs from the later observed Batch 2 snapshot, see [Generated output samples](generated-output-samples.md); that package explicitly records its own validation limitations.

## Evidence scope

| Item | Evidence value |
|---|---|
| Catalog | `g3_dev` |
| Bronze snapshot batch ID | `1` |
| DQ run ID | `RUN-01-DQ` |
| Silver run ID | `RUN-01` |
| Evidence generated at (UTC) | `2026-07-23T02:51:04.202816+00:00` |
| Executable validation status | **PASSED** |

The evidence collector automatically selects the latest complete `bronze.defects_manifest` snapshot in the chosen catalog. Its Bronze `_run_id` identifies the Silver run, and the corresponding authoritative Bronze DQ run is `<silver_run_id>-DQ`. This prevents older accumulated quarantine history from inflating the results.

## Validation summary

Run `pipeline/validation/validate_m2_dq.py` after the Bronze, DQ, and Silver stages complete successfully. A successful notebook run proves that:

- enabled rules are present in `gov.dq_rules`;
- manifest rule IDs and quarantine rule IDs resolve to the registry;
- quarantine entries contain the required audit fields;
- every Silver table contains only the current Bronze snapshot;
- quarantined physical source rows are excluded from clean Silver outputs; and
- Silver type-cast failures are excluded from the affected clean tables.

| Validation measure | Result |
|---|---:|
| Blocking checks passed | 61 |
| Warning checks clear | 0 |
| Warnings raised | 1 |
| Manifest keys missed by DQ | 209 |
| Extra DQ keys not in manifest | 21,579 |

The single warning check reported enabled registry rules without seeded examples in `bronze.defects_manifest`. This is non-blocking because the registry also contains Silver and type-contract rules that are not necessarily represented by injected Bronze defects.

Manifest reconciliation status is **WARNING**. The DQ output missed 209 expected `(rule_id, record_key)` keys and detected 21,579 keys not recorded in the injected-defect manifest. Extra keys can represent defects found by executable rules beyond the injected oracle, but this evidence alone does not classify each difference as a valid additional detection or a false positive. Consequently, no perfect recall or precision claim is made; the differences remain a documented limitation for rule-level investigation.

## Rule and quarantine results

An enabled rule is counted as **failed** when it produces at least one quarantine entry for the current DQ or Silver run. It is counted as **passed** when it produces no quarantine entry. The quarantine table stores one entry per failed physical source record and rule. A separate distinct-source-record count is included because one source record can violate multiple rules.

| Required measure | Result |
|---|---:|
| Enabled rule count | 71 |
| Passed rule count | 28 |
| Failed rule count | 43 |
| Quarantined record count (record × rule entries) | 427,617 |
| Distinct quarantined source record count | 338,296 |

### Rules with failures

| Rule ID | Rule name | Layer | Source table | Quarantined records |
|---|---|---|---|---:|
| `DQ-ACC-CUST-FK` | customer_id must exist in customers | Bronze | accounts | 655 |
| `DQ-ACC-OPENDATE-FUTURE` | open_date must not be in the future | Bronze | accounts | 224 |
| `DQ-ALT-SCORE-RANGE` | score must be within [0,1] | Bronze | fraud_alerts | 50 |
| `DQ-ALT-TXN-FK` | transaction_id must exist in Silver transactions | Silver | fraud_alerts | 558 |
| `DQ-AUTH-TS-ORDER` | auth_ts must not be later than txn_ts | Bronze | auth_attempts | 5,218 |
| `DQ-AUTH-TXN-FK` | transaction_id must exist in transactions | Bronze | auth_attempts | 135,104 |
| `DQ-CARD-ACCT-FK` | account_id must exist in Silver accounts | Silver | cards | 469 |
| `DQ-CARD-DUP` | card_id must be unique | Bronze | cards | 266 |
| `DQ-CARD-EXPIRED-ACTIVE` | active card must not have a past expiry | Bronze | cards | 5,518 |
| `DQ-CASE-LEGALHOLD` | legal_hold cases excluded from AI output | Bronze | investigation_cases | 4 |
| `DQ-CASE-STALE` | open cases older than 180 days are stale | Bronze | investigation_cases | 54 |
| `DQ-CASE-STATUS-ENUM` | status_code must be in case_status enum | Bronze | investigation_cases | 6 |
| `DQ-CASEPARTY-CASE-FK` | case_id must exist in Silver investigation cases | Silver | case_parties | 64 |
| `DQ-CASEPARTY-RESOLVE` | party_id must resolve per party_type | Bronze | case_parties | 27 |
| `DQ-CASEPARTY-TYPE-ENUM` | party_type must be in {customer,merchant,third_party} | Bronze | case_parties | 8 |
| `DQ-CASETXN-CASE-FK` | case_id must exist in Silver investigation cases | Silver | case_transactions | 50 |
| `DQ-CASETXN-TXN-FK` | transaction_id must exist in transactions | Bronze | case_transactions | 348 |
| `DQ-CBK-DISP-FK` | dispute_id must exist in disputes | Bronze | chargebacks | 507 |
| `DQ-CTL-CUST-FK` | customer_id must exist in Silver customers | Silver | customer_contact_logs | 12 |
| `DQ-CTL-DNC-VIOLATION` | no outbound contact when do_not_contact=true | Bronze | customer_contact_logs | 8 |
| `DQ-CTL-NOTE-PII` | note must not contain raw PII/PAN | Bronze | customer_contact_logs | 8 |
| `DQ-CUST-EMAIL-FMT` | email must match pattern if present | Bronze | customers | 125 |
| `DQ-CUST-ID-DUP` | customer_id must be unique | Bronze | customers | 146 |
| `DQ-CUST-NEAR-DUP` | no two customers share name+dob+address+tax_id | Bronze | customers | 223 |
| `DQ-DEV-TXN-FK` | transaction_id must exist in transactions | Bronze | transaction_devices | 90,254 |
| `DQ-DEV-TYPE-REQ` | device_type is required | Bronze | transaction_devices | 3,200 |
| `DQ-DISP-REASON-REQ` | reason_code is required | Bronze | disputes | 160 |
| `DQ-DISP-STATUS-ENUM` | status must be a lowercase dispute enum | Bronze | disputes | 160 |
| `DQ-DISP-TXN-FK` | transaction_id must exist in transactions | Bronze | disputes | 2,352 |
| `DQ-EMP-EMAIL-UNIQ` | email must be unique | Bronze | employees | 2 |
| `DQ-EMP-NAME-NEAR-DUP` | flag near-duplicate employee names | Bronze | employees | 1 |
| `DQ-MERCH-RISK-CASING` | risk_rating must be in {low,medium,high} | Bronze | merchants | 109 |
| `DQ-NOTE-CASE-FK` | case_id must exist in Silver investigation cases | Silver | investigation_notes | 177 |
| `DQ-NOTE-LEGALHOLD` | notes on legal_hold cases must not reach AI | Bronze | investigation_notes | 26 |
| `DQ-NOTE-PII-LEAK` | note_text must not contain raw PII/PAN | Bronze | investigation_notes | 50 |
| `DQ-TXN-ACCT-FK` | account_id must exist in accounts | Bronze | transactions | 22,576 |
| `DQ-TXN-AMT-POS` | amount must be > 0 | Bronze | transactions | 10,000 |
| `DQ-TXN-CARD-ACTIVE` | transaction must use an active card | Bronze | transactions | 41,536 |
| `DQ-TXN-CARD-FK` | card_id must exist in Silver cards | Silver | transactions | 75,692 |
| `DQ-TXN-ID-DUP` | transaction_id must be unique | Bronze | transactions | 8,000 |
| `DQ-TXN-MERCH-FK` | merchant_id must exist in Silver merchants | Silver | transactions | 7,818 |
| `DQ-TXN-MERCH-REQ` | merchant_id is required | Bronze | transactions | 7,852 |
| `DQ-TXN-TS-FUTURE` | txn_ts must not be in the future | Bronze | transactions | 8,000 |

## Sample failed records

The evidence sample intentionally omits `raw_record`, which can contain sensitive source values. Full forensic snapshots remain access-controlled in `silver.quarantine_records`.

| Run ID | Source table | Record key | Rule ID | Failure reason | Severity | Disposition | Detected at |
|---|---|---|---|---|---|---|---|
| `RUN-01` | accounts | `ACC-0043` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-1856 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |
| `RUN-01-DQ` | accounts | `ACC-0114` | `DQ-ACC-CUST-FK` | orphan customer_id | quarantine | quarantined | `2026-07-22T09:16:01.039659` |
| `RUN-01` | accounts | `ACC-0114` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-9999 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |
| `RUN-01-DQ` | accounts | `ACC-0115` | `DQ-ACC-CUST-FK` | orphan customer_id | quarantine | quarantined | `2026-07-22T09:16:01.039659` |
| `RUN-01` | accounts | `ACC-0115` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-9999 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |
| `RUN-01-DQ` | accounts | `ACC-0123` | `DQ-ACC-CUST-FK` | orphan customer_id | quarantine | quarantined | `2026-07-22T09:16:01.039659` |
| `RUN-01` | accounts | `ACC-0123` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-9999 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |
| `RUN-01-DQ` | accounts | `ACC-0145` | `DQ-ACC-CUST-FK` | orphan customer_id | quarantine | quarantined | `2026-07-22T09:16:01.039659` |
| `RUN-01` | accounts | `ACC-0145` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-9999 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |
| `RUN-01` | accounts | `ACC-0149` | `DQ-ACC-CUST-FK` | Referential integrity break: customer_id CUST-1051 not found in silver.customers | quarantine | quarantined | `2026-07-22T09:28:04.289436` |

## How failure handling works

1. `pipeline/dq/dq_02_load_dq_rules.py` publishes the executable rule inventory to `gov.dq_rules`, including each rule's target, pattern, severity, and human-readable expression.
2. `pipeline/dq/dq_03_failures_all_rules.py` evaluates the Bronze rules against the current snapshot and writes violations to `silver.quarantine_records` under the `<silver_run_id>-DQ` run ID.
3. Each quarantine entry records the source table and physical source ID, natural record key, rule and reason, severity, disposition, detection time, and a raw JSON snapshot for controlled forensic review.
4. The DQ stage deletes only the current DQ run's prior entries before inserting replacements. Rerunning the same snapshot is therefore idempotent while older run history remains available.
5. Silver transformations treat the DQ quarantine as an authoritative exclusion list. They also quarantine Silver relationship and type-conversion failures under the Silver run ID before publishing clean records.
6. `pipeline/validation/validate_m2_dq.py` fails the Databricks task if a blocking contract is broken. Non-blocking manifest reconciliation differences are reported as warnings with missed and extra key totals.

## Reproduce and capture the evidence

1. In Databricks, run the complete pipeline through the Silver stage for the catalog being submitted.
2. Open and run `pipeline/validation/validate_m2_dq.py` on serverless compute.
3. Set the `catalog` widget to the same catalog used by the pipeline.
4. Confirm the notebook ends with `PASS: M2 DQ/quarantine validation completed with no blocking failures.`
5. Copy everything between `D5_EVIDENCE_JSON_START` and `D5_EVIDENCE_JSON_END` and compare it with the values recorded in this document.
6. Keep the successful Databricks run URL or an exported notebook output with the submitted evidence so a reviewer can independently verify the reported values. The URL was not included in the supplied evidence and should be attached separately if required.

## Evidence sources

- Rule registry: `g3_dev.gov.dq_rules`
- Detailed failure evidence: `g3_dev.silver.quarantine_records`
- Expected injected defects: `g3_dev.bronze.defects_manifest`
- Executable evidence collector and validation gate: `pipeline/validation/validate_m2_dq.py`
