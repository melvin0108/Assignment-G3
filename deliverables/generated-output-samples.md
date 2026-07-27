# Generated Output Samples — Batch 2

## Purpose and scope

This compact, Markdown-native evidence package provides inspectable generated
input and output rows without committing the full synthetic dataset. Every row
below was read from the Databricks workspace on 2026-07-27. The samples are
deterministically selected from Batch 2 by ascending `transaction_id` (clean
source and Silver) or by the first matching `transaction_id` for each DQ rule
(manifest and quarantine).

The data is synthetic. The quarantine sample deliberately excludes
`raw_record`, even though it is available only for controlled forensic review
in the workspace.

## Provenance

| Item | Recorded value |
|---|---|
| Catalog | `g3_dev` |
| Bronze batch ID | `2` |
| Bronze and Silver pipeline run ID | `RUN-02` |
| Source file represented | `transaction02.csv` |
| Batch-2 Bronze transaction rows | 204,000 |
| Batch-2 defects-manifest rows | 36,551 |
| Batch-2 clean Silver transaction rows | 98,249 |
| Current `RUN-02` quarantine rows | 342,811 |
| Generator implementation | `generate_mock_databricks.py`, which calls `mock.generate` |
| Generator seed, command, and job-run ID | Not durably recorded with Batch 2. The repository code defaults to seed `42`, 200,000 transactions, defect rate `0.05`, and SCD rate `0.02`, but this evidence does **not** assert that those defaults were the values used for this historical batch. |
| Expected DQ run convention | The code expects `RUN-02-DQ`; no rows for that run ID were present in the inspected quarantine table. The observed quarantine rows use `RUN-02`. |

The missing runtime generator parameters are a provenance limitation of the
historical batch. Future generator runs should persist the widget values and
job-run ID alongside the generated batch before they are submitted as evidence.

## Generated Bronze source sample

These five clean source rows are from `g3_dev.bronze.transactions`, Batch 2,
after excluding transaction source records that occur in `RUN-02` quarantine.
The identifiers are synthetic internal test identifiers.

| Transaction | Account | Card | Merchant | Channel | Amount | Currency | Transaction timestamp | Status |
|---|---|---|---|---|---:|---|---|---|
| `TXN-000002` | `ACC-1478` | `CARD-0421` | `MCH-0501` | online | 15.09 | SGD | 2026-06-12T05:13:03Z | settled |
| `TXN-000004` | `ACC-5570` | `CARD-6619` | `MCH-0978` | mobile | 339.98 | USD | 2026-06-11T06:19:37Z | settled |
| `TXN-000005` | `ACC-0718` | `CARD-1310` | `MCH-1884` | mobile | 219.51 | AUD | 2026-06-15T06:05:39Z | settled |
| `TXN-000010` | `ACC-6460` | `CARD-5889` | `MCH-1086` | pos | 434.81 | AUD | 2026-06-20T06:54:45Z | settled |
| `TXN-000011` | `ACC-5184` | `CARD-2323` | `MCH-0673` | mobile | 206.87 | AUD | 2026-06-17T21:41:43Z | settled |

## Defects-manifest sample

This is a deterministic slice of `g3_dev.bronze.defects_manifest` for Batch 2.
Each row has a matching sanitized `RUN-02` quarantine entry shown below.

| Record key | Rule ID | Rule name | Manifest failure reason | Severity |
|---|---|---|---|---|
| `TXN-000029` | `DQ-TXN-ACCT-FK` | account_id must exist in accounts | orphan account+card | quarantine |
| `TXN-000009` | `DQ-TXN-AMT-POS` | amount must be > 0 | negative amount | quarantine |
| `TXN-000104` | `DQ-TXN-CARD-ACTIVE` | transaction must use an active card | uses closed card | quarantine |
| `TXN-000006` | `DQ-TXN-ID-DUP` | transaction_id must be unique | duplicate transaction_id | quarantine |
| `TXN-000027` | `DQ-TXN-MERCH-REQ` | merchant_id is required | missing merchant_id | quarantine |
| `TXN-000047` | `DQ-TXN-TS-FUTURE` | txn_ts must not be in the future | future timestamp | quarantine |

## Sanitized quarantine output sample

Source: `g3_dev.silver.quarantine_records`, `run_id = 'RUN-02'`. The
restricted `raw_record` column is omitted.

| Record key | Rule ID | Quarantine failure reason | Severity | Disposition | Detected at (UTC) |
|---|---|---|---|---|---|
| `TXN-000029` | `DQ-TXN-ACCT-FK` | account_id does not resolve to Silver accounts | quarantine | quarantined | 2026-07-27 03:07:15.987477 |
| `TXN-000009` | `DQ-TXN-AMT-POS` | amount is missing, invalid, or not positive | quarantine | quarantined | 2026-07-27 03:07:15.987477 |
| `TXN-000104` | `DQ-TXN-CARD-ACTIVE` | transaction uses a closed card | quarantine | quarantined | 2026-07-27 03:07:15.987477 |
| `TXN-000006` | `DQ-TXN-ID-DUP` | duplicate transaction_id | quarantine | quarantined | 2026-07-27 03:07:15.987477 |
| `TXN-000027` | `DQ-TXN-MERCH-REQ` | missing merchant_id | quarantine | quarantined | 2026-07-27 03:07:15.987477 |
| `TXN-000047` | `DQ-TXN-TS-FUTURE` | txn_ts is missing, invalid, or after RUN_DATE | quarantine | quarantined | 2026-07-27 03:07:15.987477 |

## Sanitized Silver output sample

These rows are from `g3_dev.silver.transactions`, Batch 2. Silver has cast
`txn_ts` to a timestamp and `amount` to `decimal(12,2)`; no customer, contact,
PAN, IP-address, free-text, or quarantine `raw_record` fields are included.

| Transaction | Account | Card | Merchant | Channel | Amount | Currency | Transaction timestamp | Status |
|---|---|---|---|---|---:|---|---|---|
| `TXN-000002` | `ACC-1478` | `CARD-0421` | `MCH-0501` | online | 15.09 | SGD | 2026-06-12 05:13:03 | settled |
| `TXN-000004` | `ACC-5570` | `CARD-6619` | `MCH-0978` | mobile | 339.98 | USD | 2026-06-11 06:19:37 | settled |
| `TXN-000005` | `ACC-0718` | `CARD-1310` | `MCH-1884` | mobile | 219.51 | AUD | 2026-06-15 06:05:39 | settled |
| `TXN-000010` | `ACC-6460` | `CARD-5889` | `MCH-1086` | pos | 434.81 | AUD | 2026-06-20 06:54:45 | settled |
| `TXN-000011` | `ACC-5184` | `CARD-2323` | `MCH-0673` | mobile | 206.87 | AUD | 2026-06-17 21:41:43 | settled |

## Reconciliation

| Measure | Result |
|---|---:|
| Batch-2 transaction defects-manifest entries | 24,000 |
| Manifest entries matched by rule and record key in `RUN-02` quarantine | 23,569 |
| Matched quarantined transaction keys present in Batch-2 Silver transactions | 0 |

The zero Silver overlap demonstrates the clean-output exclusion for the
matched sample population. It is **not** a claim of complete manifest recall:
431 transaction manifest entries did not match the observed `RUN-02`
quarantine entries, and the expected `RUN-02-DQ` run was absent. This package
therefore provides generated-output evidence, not a replacement for a passed
DQ validation run.

## Repeatable extraction logic

The evidence was selected from the latest coherent Batch 2 snapshot with
queries equivalent to the following:

```sql
-- Clean source rows
SELECT b.*
FROM g3_dev.bronze.transactions b
LEFT ANTI JOIN g3_dev.silver.quarantine_records q
  ON q.source_table = 'transactions'
 AND q.source_record_id = b._source_record_id
 AND q.run_id = 'RUN-02'
WHERE b._batch_id = 2
ORDER BY b.transaction_id
LIMIT 5;

-- Manifest and sanitized quarantine reconciliation
SELECT m.record_key, m.rule_id, q.failure_reason, q.disposition, q.detected_at
FROM g3_dev.bronze.defects_manifest m
JOIN g3_dev.silver.quarantine_records q
  ON q.source_table = m.source_table
 AND q.record_key = m.record_key
 AND q.rule_id = m.rule_id
 AND q.run_id = 'RUN-02'
WHERE m._batch_id = 2
  AND m.source_table = 'transactions';
```
