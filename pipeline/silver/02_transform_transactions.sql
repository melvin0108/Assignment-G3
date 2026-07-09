-- ============================================================================
-- SILVER TRANSACTIONS (M4) - g3_catalog.silver.transactions
-- ============================================================================
--
-- Purpose:
--   Transform bronze.transactions into a typed Silver fact table.
--
-- Prerequisites:
--   1. pipeline/bronze/01_ingest_bronze.sql has loaded g3_catalog.bronze.*
--   2. pipeline/dq/01_setup.sql has created silver.quarantine_records
--   3. pipeline/silver/01_transform_pii_tables.sql has created
--      silver.accounts and silver.cards
--
-- Handling:
--   * Invalid source rows are written to silver.quarantine_records.
--   * Duplicate transaction_id rows keep rn = 1 and quarantine rn > 1.
--   * Broken account/card relationships and closed-card use are quarantined.
--   * The Silver table contains only rows that pass this table's checks.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS g3_catalog.silver;
CREATE SCHEMA IF NOT EXISTS g3_catalog.gov;

CREATE TABLE IF NOT EXISTS g3_catalog.silver.quarantine_records (
  run_id           STRING,
  source_table     STRING,
  source_record_id STRING,
  record_key       STRING,
  rule_id          STRING,
  rule_name        STRING,
  failure_reason   STRING,
  severity         STRING,
  disposition      STRING,
  raw_record       STRING,
  detected_at      TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS g3_catalog.silver.transactions (
  transaction_id        STRING,
  account_id            STRING,
  card_id               STRING,
  merchant_id           STRING,
  channel               STRING,
  amount                DECIMAL(12,2),
  currency              STRING,
  txn_ts                TIMESTAMP,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS g3_catalog.gov.metadata_lineage (
  source_catalog       STRING,
  source_schema        STRING,
  source_table         STRING,
  source_field         STRING,
  target_catalog       STRING,
  target_schema        STRING,
  target_table         STRING,
  target_field         STRING,
  transformation_logic STRING
) USING DELTA;

DELETE FROM g3_catalog.silver.quarantine_records
WHERE run_id = 'RUN-20260706-1'
  AND source_table = 'transactions';

WITH checked AS (
  SELECT
    t.*,
    try_cast(t.amount AS DECIMAL(12,2)) AS amount_typed,
    try_to_timestamp(replace(replace(t.txn_ts, 'T', ' '), 'Z', '')) AS txn_ts_typed,
    row_number() OVER (
      PARTITION BY t.transaction_id
      ORDER BY t._ingest_ts ASC, t._record_hash ASC
    ) AS rn_txn,
    a.account_id AS silver_account_id,
    c.card_id AS silver_card_id,
    c.status AS silver_card_status
  FROM g3_catalog.bronze.transactions t
  LEFT JOIN g3_catalog.silver.accounts a
    ON t.account_id = a.account_id
  LEFT JOIN g3_catalog.silver.cards c
    ON t.card_id = c.card_id
)
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260706-1',
  'transactions',
  _source_record_id,
  transaction_id,
  rule_id,
  rule_name,
  failure_reason,
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'transaction_id', transaction_id,
    'account_id', account_id,
    'card_id', card_id,
    'merchant_id', merchant_id,
    'channel', channel,
    'amount', amount,
    'currency', currency,
    'txn_ts', txn_ts,
    'status', status
  )),
  current_timestamp()
FROM (
  SELECT *, 'DQ-TXN-AMT-POS' AS rule_id, 'amount must be > 0' AS rule_name, 'amount is missing, invalid, or not positive' AS failure_reason
  FROM checked
  WHERE amount_typed IS NULL OR amount_typed <= 0

  UNION ALL
  SELECT *, 'DQ-TXN-MERCH-REQ', 'merchant_id is required', 'missing merchant_id'
  FROM checked
  WHERE merchant_id IS NULL OR trim(merchant_id) = ''

  UNION ALL
  SELECT *, 'DQ-TXN-TS-FUTURE', 'txn_ts must not be in the future', 'txn_ts is missing, invalid, or after RUN_DATE'
  FROM checked
  WHERE txn_ts_typed IS NULL
     OR txn_ts_typed > TIMESTAMP '2026-07-06 23:59:59'

  UNION ALL
  SELECT *, 'DQ-TXN-ID-DUP', 'transaction_id must be unique', 'duplicate transaction_id'
  FROM checked
  WHERE rn_txn > 1

  UNION ALL
  SELECT *, 'DQ-TXN-ACCT-FK', 'account_id must exist in accounts', 'account_id or card_id does not resolve to Silver account/card'
  FROM checked
  WHERE silver_account_id IS NULL
     OR (card_id IS NOT NULL AND trim(card_id) != '' AND silver_card_id IS NULL)

  UNION ALL
  SELECT *, 'DQ-TXN-CARD-ACTIVE', 'transaction must use an active card', 'transaction uses a closed card'
  FROM checked
  WHERE silver_card_status = 'closed'
);

INSERT OVERWRITE g3_catalog.silver.transactions
WITH checked AS (
  SELECT
    t.*,
    try_cast(t.amount AS DECIMAL(12,2)) AS amount_typed,
    try_to_timestamp(replace(replace(t.txn_ts, 'T', ' '), 'Z', '')) AS txn_ts_typed,
    row_number() OVER (
      PARTITION BY t.transaction_id
      ORDER BY t._ingest_ts ASC, t._record_hash ASC
    ) AS rn_txn,
    a.account_id AS silver_account_id,
    c.card_id AS silver_card_id,
    c.status AS silver_card_status
  FROM g3_catalog.bronze.transactions t
  LEFT JOIN g3_catalog.silver.accounts a
    ON t.account_id = a.account_id
  LEFT JOIN g3_catalog.silver.cards c
    ON t.card_id = c.card_id
)
SELECT
  transaction_id,
  account_id,
  card_id,
  merchant_id,
  lower(trim(channel)) AS channel,
  amount_typed AS amount,
  upper(trim(currency)) AS currency,
  txn_ts_typed AS txn_ts,
  lower(trim(status)) AS status,
  _source_file,
  _source_file_mod_time,
  _ingest_ts,
  _run_id,
  _batch_id,
  _source_record_id,
  _record_hash
FROM checked
WHERE amount_typed > 0
  AND merchant_id IS NOT NULL
  AND trim(merchant_id) != ''
  AND txn_ts_typed <= TIMESTAMP '2026-07-06 23:59:59'
  AND rn_txn = 1
  AND silver_account_id IS NOT NULL
  AND (card_id IS NULL OR trim(card_id) = '' OR silver_card_id IS NOT NULL)
  AND COALESCE(silver_card_status, '') != 'closed';

DELETE FROM g3_catalog.gov.metadata_lineage
WHERE target_schema = 'silver'
  AND target_table = 'transactions';

INSERT INTO g3_catalog.gov.metadata_lineage VALUES
  ('g3_catalog', 'bronze', 'transactions', 'transaction_id', 'g3_catalog', 'silver', 'transactions', 'transaction_id', 'Direct copy after duplicate filtering'),
  ('g3_catalog', 'bronze', 'transactions', 'account_id', 'g3_catalog', 'silver', 'transactions', 'account_id', 'Direct copy after Silver account relationship check'),
  ('g3_catalog', 'bronze', 'transactions', 'card_id', 'g3_catalog', 'silver', 'transactions', 'card_id', 'Direct copy after Silver card relationship and active-card check'),
  ('g3_catalog', 'bronze', 'transactions', 'merchant_id', 'g3_catalog', 'silver', 'transactions', 'merchant_id', 'Direct copy when present'),
  ('g3_catalog', 'bronze', 'transactions', 'channel', 'g3_catalog', 'silver', 'transactions', 'channel', 'Lowercased and trimmed'),
  ('g3_catalog', 'bronze', 'transactions', 'amount', 'g3_catalog', 'silver', 'transactions', 'amount', 'TRY_CAST to DECIMAL(12,2); non-positive or invalid values quarantined'),
  ('g3_catalog', 'bronze', 'transactions', 'currency', 'g3_catalog', 'silver', 'transactions', 'currency', 'Uppercased and trimmed'),
  ('g3_catalog', 'bronze', 'transactions', 'txn_ts', 'g3_catalog', 'silver', 'transactions', 'txn_ts', 'Parsed to TIMESTAMP; future or invalid timestamps quarantined'),
  ('g3_catalog', 'bronze', 'transactions', 'status', 'g3_catalog', 'silver', 'transactions', 'status', 'Lowercased and trimmed');

SELECT
  'silver.transactions' AS table_name,
  COUNT(*) AS silver_rows
FROM g3_catalog.silver.transactions
UNION ALL
SELECT
  'transactions quarantine rows',
  COUNT(*)
FROM g3_catalog.silver.quarantine_records
WHERE run_id = 'RUN-20260706-1'
  AND source_table = 'transactions';
