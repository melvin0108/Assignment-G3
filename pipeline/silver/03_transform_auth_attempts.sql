-- ============================================================================
-- SILVER AUTH ATTEMPTS (M4) - g3_dev.silver.auth_attempts
-- ============================================================================
--
-- Purpose:
--   Transform bronze.auth_attempts into typed Silver authorization activity.
--
-- Prerequisites:
--   1. pipeline/bronze/01_ingest_bronze.sql has loaded g3_dev.bronze.*
--   2. pipeline/silver/02_transform_transactions.sql has created
--      silver.transactions
--
-- Handling:
--   * Auth attempts must resolve to a Silver transaction.
--   * auth_ts must not be after the linked transaction timestamp.
--   * Rows failing either condition are quarantined and excluded.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS g3_dev.silver;
CREATE SCHEMA IF NOT EXISTS g3_dev.gov;

CREATE TABLE IF NOT EXISTS g3_dev.silver.quarantine_records (
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

CREATE TABLE IF NOT EXISTS g3_dev.silver.auth_attempts (
  attempt_id            STRING,
  transaction_id        STRING,
  decision              STRING,
  decline_reason        STRING,
  auth_ts               TIMESTAMP,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS g3_dev.gov.metadata_lineage (
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

DELETE FROM g3_dev.silver.quarantine_records
WHERE run_id = 'RUN-20260706-1'
  AND source_table = 'auth_attempts';

WITH checked AS (
  SELECT
    a.*,
    try_to_timestamp(replace(replace(a.auth_ts, 'T', ' '), 'Z', '')) AS auth_ts_typed,
    t.transaction_id AS silver_transaction_id,
    t.txn_ts AS silver_txn_ts
  FROM g3_dev.bronze.auth_attempts a
  LEFT JOIN g3_dev.silver.transactions t
    ON a.transaction_id = t.transaction_id
)
INSERT INTO g3_dev.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260706-1',
  'auth_attempts',
  _source_record_id,
  attempt_id,
  rule_id,
  rule_name,
  failure_reason,
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'attempt_id', attempt_id,
    'transaction_id', transaction_id,
    'decision', decision,
    'decline_reason', decline_reason,
    'auth_ts', auth_ts
  )),
  current_timestamp()
FROM (
  SELECT *, 'DQ-AUTH-TXN-FK' AS rule_id, 'transaction_id must exist in transactions' AS rule_name, 'transaction_id does not resolve to Silver transactions' AS failure_reason
  FROM checked
  WHERE silver_transaction_id IS NULL

  UNION ALL
  SELECT *, 'DQ-AUTH-TS-ORDER', 'auth_ts must not be later than txn_ts', 'auth_ts is missing, invalid, or after linked transaction timestamp'
  FROM checked
  WHERE auth_ts_typed IS NULL
     OR (silver_transaction_id IS NOT NULL AND auth_ts_typed > silver_txn_ts)
);

INSERT OVERWRITE g3_dev.silver.auth_attempts
WITH checked AS (
  SELECT
    a.*,
    try_to_timestamp(replace(replace(a.auth_ts, 'T', ' '), 'Z', '')) AS auth_ts_typed,
    t.transaction_id AS silver_transaction_id,
    t.txn_ts AS silver_txn_ts
  FROM g3_dev.bronze.auth_attempts a
  LEFT JOIN g3_dev.silver.transactions t
    ON a.transaction_id = t.transaction_id
)
SELECT
  attempt_id,
  transaction_id,
  lower(trim(decision)) AS decision,
  NULLIF(trim(decline_reason), '') AS decline_reason,
  auth_ts_typed AS auth_ts,
  _source_file,
  _source_file_mod_time,
  _ingest_ts,
  _run_id,
  _batch_id,
  _source_record_id,
  _record_hash
FROM checked
WHERE silver_transaction_id IS NOT NULL
  AND auth_ts_typed IS NOT NULL
  AND auth_ts_typed <= silver_txn_ts;

DELETE FROM g3_dev.gov.metadata_lineage
WHERE target_schema = 'silver'
  AND target_table = 'auth_attempts';

INSERT INTO g3_dev.gov.metadata_lineage VALUES
  ('g3_dev', 'bronze', 'auth_attempts', 'attempt_id', 'g3_dev', 'silver', 'auth_attempts', 'attempt_id', 'Direct copy'),
  ('g3_dev', 'bronze', 'auth_attempts', 'transaction_id', 'g3_dev', 'silver', 'auth_attempts', 'transaction_id', 'Direct copy after Silver transaction relationship check'),
  ('g3_dev', 'bronze', 'auth_attempts', 'decision', 'g3_dev', 'silver', 'auth_attempts', 'decision', 'Lowercased and trimmed'),
  ('g3_dev', 'bronze', 'auth_attempts', 'decline_reason', 'g3_dev', 'silver', 'auth_attempts', 'decline_reason', 'Trimmed; empty string converted to NULL'),
  ('g3_dev', 'bronze', 'auth_attempts', 'auth_ts', 'g3_dev', 'silver', 'auth_attempts', 'auth_ts', 'Parsed to TIMESTAMP; invalid or after transaction timestamp quarantined');

SELECT
  'silver.auth_attempts' AS table_name,
  COUNT(*) AS silver_rows
FROM g3_dev.silver.auth_attempts
UNION ALL
SELECT
  'auth_attempts quarantine rows',
  COUNT(*)
FROM g3_dev.silver.quarantine_records
WHERE run_id = 'RUN-20260706-1'
  AND source_table = 'auth_attempts';
