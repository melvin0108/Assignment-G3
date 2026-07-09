-- ============================================================================
-- SILVER TRANSACTION DEVICES (M4) - g3_dev.silver.transaction_devices
-- ============================================================================
--
-- Purpose:
--   Transform bronze.transaction_devices into protected Silver device activity.
--
-- Prerequisites:
--   1. pipeline/bronze/01_ingest_bronze.sql has loaded g3_dev.bronze.*
--   2. pipeline/silver/02_transform_transactions.sql has created
--      silver.transactions
--
-- Handling:
--   * Device rows must resolve to a Silver transaction.
--   * Missing device_type rows are quarantined and excluded.
--   * device_id is tokenized and ip is reduced to an IPv4 /24-style network.
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

CREATE TABLE IF NOT EXISTS g3_dev.silver.transaction_devices (
  device_id             STRING,
  transaction_id        STRING,
  device_type           STRING,
  ip                    STRING,
  geo_country           STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS g3_dev.gov.masking_policies (
  table_name        STRING,
  field_name        STRING,
  classification    STRING,
  protection_method STRING,
  allowed_role      STRING,
  owner             STRING
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
  AND source_table = 'transaction_devices';

WITH checked AS (
  SELECT
    d.*,
    t.transaction_id AS silver_transaction_id
  FROM g3_dev.bronze.transaction_devices d
  LEFT JOIN g3_dev.silver.transactions t
    ON d.transaction_id = t.transaction_id
)
INSERT INTO g3_dev.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260706-1',
  'transaction_devices',
  _source_record_id,
  device_id,
  rule_id,
  rule_name,
  failure_reason,
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'device_id', device_id,
    'transaction_id', transaction_id,
    'device_type', device_type,
    'ip', ip,
    'geo_country', geo_country
  )),
  current_timestamp()
FROM (
  SELECT *, 'DQ-DEV-TXN-FK' AS rule_id, 'transaction_id must exist in transactions' AS rule_name, 'transaction_id does not resolve to Silver transactions' AS failure_reason
  FROM checked
  WHERE silver_transaction_id IS NULL

  UNION ALL
  SELECT *, 'DQ-DEV-TYPE-REQ', 'device_type is required', 'missing device_type'
  FROM checked
  WHERE device_type IS NULL OR trim(device_type) = ''
);

INSERT OVERWRITE g3_dev.silver.transaction_devices
WITH checked AS (
  SELECT
    d.*,
    t.transaction_id AS silver_transaction_id
  FROM g3_dev.bronze.transaction_devices d
  LEFT JOIN g3_dev.silver.transactions t
    ON d.transaction_id = t.transaction_id
)
SELECT
  concat('DEV_', substring(sha2(concat(lower(trim(device_id)), 'NAB_SALT_2026'), 256), 1, 16)) AS device_id,
  transaction_id,
  lower(trim(device_type)) AS device_type,
  CASE
    WHEN ip RLIKE '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'
      THEN regexp_replace(ip, '^(\\d+\\.\\d+\\.\\d+)\\.\\d+$', '$1.0/24')
    WHEN ip IS NULL OR trim(ip) = ''
      THEN NULL
    ELSE concat('IP_HASH_', substring(sha2(concat(lower(trim(ip)), 'NAB_SALT_2026'), 256), 1, 16))
  END AS ip,
  upper(trim(geo_country)) AS geo_country,
  _source_file,
  _source_file_mod_time,
  _ingest_ts,
  _run_id,
  _batch_id,
  _source_record_id,
  _record_hash
FROM checked
WHERE silver_transaction_id IS NOT NULL
  AND device_type IS NOT NULL
  AND trim(device_type) != '';

DELETE FROM g3_dev.gov.masking_policies
WHERE table_name = 'transaction_devices';

INSERT INTO g3_dev.gov.masking_policies VALUES
  ('transaction_devices', 'device_id', 'device/session identifier', 'tokenize with salted SHA256 prefix', 'unprivileged', 'M4'),
  ('transaction_devices', 'ip', 'network identifier', 'truncate IPv4 to /24 or hash non-IPv4 value', 'unprivileged', 'M4');

DELETE FROM g3_dev.gov.metadata_lineage
WHERE target_schema = 'silver'
  AND target_table = 'transaction_devices';

INSERT INTO g3_dev.gov.metadata_lineage VALUES
  ('g3_dev', 'bronze', 'transaction_devices', 'device_id', 'g3_dev', 'silver', 'transaction_devices', 'device_id', 'Tokenized with salted SHA256 prefix'),
  ('g3_dev', 'bronze', 'transaction_devices', 'transaction_id', 'g3_dev', 'silver', 'transaction_devices', 'transaction_id', 'Direct copy after Silver transaction relationship check'),
  ('g3_dev', 'bronze', 'transaction_devices', 'device_type', 'g3_dev', 'silver', 'transaction_devices', 'device_type', 'Lowercased and trimmed; missing values quarantined'),
  ('g3_dev', 'bronze', 'transaction_devices', 'ip', 'g3_dev', 'silver', 'transaction_devices', 'ip', 'IPv4 reduced to /24-style network; non-IPv4 hashed'),
  ('g3_dev', 'bronze', 'transaction_devices', 'geo_country', 'g3_dev', 'silver', 'transaction_devices', 'geo_country', 'Uppercased and trimmed');

SELECT
  'silver.transaction_devices' AS table_name,
  COUNT(*) AS silver_rows
FROM g3_dev.silver.transaction_devices
UNION ALL
SELECT
  'transaction_devices quarantine rows',
  COUNT(*)
FROM g3_dev.silver.quarantine_records
WHERE run_id = 'RUN-20260706-1'
  AND source_table = 'transaction_devices';
