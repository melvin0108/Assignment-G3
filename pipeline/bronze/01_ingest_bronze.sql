-- ============================================================================
-- BRONZE INGEST  —  g3_catalog.bronze.*
-- ----------------------------------------------------------------------------
-- Loads the 25 raw mock CSVs (flat in Volume g3_catalog.bronze.raw_data) into
-- string-typed, append-only Delta tables with lineage metadata.
--
-- HOW TO RUN
--   1. Open a Databricks SQL notebook (language = SQL) attached to a
--      SQL Warehouse (Serverless or Pro). COPY INTO is not supported on
--      Classic SQL Warehouses.
--   2. Edit the two paths below ONLY if your volume lives somewhere other than
--      /Volumes/g3_catalog/bronze/raw_data  (verify in Catalog Explorer).
--   3. Run top-to-bottom. COPY INTO is idempotent — re-running does NOT duplicate.
--
-- BRONZE CONTRACT (docs/bronze-layer.md)
--   * Every source column stored as STRING (typing is Silver's job).
--   * 8 metadata columns on every table: _source_file, _source_file_mod_time,
--     _ingest_ts, _run_id, _batch_id, _source_record_id, _record_hash, _rescued_data.
--   * Append-only: never UPDATE/DELETE/OVERWRITE these tables.
--
-- NOTE: _defects_manifest.csv is intentionally NOT loaded here — it is the DQ
-- oracle and lands later in g3_catalog.gov._defects_manifest_staging (epic E3).
-- ============================================================================

CREATE CATALOG IF NOT EXISTS g3_catalog;
CREATE SCHEMA IF NOT EXISTS g3_catalog.bronze;

-- run_id is inlined as the literal 'RUN-20260706-1' in every COPY INTO below.
-- (We avoid SQL variables here: Databricks SQL notebooks treat a dollar-brace
--  marker as a widget parameter and error out. Change the literal in each block
--  if you ever need a different run id.)

-- ----------------------------------------------------------------------------
-- accounts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.accounts (
  account_id            STRING,
  customer_id           STRING,
  product_type          STRING,
  open_date             STRING,
  status                STRING,
  currency              STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.accounts
FROM (
  SELECT
    account_id, customer_id, product_type, open_date, status, currency,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    account_id              AS _source_record_id,
    sha2(concat_ws('|', account_id, customer_id, product_type, open_date, status, currency), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/accounts.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- auth_attempts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.auth_attempts (
  attempt_id            STRING,
  transaction_id        STRING,
  decision              STRING,
  decline_reason        STRING,
  auth_ts               STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.auth_attempts
FROM (
  SELECT
    attempt_id, transaction_id, decision, decline_reason, auth_ts,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    attempt_id              AS _source_record_id,
    sha2(concat_ws('|', attempt_id, transaction_id, decision, decline_reason, auth_ts), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/auth_attempts.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- branches
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.branches (
  branch_code           STRING,
  name                  STRING,
  country               STRING,
  region                STRING,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.branches
FROM (
  SELECT
    branch_code, name, country, region, status,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    branch_code             AS _source_record_id,
    sha2(concat_ws('|', branch_code, name, country, region, status), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/branches.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- cards
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.cards (
  card_id               STRING,
  account_id            STRING,
  card_type             STRING,
  pan                   STRING,
  expiry                STRING,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.cards
FROM (
  SELECT
    card_id, account_id, card_type, pan, expiry, status,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    card_id                 AS _source_record_id,
    sha2(concat_ws('|', card_id, account_id, card_type, pan, expiry, status), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/cards.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- case_parties  (composite key: case_id + party_type + party_id)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.case_parties (
  case_id               STRING,
  party_type            STRING,
  party_id              STRING,
  role                  STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.case_parties
FROM (
  SELECT
    case_id, party_type, party_id, role,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    concat_ws('|', case_id, party_type, party_id) AS _source_record_id,
    sha2(concat_ws('|', case_id, party_type, party_id, role), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/case_parties.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- case_status_types  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.case_status_types (
  status_code           STRING,
  description           STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.case_status_types
FROM (
  SELECT
    status_code, description,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    status_code             AS _source_record_id,
    sha2(concat_ws('|', status_code, description), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/case_status_types.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- case_transactions  (composite key: case_id + transaction_id)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.case_transactions (
  case_id               STRING,
  transaction_id        STRING,
  linked_at             STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.case_transactions
FROM (
  SELECT
    case_id, transaction_id, linked_at,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    concat_ws('|', case_id, transaction_id) AS _source_record_id,
    sha2(concat_ws('|', case_id, transaction_id, linked_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/case_transactions.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- channels  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.channels (
  channel_code          STRING,
  channel_name          STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.channels
FROM (
  SELECT
    channel_code, channel_name,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    channel_code            AS _source_record_id,
    sha2(concat_ws('|', channel_code, channel_name), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/channels.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- chargebacks
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.chargebacks (
  chargeback_id         STRING,
  dispute_id            STRING,
  scheme                STRING,
  amount                STRING,
  stage                 STRING,
  processed_at          STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.chargebacks
FROM (
  SELECT
    chargeback_id, dispute_id, scheme, amount, stage, processed_at,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    chargeback_id           AS _source_record_id,
    sha2(concat_ws('|', chargeback_id, dispute_id, scheme, amount, stage, processed_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/chargebacks.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- countries  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.countries (
  iso_code              STRING,
  name                  STRING,
  region                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.countries
FROM (
  SELECT
    iso_code, name, region,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    iso_code                AS _source_record_id,
    sha2(concat_ws('|', iso_code, name, region), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/countries.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- currencies  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.currencies (
  currency_code         STRING,
  name                  STRING,
  decimals              STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.currencies
FROM (
  SELECT
    currency_code, name, decimals,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    currency_code           AS _source_record_id,
    sha2(concat_ws('|', currency_code, name, decimals), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/currencies.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- customer_contact_logs  (PII in free-text 'note')
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.customer_contact_logs (
  contact_id            STRING,
  customer_id           STRING,
  direction             STRING,
  contact_method        STRING,
  do_not_contact        STRING,
  contacted_at          STRING,
  employee_id           STRING,
  note                  STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.customer_contact_logs
FROM (
  SELECT
    contact_id, customer_id, direction, contact_method, do_not_contact,
    contacted_at, employee_id, note,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    contact_id              AS _source_record_id,
    sha2(concat_ws('|', contact_id, customer_id, direction, contact_method, do_not_contact, contacted_at, employee_id, note), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/customer_contact_logs.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- customers  (PII)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.customers (
  customer_id           STRING,
  first_name            STRING,
  last_name             STRING,
  dob                   STRING,
  email                 STRING,
  phone                 STRING,
  address               STRING,
  tax_id                STRING,
  created_at            STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.customers
FROM (
  SELECT
    customer_id, first_name, last_name, dob, email, phone, address, tax_id, created_at,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    customer_id             AS _source_record_id,
    sha2(concat_ws('|', customer_id, first_name, last_name, dob, email, phone, address, tax_id, created_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/customers.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- date_dim  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.date_dim (
  date_id               STRING,
  year                  STRING,
  month                 STRING,
  quarter               STRING,
  is_weekend            STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.date_dim
FROM (
  SELECT
    date_id, year, month, quarter, is_weekend,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    date_id                 AS _source_record_id,
    sha2(concat_ws('|', date_id, year, month, quarter, is_weekend), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/date_dim.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- dispute_reason_codes  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.dispute_reason_codes (
  reason_code           STRING,
  description           STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.dispute_reason_codes
FROM (
  SELECT
    reason_code, description,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    reason_code             AS _source_record_id,
    sha2(concat_ws('|', reason_code, description), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/dispute_reason_codes.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- disputes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.disputes (
  dispute_id            STRING,
  transaction_id        STRING,
  reason_code           STRING,
  amount                STRING,
  status                STRING,
  raised_at             STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.disputes
FROM (
  SELECT
    dispute_id, transaction_id, reason_code, amount, status, raised_at,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    dispute_id              AS _source_record_id,
    sha2(concat_ws('|', dispute_id, transaction_id, reason_code, amount, status, raised_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/disputes.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- employees  (PII)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.employees (
  employee_id           STRING,
  full_name             STRING,
  email                 STRING,
  team                  STRING,
  role                  STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.employees
FROM (
  SELECT
    employee_id, full_name, email, team, role,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    employee_id             AS _source_record_id,
    sha2(concat_ws('|', employee_id, full_name, email, team, role), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/employees.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- fraud_alerts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.fraud_alerts (
  alert_id              STRING,
  transaction_id        STRING,
  rule_name             STRING,
  score                 STRING,
  triggered_at          STRING,
  disposition           STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.fraud_alerts
FROM (
  SELECT
    alert_id, transaction_id, rule_name, score, triggered_at, disposition,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    alert_id                AS _source_record_id,
    sha2(concat_ws('|', alert_id, transaction_id, rule_name, score, triggered_at, disposition), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/fraud_alerts.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- fraud_types  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.fraud_types (
  fraud_type_code       STRING,
  description           STRING,
  severity              STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.fraud_types
FROM (
  SELECT
    fraud_type_code, description, severity,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    fraud_type_code         AS _source_record_id,
    sha2(concat_ws('|', fraud_type_code, description, severity), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/fraud_types.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- investigation_cases
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.investigation_cases (
  case_id               STRING,
  priority              STRING,
  status_code           STRING,
  fraud_type_code       STRING,
  owner_employee_id     STRING,
  opened_at             STRING,
  closed_at             STRING,
  legal_hold            STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.investigation_cases
FROM (
  SELECT
    case_id, priority, status_code, fraud_type_code, owner_employee_id,
    opened_at, closed_at, legal_hold,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    case_id                 AS _source_record_id,
    sha2(concat_ws('|', case_id, priority, status_code, fraud_type_code, owner_employee_id, opened_at, closed_at, legal_hold), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/investigation_cases.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- investigation_notes  (PII / PAN leaked in free text)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.investigation_notes (
  note_id               STRING,
  case_id               STRING,
  author_employee_id    STRING,
  note_text             STRING,
  created_at            STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.investigation_notes
FROM (
  SELECT
    note_id, case_id, author_employee_id, note_text, created_at,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    note_id                 AS _source_record_id,
    sha2(concat_ws('|', note_id, case_id, author_employee_id, note_text, created_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/investigation_notes.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- merchant_categories  (reference)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.merchant_categories (
  mcc                   STRING,
  category_name         STRING,
  category_group        STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.merchant_categories
FROM (
  SELECT
    mcc, category_name, category_group,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    mcc                     AS _source_record_id,
    sha2(concat_ws('|', mcc, category_name, category_group), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/merchant_categories.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- merchants
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.merchants (
  merchant_id           STRING,
  name                  STRING,
  mcc                   STRING,
  country               STRING,
  risk_rating           STRING,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.merchants
FROM (
  SELECT
    merchant_id, name, mcc, country, risk_rating, status,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    merchant_id             AS _source_record_id,
    sha2(concat_ws('|', merchant_id, name, mcc, country, risk_rating, status), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/merchants.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- transactions  (STRESS TABLE — 2M rows; may take several minutes)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.transactions (
  transaction_id        STRING,
  account_id            STRING,
  card_id               STRING,
  merchant_id           STRING,
  channel               STRING,
  amount                STRING,
  currency              STRING,
  txn_ts                STRING,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.transactions
FROM (
  SELECT
    transaction_id, account_id, card_id, merchant_id, channel,
    amount, currency, txn_ts, status,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    transaction_id          AS _source_record_id,
    sha2(concat_ws('|', transaction_id, account_id, card_id, merchant_id, channel, amount, currency, txn_ts, status), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/transactions.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- transaction_devices
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.transaction_devices (
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
  _record_hash          STRING,
  _rescued_data         STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.transaction_devices
FROM (
  SELECT
    device_id, transaction_id, device_type, ip, geo_country,
    _metadata.file_name     AS _source_file,
    _metadata.file_modification_time AS _source_file_mod_time,
    current_timestamp()     AS _ingest_ts,
    'RUN-20260706-1'        AS _run_id,
    CAST(1 AS BIGINT)       AS _batch_id,
    device_id               AS _source_record_id,
    sha2(concat_ws('|', device_id, transaction_id, device_type, ip, geo_country), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/transaction_devices.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ============================================================================
-- VERIFY  — row counts per bronze table (should all be > 0)
-- ============================================================================
SELECT 'accounts'            AS table_name, COUNT(*) AS rows FROM g3_catalog.bronze.accounts
UNION ALL SELECT 'auth_attempts',         COUNT(*) FROM g3_catalog.bronze.auth_attempts
UNION ALL SELECT 'branches',              COUNT(*) FROM g3_catalog.bronze.branches
UNION ALL SELECT 'cards',                 COUNT(*) FROM g3_catalog.bronze.cards
UNION ALL SELECT 'case_parties',          COUNT(*) FROM g3_catalog.bronze.case_parties
UNION ALL SELECT 'case_status_types',     COUNT(*) FROM g3_catalog.bronze.case_status_types
UNION ALL SELECT 'case_transactions',     COUNT(*) FROM g3_catalog.bronze.case_transactions
UNION ALL SELECT 'channels',              COUNT(*) FROM g3_catalog.bronze.channels
UNION ALL SELECT 'chargebacks',           COUNT(*) FROM g3_catalog.bronze.chargebacks
UNION ALL SELECT 'countries',             COUNT(*) FROM g3_catalog.bronze.countries
UNION ALL SELECT 'currencies',            COUNT(*) FROM g3_catalog.bronze.currencies
UNION ALL SELECT 'customer_contact_logs', COUNT(*) FROM g3_catalog.bronze.customer_contact_logs
UNION ALL SELECT 'customers',             COUNT(*) FROM g3_catalog.bronze.customers
UNION ALL SELECT 'date_dim',              COUNT(*) FROM g3_catalog.bronze.date_dim
UNION ALL SELECT 'dispute_reason_codes',  COUNT(*) FROM g3_catalog.bronze.dispute_reason_codes
UNION ALL SELECT 'disputes',              COUNT(*) FROM g3_catalog.bronze.disputes
UNION ALL SELECT 'employees',             COUNT(*) FROM g3_catalog.bronze.employees
UNION ALL SELECT 'fraud_alerts',          COUNT(*) FROM g3_catalog.bronze.fraud_alerts
UNION ALL SELECT 'fraud_types',           COUNT(*) FROM g3_catalog.bronze.fraud_types
UNION ALL SELECT 'investigation_cases',   COUNT(*) FROM g3_catalog.bronze.investigation_cases
UNION ALL SELECT 'investigation_notes',   COUNT(*) FROM g3_catalog.bronze.investigation_notes
UNION ALL SELECT 'merchant_categories',   COUNT(*) FROM g3_catalog.bronze.merchant_categories
UNION ALL SELECT 'merchants',             COUNT(*) FROM g3_catalog.bronze.merchants
UNION ALL SELECT 'transaction_devices',   COUNT(*) FROM g3_catalog.bronze.transaction_devices
UNION ALL SELECT 'transactions',          COUNT(*) FROM g3_catalog.bronze.transactions
ORDER BY table_name;