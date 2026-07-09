-- ============================================================================
-- SILVER INGESTION & DATA PRIVACY (M3) — g3_dev.silver.*
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. SETUP SCHEMAS
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS g3_dev.silver;
CREATE SCHEMA IF NOT EXISTS g3_dev.gov;

-- ----------------------------------------------------------------------------
-- 2. CREATE SILVER TABLES
-- ----------------------------------------------------------------------------

-- customers
CREATE TABLE IF NOT EXISTS g3_dev.silver.customers (
  customer_id           STRING,
  first_name            STRING,
  last_name             STRING,
  dob                   STRING,  -- generalized to age band
  email                 STRING,  -- masked
  phone                 STRING,  -- masked
  address               STRING,  -- hashed
  tax_id                STRING,  -- hashed
  created_at            TIMESTAMP,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

-- employees
CREATE TABLE IF NOT EXISTS g3_dev.silver.employees (
  employee_id           STRING,
  full_name             STRING,  -- hashed
  email                 STRING,  -- hashed
  team                  STRING,
  role                  STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

-- accounts
CREATE TABLE IF NOT EXISTS g3_dev.silver.accounts (
  account_id            STRING,
  customer_id           STRING,
  product_type          STRING,
  open_date             DATE,
  status                STRING,
  currency              STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

-- cards
CREATE TABLE IF NOT EXISTS g3_dev.silver.cards (
  card_id               STRING,
  account_id            STRING,
  card_type             STRING,
  pan                   STRING,  -- masked
  expiry                STRING,
  status                STRING,
  _source_file          STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts            TIMESTAMP,
  _run_id               STRING,
  _batch_id             BIGINT,
  _source_record_id     STRING,
  _record_hash          STRING
) USING DELTA;

-- ----------------------------------------------------------------------------
-- 3. GOVERNANCE SCHEMAS
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 4. CLEAN & REPOPULATE METADATA FOR RUN
-- ----------------------------------------------------------------------------
-- Ensure idempotency by dropping quarantine entries for the current run first
DELETE FROM g3_dev.silver.quarantine_records 
WHERE source_table IN ('customers', 'employees', 'accounts', 'cards') 
  AND run_id = 'RUN-20260706-1';

-- ============================================================================
-- 5. PIPELINE FOR CUSTOMERS
-- ============================================================================
WITH cust_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY customer_id ORDER BY created_at ASC, _ingest_ts ASC) AS rn_pk,
    row_number() OVER (PARTITION BY first_name, last_name, dob, address, tax_id ORDER BY customer_id ASC, _ingest_ts ASC) AS rn_near
  FROM g3_dev.bronze.customers
),
cust_failed AS (
  SELECT
    'RUN-20260706-1' AS run_id,
    'customers' AS source_table,
    _source_record_id,
    customer_id AS record_key,
    CASE
      WHEN rn_pk > 1 THEN 'DQ-CUST-ID-DUP'
      WHEN email IS NOT NULL AND email != '' AND email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+$' THEN 'DQ-CUST-EMAIL-FMT'
      WHEN rn_near > 1 THEN 'DQ-CUST-NEAR-DUP'
    END AS rule_id,
    CASE
      WHEN rn_pk > 1 THEN 'customer_id must be unique'
      WHEN email IS NOT NULL AND email != '' AND email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+$' THEN 'email must match pattern if present'
      WHEN rn_near > 1 THEN 'no two customers share name+dob+address+tax_id'
    END AS rule_name,
    CASE
      WHEN rn_pk > 1 THEN concat('Duplicate customer_id found: ', customer_id)
      WHEN email IS NOT NULL AND email != '' AND email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+$' THEN concat('Invalid email format: ', email)
      WHEN rn_near > 1 THEN concat('Near duplicate customer found with same details. tax_id: ', tax_id)
    END AS failure_reason,
    'quarantine' AS severity,
    'quarantined' AS disposition,
    to_json(struct(customer_id, first_name, last_name, dob, email, phone, address, tax_id, created_at)) AS raw_record,
    current_timestamp() AS detected_at
  FROM cust_ranked
  WHERE rn_pk > 1
     OR (email IS NOT NULL AND email != '' AND email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+$')
     OR rn_near > 1
)
INSERT INTO g3_dev.silver.quarantine_records
SELECT * FROM cust_failed;

INSERT OVERWRITE g3_dev.silver.customers
WITH cust_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY customer_id ORDER BY created_at ASC, _ingest_ts ASC) AS rn_pk,
    row_number() OVER (PARTITION BY first_name, last_name, dob, address, tax_id ORDER BY customer_id ASC, _ingest_ts ASC) AS rn_near
  FROM g3_dev.bronze.customers
)
SELECT
  c.customer_id,
  concat('TOK_', substring(sha2(concat(lower(trim(c.first_name)), 'NAB_SALT_2026'), 256), 1, 16)) AS first_name,
  concat('TOK_', substring(sha2(concat(lower(trim(c.last_name)), 'NAB_SALT_2026'), 256), 1, 16)) AS last_name,
  CASE
    WHEN c.dob IS NULL OR c.dob = '' THEN 'UNKNOWN'
    ELSE
      CASE
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) < 18 THEN 'Under 18'
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) BETWEEN 18 AND 25 THEN '18-25'
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) BETWEEN 26 AND 35 THEN '26-35'
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) BETWEEN 36 AND 45 THEN '36-45'
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) BETWEEN 46 AND 55 THEN '46-55'
        WHEN floor(months_between(CAST('2026-07-06' AS DATE), CAST(c.dob AS DATE)) / 12) BETWEEN 56 AND 65 THEN '56-65'
        ELSE '66+'
      END
  END AS dob,
  CASE 
    WHEN c.email IS NULL OR c.email = '' THEN NULL
    WHEN c.email NOT LIKE '%@%' THEN 'invalid_masked_email'
    ELSE concat(
      substring(split(c.email, '@')[0], 1, 1),
      '***@',
      regexp_replace(split(c.email, '@')[1], '^[^.]+', '***')
    )
  END AS email,
  CASE
    WHEN c.phone IS NULL OR c.phone = '' THEN NULL
    ELSE concat('******', right(c.phone, 4))
  END AS phone,
  CASE
    WHEN c.address IS NULL OR c.address = '' THEN NULL
    ELSE sha2(concat(lower(trim(c.address)), 'NAB_SALT_2026'), 256)
  END AS address,
  CASE
    WHEN c.tax_id IS NULL OR c.tax_id = '' THEN NULL
    ELSE sha2(concat(lower(trim(c.tax_id)), 'NAB_SALT_2026'), 256)
  END AS tax_id,
  CAST(c.created_at AS TIMESTAMP) AS created_at,
  c._source_file,
  c._source_file_mod_time,
  c._ingest_ts,
  c._run_id,
  c._batch_id,
  c._source_record_id,
  c._record_hash
FROM cust_ranked c
LEFT ANTI JOIN g3_dev.silver.quarantine_records q
  ON c._source_record_id = q.source_record_id
 AND q.source_table = 'customers';

-- ============================================================================
-- 6. PIPELINE FOR EMPLOYEES
-- ============================================================================
WITH emp_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY email ORDER BY employee_id ASC, _ingest_ts ASC) AS rn_email,
    row_number() OVER (PARTITION BY full_name ORDER BY employee_id ASC, _ingest_ts ASC) AS rn_name
  FROM g3_dev.bronze.employees
),
emp_failed AS (
  SELECT
    'RUN-20260706-1' AS run_id,
    'employees' AS source_table,
    _source_record_id,
    employee_id AS record_key,
    CASE
      WHEN rn_email > 1 THEN 'DQ-EMP-EMAIL-UNIQ'
      WHEN rn_name > 1 THEN 'DQ-EMP-NAME-NEAR-DUP'
    END AS rule_id,
    CASE
      WHEN rn_email > 1 THEN 'email must be unique'
      WHEN rn_name > 1 THEN 'flag near-duplicate employee names'
    END AS rule_name,
    CASE
      WHEN rn_email > 1 THEN concat('Duplicate email found: ', email)
      WHEN rn_name > 1 THEN concat('Duplicate employee name found: ', full_name)
    END AS failure_reason,
    'quarantine' AS severity,
    'quarantined' AS disposition,
    to_json(struct(employee_id, full_name, email, team, role)) AS raw_record,
    current_timestamp() AS detected_at
  FROM emp_ranked
  WHERE rn_email > 1 OR rn_name > 1
)
INSERT INTO g3_dev.silver.quarantine_records
SELECT * FROM emp_failed;

INSERT OVERWRITE g3_dev.silver.employees
WITH emp_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY email ORDER BY employee_id ASC, _ingest_ts ASC) AS rn_email,
    row_number() OVER (PARTITION BY full_name ORDER BY employee_id ASC, _ingest_ts ASC) AS rn_name
  FROM g3_dev.bronze.employees
)
SELECT
  e.employee_id,
  sha2(concat(lower(trim(e.full_name)), 'NAB_SALT_2026'), 256) AS full_name,
  sha2(concat(lower(trim(e.email)), 'NAB_SALT_2026'), 256) AS email,
  e.team,
  e.role,
  e._source_file,
  e._source_file_mod_time,
  e._ingest_ts,
  e._run_id,
  e._batch_id,
  e._source_record_id,
  e._record_hash
FROM emp_ranked e
LEFT ANTI JOIN g3_dev.silver.quarantine_records q
  ON e._source_record_id = q.source_record_id
 AND q.source_table = 'employees';

-- ============================================================================
-- 7. PIPELINE FOR ACCOUNTS
-- ============================================================================
WITH acc_checked AS (
  SELECT
    *,
    CASE
      WHEN open_date IS NOT NULL AND open_date != '' AND CAST(open_date AS DATE) > CAST('2026-07-06' AS DATE) THEN 'DQ-ACC-OPENDATE-FUTURE'
      WHEN customer_id NOT IN (SELECT customer_id FROM g3_dev.silver.customers) THEN 'DQ-ACC-CUST-FK'
      ELSE NULL
    END AS failed_rule_id
  FROM g3_dev.bronze.accounts
),
acc_failed AS (
  SELECT
    'RUN-20260706-1' AS run_id,
    'accounts' AS source_table,
    _source_record_id,
    account_id AS record_key,
    failed_rule_id AS rule_id,
    CASE
      WHEN failed_rule_id = 'DQ-ACC-OPENDATE-FUTURE' THEN 'open_date must not be in the future'
      WHEN failed_rule_id = 'DQ-ACC-CUST-FK' THEN 'customer_id must exist in customers'
    END AS rule_name,
    CASE
      WHEN failed_rule_id = 'DQ-ACC-OPENDATE-FUTURE' THEN concat('open_date in the future: ', open_date)
      WHEN failed_rule_id = 'DQ-ACC-CUST-FK' THEN concat('Referential integrity break: customer_id ', customer_id, ' not found in silver.customers')
    END AS failure_reason,
    'quarantine' AS severity,
    'quarantined' AS disposition,
    to_json(struct(account_id, customer_id, product_type, open_date, status, currency)) AS raw_record,
    current_timestamp() AS detected_at
  FROM acc_checked
  WHERE failed_rule_id IS NOT NULL
)
INSERT INTO g3_dev.silver.quarantine_records
SELECT * FROM acc_failed;

INSERT OVERWRITE g3_dev.silver.accounts
WITH acc_checked AS (
  SELECT
    *,
    CASE
      WHEN open_date IS NOT NULL AND open_date != '' AND CAST(open_date AS DATE) > CAST('2026-07-06' AS DATE) THEN 'DQ-ACC-OPENDATE-FUTURE'
      WHEN customer_id NOT IN (SELECT customer_id FROM g3_dev.silver.customers) THEN 'DQ-ACC-CUST-FK'
      ELSE NULL
    END AS failed_rule_id
  FROM g3_dev.bronze.accounts
)
SELECT
  a.account_id,
  a.customer_id,
  a.product_type,
  CAST(a.open_date AS DATE) AS open_date,
  a.status,
  a.currency,
  a._source_file,
  a._source_file_mod_time,
  a._ingest_ts,
  a._run_id,
  a._batch_id,
  a._source_record_id,
  a._record_hash
FROM acc_checked a
LEFT ANTI JOIN g3_dev.silver.quarantine_records q
  ON a._source_record_id = q.source_record_id
 AND q.source_table = 'accounts';

-- ============================================================================
-- 8. PIPELINE FOR CARDS
-- ============================================================================
WITH card_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY card_id ORDER BY _ingest_ts ASC) AS rn_pk
  FROM g3_dev.bronze.cards
),
card_failed AS (
  SELECT
    'RUN-20260706-1' AS run_id,
    'cards' AS source_table,
    _source_record_id,
    card_id AS record_key,
    CASE
      WHEN rn_pk > 1 THEN 'DQ-CARD-DUP'
      WHEN status = 'active' AND to_date(concat(expiry, '-01'), 'yyyy-MM-dd') < to_date('2026-07-01', 'yyyy-MM-dd') THEN 'DQ-CARD-EXPIRED-ACTIVE'
    END AS rule_id,
    CASE
      WHEN rn_pk > 1 THEN 'card_id must be unique'
      WHEN status = 'active' AND to_date(concat(expiry, '-01'), 'yyyy-MM-dd') < to_date('2026-07-01', 'yyyy-MM-dd') THEN 'active card must not have a past expiry'
    END AS rule_name,
    CASE
      WHEN rn_pk > 1 THEN concat('Duplicate card_id found: ', card_id)
      WHEN status = 'active' AND to_date(concat(expiry, '-01'), 'yyyy-MM-dd') < to_date('2026-07-01', 'yyyy-MM-dd') THEN concat('Card active but expired. expiry: ', expiry)
    END AS failure_reason,
    'quarantine' AS severity,
    'quarantined' AS disposition,
    to_json(struct(card_id, account_id, card_type, pan, expiry, status)) AS raw_record,
    current_timestamp() AS detected_at
  FROM card_ranked
  WHERE rn_pk > 1
     OR (status = 'active' AND to_date(concat(expiry, '-01'), 'yyyy-MM-dd') < to_date('2026-07-01', 'yyyy-MM-dd'))
)
INSERT INTO g3_dev.silver.quarantine_records
SELECT * FROM card_failed;

INSERT OVERWRITE g3_dev.silver.cards
WITH card_ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY card_id ORDER BY _ingest_ts ASC) AS rn_pk
  FROM g3_dev.bronze.cards
)
SELECT
  c.card_id,
  c.account_id,
  c.card_type,
  concat('XXXX-XXXX-XXXX-', right(c.pan, 4)) AS pan,
  c.expiry,
  c.status,
  c._source_file,
  c._source_file_mod_time,
  c._ingest_ts,
  c._run_id,
  c._batch_id,
  c._source_record_id,
  c._record_hash
FROM card_ranked c
LEFT ANTI JOIN g3_dev.silver.quarantine_records q
  ON c._source_record_id = q.source_record_id
 AND q.source_table = 'cards';

-- ============================================================================
-- 9. POPULATE MASKING POLICY REGISTRY
-- ============================================================================
INSERT OVERWRITE g3_dev.gov.masking_policies VALUES
  ('customers', 'first_name', 'direct id', 'tokenize (FPE)', 'unprivileged', 'M3'),
  ('customers', 'last_name', 'direct id', 'tokenize (FPE)', 'unprivileged', 'M3'),
  ('customers', 'email', 'contact', 'mask (j***@***.com)', 'unprivileged', 'M3'),
  ('customers', 'phone', 'contact', 'mask (******1234)', 'unprivileged', 'M3'),
  ('customers', 'address', 'sensitive', 'hash (SHA256)', 'unprivileged', 'M3'),
  ('customers', 'dob', 'sensitive', 'generalise (age band)', 'unprivileged', 'M3'),
  ('customers', 'tax_id', 'sensitive', 'hash (SHA256)', 'unprivileged', 'M3'),
  ('cards', 'pan', 'payment', 'mask (XXXX-XXXX-XXXX-1234)', 'unprivileged', 'M3'),
  ('employees', 'full_name', 'staff', 'hash (SHA256)', 'unprivileged', 'M3'),
  ('employees', 'email', 'staff', 'hash (SHA256)', 'unprivileged', 'M3');

-- ============================================================================
-- 10. POPULATE METADATA LINEAGE
-- ============================================================================
INSERT OVERWRITE g3_dev.gov.metadata_lineage VALUES
  ('g3_dev', 'bronze', 'customers', 'customer_id', 'g3_dev', 'silver', 'customers', 'customer_id', 'Direct copy'),
  ('g3_dev', 'bronze', 'customers', 'first_name', 'g3_dev', 'silver', 'customers', 'first_name', 'Tokenized with SHA256 and salt'),
  ('g3_dev', 'bronze', 'customers', 'last_name', 'g3_dev', 'silver', 'customers', 'last_name', 'Tokenized with SHA256 and salt'),
  ('g3_dev', 'bronze', 'customers', 'dob', 'g3_dev', 'silver', 'customers', 'dob', 'Generalized into age bands based on RUN_DATE'),
  ('g3_dev', 'bronze', 'customers', 'email', 'g3_dev', 'silver', 'customers', 'email', 'Masked first character + domain replace'),
  ('g3_dev', 'bronze', 'customers', 'phone', 'g3_dev', 'silver', 'customers', 'phone', 'Masked keeping last 4 digits only'),
  ('g3_dev', 'bronze', 'customers', 'address', 'g3_dev', 'silver', 'customers', 'address', 'Hashed with SHA256 and salt'),
  ('g3_dev', 'bronze', 'customers', 'tax_id', 'g3_dev', 'silver', 'customers', 'tax_id', 'Hashed with SHA256 and salt'),
  ('g3_dev', 'bronze', 'cards', 'pan', 'g3_dev', 'silver', 'cards', 'pan', 'Masked showing last 4 digits only'),
  ('g3_dev', 'bronze', 'employees', 'full_name', 'g3_dev', 'silver', 'employees', 'full_name', 'Hashed with SHA256 and salt'),
  ('g3_dev', 'bronze', 'employees', 'email', 'g3_dev', 'silver', 'employees', 'email', 'Hashed with SHA256 and salt');
