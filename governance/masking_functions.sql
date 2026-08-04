-- ============================================================================
-- GOVERNANCE PIPELINE: masking_functions
-- ============================================================================
-- SQL UDFs used by Unity Catalog ABAC column mask policies.
--
-- Replace ${catalog} with the target catalog before execution, for example:
--   g3_dev
--   g3_test
--   g3_catalog
--
-- These functions are display-safety focused. They avoid hash/tokenization as
-- general display masks. The only hash-like output is the non-IPv4 fallback in
-- mask_network_identifier, because there is no safe subnet equivalent for all
-- network identifier formats.
--
-- Expected privileged groups/service principals:
--   governance_admins
--   pipeline_service_principals
--
-- ABAC policy design:
--   Match policies by governed tag protection_method:
--     mask_name
--     mask_email
--     mask_phone
--     mask_pan
--     redact_string
--     null_string
--     null_date
--     network_redact
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ${catalog}.governance;

-- ---------------------------------------------------------------------------
-- Common string display masks
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ${catalog}.governance.mask_name(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    WHEN length(trim(value)) = 0 THEN value
    ELSE concat(substr(trim(value), 1, 1), '***')
  END;

CREATE OR REPLACE FUNCTION ${catalog}.governance.mask_email(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    WHEN length(trim(value)) = 0 THEN value
    WHEN instr(value, '@') > 1 THEN concat(substr(trim(value), 1, 1), '***@***')
    ELSE '***@***'
  END;

CREATE OR REPLACE FUNCTION ${catalog}.governance.mask_phone(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    WHEN length(regexp_replace(value, '[^0-9]', '')) >= 4
      THEN concat('******', right(regexp_replace(value, '[^0-9]', ''), 4))
    ELSE '******'
  END;

CREATE OR REPLACE FUNCTION ${catalog}.governance.mask_pan(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    WHEN length(regexp_replace(value, '[^0-9]', '')) >= 4
      THEN concat('XXXX-XXXX-XXXX-', right(regexp_replace(value, '[^0-9]', ''), 4))
    ELSE 'XXXX-XXXX-XXXX-XXXX'
  END;

CREATE OR REPLACE FUNCTION ${catalog}.governance.redact_string(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    ELSE '***REDACTED***'
  END;

CREATE OR REPLACE FUNCTION ${catalog}.governance.null_string(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    ELSE NULL
  END;

-- ---------------------------------------------------------------------------
-- Date display mask
-- ---------------------------------------------------------------------------
-- Use this on raw DATE columns such as silver.customers.dob.
-- For analytics, expose a derived age_band column separately.

CREATE OR REPLACE FUNCTION ${catalog}.governance.null_date(value DATE)
RETURNS DATE
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    ELSE NULL
  END;

-- ---------------------------------------------------------------------------
-- Network display mask
-- ---------------------------------------------------------------------------
-- IPv4 values are generalized to /24. Other formats are hidden behind a stable
-- non-raw marker. If strict display-only redaction is preferred, replace the
-- ELSE branch with 'NETWORK_REDACTED'.

CREATE OR REPLACE FUNCTION ${catalog}.governance.mask_network_identifier(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('governance_admins') THEN value
    WHEN is_account_group_member('pipeline_service_principals') THEN value
    WHEN value IS NULL THEN NULL
    WHEN value RLIKE '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'
      THEN concat(
        split(value, '\\.')[0], '.',
        split(value, '\\.')[1], '.',
        split(value, '\\.')[2], '.0/24'
      )
    ELSE concat('NETWORK_REDACTED_', substr(sha2(value, 256), 1, 12))
  END;
