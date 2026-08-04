-- ============================================================================
-- GOVERNANCE PIPELINE: abac_policies
-- ============================================================================
-- Unity Catalog ABAC column mask policies for the G3 governed tag taxonomy.
--
-- Replace ${catalog} with the target catalog before execution, for example:
--   g3_dev
--   g3_test
--   g3_catalog
--
-- Required order:
--   1. Create governed tags with governance/apply_column_tags.py
--      or another account-level governed-tag deployment process.
--   2. Create masking functions with governance/masking_functions.sql.
--   3. Create these ABAC policies.
--   4. Apply/re-apply column tags after table publication with
--      governance/apply_column_tags.py.
--
-- Policy design:
-- - Policies attach at catalog scope and evaluate all descendant tables.
-- - Policies match column-level governed tags only.
-- - Policies match protection_method, not classification, because different
--   restricted columns require different return types and masking behavior.
-- - `no_mask` has no policy. Those tags are audit/discovery tags only.
--
-- Required privileges for the deploying principal:
-- - MANAGE on the target catalog or catalog ownership.
-- - EXECUTE on each masking function referenced below.
--
-- Runtime requirement:
-- - Serverless SQL warehouse, or Databricks Runtime 16.4+ compatible compute.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Person/name display mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY mask_name_columns
ON CATALOG ${catalog}
COMMENT 'Masks name-like direct identifiers tagged protection_method=mask_name.'
COLUMN MASK ${catalog}.governance.mask_name
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'mask_name') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Email display mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY mask_email_columns
ON CATALOG ${catalog}
COMMENT 'Masks email columns tagged protection_method=mask_email.'
COLUMN MASK ${catalog}.governance.mask_email
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'mask_email') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Phone display mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY mask_phone_columns
ON CATALOG ${catalog}
COMMENT 'Masks phone columns tagged protection_method=mask_phone.'
COLUMN MASK ${catalog}.governance.mask_phone
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'mask_phone') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Payment card display mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY mask_pan_columns
ON CATALOG ${catalog}
COMMENT 'Masks payment-card columns tagged protection_method=mask_pan.'
COLUMN MASK ${catalog}.governance.mask_pan
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'mask_pan') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Generic string redaction
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY redact_string_columns
ON CATALOG ${catalog}
COMMENT 'Redacts string columns tagged protection_method=redact_string.'
COLUMN MASK ${catalog}.governance.redact_string
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'redact_string') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- String NULL mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY null_string_columns
ON CATALOG ${catalog}
COMMENT 'Nulls string columns tagged protection_method=null_string.'
COLUMN MASK ${catalog}.governance.null_string
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'null_string') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Date NULL mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY null_date_columns
ON CATALOG ${catalog}
COMMENT 'Nulls DATE columns tagged protection_method=null_date.'
COLUMN MASK ${catalog}.governance.null_date
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'null_date') AS masked_col
ON COLUMN masked_col;

-- ---------------------------------------------------------------------------
-- Network/IP display mask
-- ---------------------------------------------------------------------------

CREATE OR REPLACE POLICY network_redact_columns
ON CATALOG ${catalog}
COMMENT 'Redacts or generalizes network identifiers tagged protection_method=network_redact.'
COLUMN MASK ${catalog}.governance.mask_network_identifier
TO `account users`
EXCEPT `governance_admins`, `pipeline_service_principals`
FOR TABLES
MATCH COLUMNS has_tag_value('protection_method', 'network_redact') AS masked_col
ON COLUMN masked_col;
