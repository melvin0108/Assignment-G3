-- ============================================================================
-- DEFECTS MANIFEST INGEST  —  g3_catalog.bronze.defects_manifest
-- ----------------------------------------------------------------------------
-- Loads defects_manifest.csv (the DQ oracle / validation ground truth) from
-- the landing Volume into a lean reference table for later reconciliation
-- (E3 DQ engine + E7 manifest-vs-quarantine tests).
--
-- This is kept OUT of 01_ingest_bronze.sql on purpose: the manifest is NOT a
-- source/domain table — it is metadata ABOUT the data (the list of every
-- intentionally injected bad record). It currently lives in `bronze` because
-- the `gov` schema does not exist yet (created in E3); it will move to
-- g3_catalog.gov._defects_manifest_staging when `gov` lands.
--
-- WHY LEAN (no 8 metadata columns): the manifest is regenerated wholesale on
-- each mock run (deterministic seed) and consumed read-only for
-- reconciliation, so it does not need _run_id/_batch_id/_record_hash for
-- replay/dedup like the append-only source tables do.
--
-- HOW TO RUN
--   1. Open a Databricks SQL notebook (language = SQL) on a SQL Warehouse
--      (Serverless or Pro) — COPY INTO is not supported on Classic SQL Warehouses.
--   2. Run top-to-bottom. NOTE on idempotency: a plain re-run of COPY INTO
--      alone will NOT reload (it tracks the file as already consumed). After
--      regenerating mock data (new content, same filename), re-run the WHOLE
--      script (DROP + CREATE + COPY) from clean state.
--
-- EXPECTED RESULT (default seed 42, stress scale): 1,070,282 rows.
--   Top rules: DQ-TXN-AMT-POS=150000, DQ-TXN-TS-FUTURE=120000,
--              DQ-TXN-MERCH-REQ=120000, DQ-TXN-ID-DUP=120000, DQ-TXN-ACCT-FK=120000.
-- ============================================================================

-- Clean up the two earlier ad-hoc/broken attempts (one was empty, one corrupt).
DROP TABLE IF EXISTS g3_catalog.bronze._defects_manifest;
DROP TABLE IF EXISTS g3_catalog.bronze.defects_manifest;

CREATE TABLE IF NOT EXISTS g3_catalog.bronze.defects_manifest (
  source_table   STRING,
  record_key     STRING,
  rule_id        STRING,
  rule_name      STRING,
  failure_reason STRING,
  severity       STRING
) USING DELTA;

COPY INTO g3_catalog.bronze.defects_manifest
FROM (
  SELECT
    source_table, record_key, rule_id, rule_name, failure_reason, severity
  FROM '/Volumes/g3_catalog/bronze/raw_data/defects_manifest.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- ----------------------------------------------------------------------------
-- VERIFY  — row count (expect 1,070,282) + per-rule spot-check
-- ----------------------------------------------------------------------------
SELECT COUNT(*) AS row_count FROM g3_catalog.bronze.defects_manifest;

SELECT rule_id, COUNT(*) AS n
FROM g3_catalog.bronze.defects_manifest
GROUP BY rule_id
ORDER BY n DESC
LIMIT 10;
