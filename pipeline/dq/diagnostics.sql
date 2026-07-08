-- ============================================================================
-- diagnostics.sql
-- Read-only probes to confirm E3 verify root causes (run on Serverless/Pro).
-- All SELECTs — safe to run anytime against the current quarantine run.
-- ============================================================================

-- (1) DQ-CBK-DISP-FK (caught 0/480) — pin the root cause.
--     chargebacks IS in the bronze ingest, so this should be non-empty.
SELECT
  COUNT(*)                                                   AS cbk_rows,
  SUM(CASE WHEN dispute_id = 'DSP-9999' THEN 1 ELSE 0 END)  AS dsp9999_in_chargebacks
FROM g3_catalog.bronze.chargebacks;
-- EXPECTED: cbk_rows > 0 AND dsp9999_in_chargebacks = 480.
--   cbk_rows = 0            → chargebacks.csv not uploaded/loaded (re-upload + re-COPY).
--   dsp9999 = 0, rows > 0   → loaded CSV is stale vs the current manifest (regenerate/re-upload).

SELECT COUNT(*) AS dsp9999_in_disputes
FROM g3_catalog.bronze.disputes
WHERE dispute_id = 'DSP-9999';
-- EXPECTED 0. If >0 the anti-join matched and DSP-9999 unexpectedly resolves.


-- (2) CARD-EXPIRED-ACTIVE & CASE-STALE over-fire — confirm recall-on-logged = 100%:
--     every LOGGED defect key is inside our caught set. Proves the extra rows are
--     real-but-unlogged base-data violations, not a query bug. EXPECTED = 540 and 120.
SELECT 'DQ-CARD-EXPIRED-ACTIVE' AS rule_id, COUNT(*) AS logged_keys_caught
FROM (
  SELECT DISTINCT record_key FROM g3_catalog.bronze.defects_manifest
  WHERE rule_id = 'DQ-CARD-EXPIRED-ACTIVE'
    AND record_key IN (
      SELECT DISTINCT record_key FROM g3_catalog.silver.quarantine_records
      WHERE rule_id = 'DQ-CARD-EXPIRED-ACTIVE' AND run_id = 'RUN-20260708-DQ1')
) z
UNION ALL
SELECT 'DQ-CASE-STALE', COUNT(*)
FROM (
  SELECT DISTINCT record_key FROM g3_catalog.bronze.defects_manifest
  WHERE rule_id = 'DQ-CASE-STALE'
    AND record_key IN (
      SELECT DISTINCT record_key FROM g3_catalog.silver.quarantine_records
      WHERE rule_id = 'DQ-CASE-STALE' AND run_id = 'RUN-20260708-DQ1')
) z;


-- (3) CASE-STATUS-ENUM (6 short) & CASEPARTY-RESOLVE (4 short) — confirm they are
--     generator defect-OVERLAP artifacts: the injected violation was OVERWRITTEN
--     by a later defect, so bronze no longer shows it (uncatchable from data).

-- STATUS-ENUM: missed cases whose status_code is no longer 'on_hold'
-- (overwritten to a valid value by the STALE or LEGALHOLD defect).
SELECT m.record_key AS case_id, c.status_code
FROM (SELECT DISTINCT record_key FROM g3_catalog.bronze.defects_manifest WHERE rule_id='DQ-CASE-STATUS-ENUM') m
JOIN g3_catalog.bronze.investigation_cases c ON m.record_key = c.case_id
WHERE c.status_code IN ('open','in_progress','suspended','closed');
-- EXPECTED ~6 rows whose status_code is NOT 'on_hold' (the violation was masked).

-- CASEPARTY-RESOLVE: missed rows whose party_type is no longer 'customer'
-- (overwritten to 'suspect' by the TYPE-ENUM defect).
SELECT m.record_key, cp.party_type, cp.party_id
FROM (SELECT DISTINCT record_key FROM g3_catalog.bronze.defects_manifest WHERE rule_id='DQ-CASEPARTY-RESOLVE') m
JOIN g3_catalog.bronze.case_parties cp
  ON cp.case_id    = SPLIT(m.record_key,'\\|')[0]
 AND cp.party_type = SPLIT(m.record_key,'\\|')[1]
WHERE cp.party_type <> 'customer';
-- EXPECTED ~4 rows with party_type = 'suspect'.
