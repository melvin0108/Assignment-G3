-- ============================================================================
-- reload_chargebacks.sql
-- One-off: reload bronze.chargebacks after the generator sentinel fix
-- (DSP-9999 -> ORPHAN_DISPUTE_ID = DSP-999999).
--
-- RUN ORDER:
--   1. Generator already edited (config.py ORPHAN_DISPUTE_ID + gen_chargebacks).
--   2. Regenerate:  python -m mock.generate   (same seed/scale as before)
--      -> only chargebacks.csv changes (defects_manifest.csv is byte-identical:
--         CBK manifest keys are chargeback_ids, and the RNG path is unchanged).
--   3. Re-upload chargebacks.csv to /Volumes/g3_catalog/bronze/raw_data/chargebacks.csv
--   4. Run THIS script (DROP+CREATE forces COPY INTO to reload the same filename;
--      COPY INTO otherwise remembers the file as already consumed).
--   5. Re-run pipeline/dq/04_failures_all_rules.sql  -> DQ-CBK-DISP-FK now 480/480.
-- ============================================================================

DROP TABLE IF EXISTS g3_catalog.bronze.chargebacks;

CREATE TABLE g3_catalog.bronze.chargebacks (
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
    _metadata.file_name                AS _source_file,
    _metadata.file_modification_time   AS _source_file_mod_time,
    current_timestamp()                AS _ingest_ts,
    'RUN-20260706-1'                   AS _run_id,
    CAST(1 AS BIGINT)                  AS _batch_id,
    chargeback_id                      AS _source_record_id,
    sha2(concat_ws('|', chargeback_id, dispute_id, scheme, amount, stage, processed_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/chargebacks.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');


-- VERIFY — the new sentinel must NOT resolve, and the old one must be gone.
SELECT
  COUNT(*)                                                        AS cbk_rows,
  SUM(CASE WHEN dispute_id = 'DSP-999999' THEN 1 ELSE 0 END)      AS orphan_rows,
  SUM(CASE WHEN dispute_id = 'DSP-9999'  THEN 1 ELSE 0 END)      AS old_sentinel_rows
FROM g3_catalog.bronze.chargebacks;
-- EXPECTED: cbk_rows = 8000, orphan_rows = 480, old_sentinel_rows = 0.
