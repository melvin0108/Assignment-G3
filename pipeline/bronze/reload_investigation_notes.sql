-- ============================================================================
-- reload_investigation_notes.sql
-- One-off: reload bronze.investigation_notes after the generator PII-note fix
-- (the cosmetic double-quotes around the customer name were removed, so COPY
-- INTO no longer mangles the quoted CSV field).  embedded " in a quoted CSV
-- field is read by Spark as end-of-field+start-of-field, not as an escaped
-- quote — so the PII note_text was truncated to ~41 chars and the email/phone/
-- PAN spilled into later columns. Single quotes avoid it.
--
-- RUN ORDER:
--   1. Generator already edited (generators.py: f"name '{...}'").
--   2. Regenerate:  python -m mock.generate   (same seed/scale)
--      -> only investigation_notes.csv changes (manifest byte-identical).
--   3. Re-upload investigation_notes.csv to /Volumes/g3_catalog/bronze/raw_data/
--   4. Run THIS script (DROP+CREATE forces COPY INTO to reload the filename).
--   5. Re-run pipeline/dq/04_failures_all_rules.sql -> DQ-NOTE-PII-LEAK now 750/750.
-- ============================================================================

DROP TABLE IF EXISTS g3_catalog.bronze.investigation_notes;

CREATE TABLE g3_catalog.bronze.investigation_notes (
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
    _metadata.file_name                AS _source_file,
    _metadata.file_modification_time   AS _source_file_mod_time,
    current_timestamp()                AS _ingest_ts,
    'RUN-20260706-1'                   AS _run_id,
    CAST(1 AS BIGINT)                  AS _batch_id,
    note_id                            AS _source_record_id,
    sha2(concat_ws('|', note_id, case_id, author_employee_id, note_text, created_at), 256) AS _record_hash
  FROM '/Volumes/g3_catalog/bronze/raw_data/investigation_notes.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');


-- VERIFY — the PII-leak notes must now carry their full text.
SELECT
  COUNT(*)                                                              AS total_notes,
  SUM(CASE WHEN note_text LIKE 'Spoke to customer%' THEN 1 ELSE 0 END)  AS pii_prefix,
  SUM(CASE WHEN note_text RLIKE '\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b' THEN 1 ELSE 0 END) AS notes_with_pan
FROM g3_catalog.bronze.investigation_notes;
-- EXPECTED: total_notes = 10000, pii_prefix ≈ 750, notes_with_pan ≈ 750.
