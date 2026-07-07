-- ============================================================================
-- 02_ingest_defects_manifest.sql
-- Epic 2 · Item 4 (E2-I4) — land the defects manifest as the DQ validation oracle
-- Target table: g3_catalog.bronze.defects_manifest
-- ============================================================================
--
-- ── WHAT THIS IS ────────────────────────────────────────────────────────────
-- The mock generator emits a "defects manifest" alongside the 25 source CSVs:
-- one row per *intentionally injected* bad record, with columns
--   source_table, record_key, rule_id, rule_name, failure_reason, severity
-- It is the GROUND TRUTH for data quality: later, the DQ engine (E3) writes the
-- records it actually caught into quarantine, and the E7 reconciliation test
-- compares quarantine vs. this manifest (precision/recall per rule). So this
-- table is not business data — it is the oracle that proves the DQ rules work.
--
-- ── WHY IT IS ITS OWN SCRIPT (not in 01_ingest_bronze.sql) ───────────────────
-- 01_ingest_bronze.sql loads the 25 *domain* source tables (customers, cards,
-- transactions, …). The manifest is metadata ABOUT the data, not a source table,
-- so it is deliberately kept out of that script. See the NOTE in
-- 01_ingest_bronze.sql which points here.
--
-- ── SCHEMA DECISION: lean 6-column table, in `bronze` ───────────────────────
-- * Lean (no _source_file / _run_id / _record_hash / _rescued_data …): the
--   manifest is regenerated wholesale on every mock run (deterministic seed)
--   and consumed read-only for reconciliation, so it does NOT need the
--   append-only replay/dedup metadata that the 25 source tables carry.
-- * In `g3_catalog.bronze` for now because the `gov` schema does not exist yet
--   (it is created in E3). The design's eventual home is
--   g3_catalog.gov._defects_manifest_staging; when `gov` lands, this table
--   moves there (a rename) — no logic change.
--
-- ── ⚠️  SOURCE FILENAME MUST NOT START WITH AN UNDERSCORE ────────────────────
-- The source file is `defects_manifest.csv` (NO leading underscore). Databricks'
-- file source treats any file/dir whose name begins with `_` or `.` as HIDDEN
-- and silently filters it out. A `_`-prefixed name makes COPY INTO throw
--   COPY_INTO_SOURCE_SCHEMA_INFERENCE_FAILED: … did not contain any parsable
--   files of type CSV
-- (Do NOT silence it with spark.databricks.delta.copyInto.emptySourceCheck.enabled
-- =false — that just makes COPY INTO load 0 rows and look like success.)
-- The generator was changed to emit `defects_manifest.csv` for this reason;
-- do not re-add the leading underscore.
--
-- ── PREREQUISITES / HOW TO RUN ──────────────────────────────────────────────
-- 1. Source file present in the landing Volume:
--      /Volumes/g3_catalog/bronze/raw_data/defects_manifest.csv
--    (same flat folder as the 25 source CSVs; uploaded from local data/raw/).
-- 2. Run in a Databricks SQL editor/notebook attached to a **Serverless or Pro**
--    SQL Warehouse. COPY INTO is NOT supported on Classic SQL Warehouses.
-- 3. Statements use full 3-part names (g3_catalog.bronze.…), so no default
--    catalog/schema needs to be set — it runs as-is.
-- 4. Run top-to-bottom (DROP → CREATE → COPY → VERIFY).
--
-- ── IDEMPOTENCY / RE-RUN AFTER REGENERATING MOCK DATA ───────────────────────
-- COPY INTO tracks already-consumed files, so a plain re-run of just the
-- COPY block will NOT reload. When mock data is regenerated (same filename,
-- new content), re-run the WHOLE script (DROP + CREATE + COPY) from clean
-- state. The DROP IF EXISTS clauses make this safe from any starting state.
--
-- ── EXPECTED RESULT (default seed 42, full/stress scale) ────────────────────
--   row_count = 1,070,282
--   top rules: DQ-TXN-AMT-POS   = 150000
--              DQ-TXN-TS-FUTURE = 120000
--              DQ-TXN-MERCH-REQ = 120000
--              DQ-TXN-ID-DUP    = 120000
--              DQ-TXN-ACCT-FK   = 120000
--              DQ-AUTH-TXN-FK   = 108000
--              DQ-AUTH-TS-ORDER = 108000
--              DQ-TXN-CARD-ACTIVE = 90000
--   NOTE: these counts are for human eyeballing only. Automated tests (E7) must
--   derive expected sets from the manifest table at runtime — never hardcode
--   counts (they drift if the seed or defect-rate changes).
--
-- ── DOWNSTREAM CONSUMERS ────────────────────────────────────────────────────
--   E3 — DQ rule registry + failure engine (rules must reconcile to this oracle)
--   E7 — manifest-vs-quarantine reconciliation test (precision/recall per rule)
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. CLEAN UP any prior manifest tables so the load starts from a known state.
--    Two earlier ad-hoc attempts existed and were both unusable:
--      bronze._defects_manifest  — created with a _rescued_data col, 0 rows
--      bronze.defects_manifest   — 3.5M rows, but only 1.07M valid + 2.4M all-
--                                  NULL junk from a non-idempotent mis-load.
--    Both are dropped here; IF EXISTS makes this a safe no-op if they're gone.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS g3_catalog.bronze._defects_manifest;
DROP TABLE IF EXISTS g3_catalog.bronze.defects_manifest;


-- ----------------------------------------------------------------------------
-- 2. CREATE the lean oracle table. All columns STRING (typing is not this
--    table's job — it is a reference table, read as-is for reconciliation).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g3_catalog.bronze.defects_manifest (
  source_table   STRING,
  record_key     STRING,
  rule_id        STRING,
  rule_name      STRING,
  failure_reason STRING,
  severity       STRING
) USING DELTA;


-- ----------------------------------------------------------------------------
-- 3. COPY INTO — idempotent single-shot load from the landing Volume.
--    Inner SELECT lists the 6 source columns by name in table-column order so
--    COPY INTO maps them positionally. FORMAT_OPTIONS match the proven pattern
--    used by the 25 source tables in 01_ingest_bronze.sql (header row present,
--    read everything as STRING — no schema inference).
-- ----------------------------------------------------------------------------
COPY INTO g3_catalog.bronze.defects_manifest
FROM (
  SELECT
    source_table, record_key, rule_id, rule_name, failure_reason, severity
  FROM '/Volumes/g3_catalog/bronze/raw_data/defects_manifest.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');


-- ============================================================================
-- VERIFY — run these after the COPY; all three must match expectations.
-- ============================================================================

-- (a) Total rows loaded.  EXPECTED: 1,070,282
SELECT COUNT(*) AS row_count FROM g3_catalog.bronze.defects_manifest;

-- (b) Junk guard: no all-NULL rows.  EXPECTED: 0
--     (the earlier corrupt load had ~2.43M of these; this confirms we did not
--      reintroduce that failure mode.)
SELECT COUNT(*) AS null_rule_rows
FROM g3_catalog.bronze.defects_manifest
WHERE rule_id IS NULL OR rule_id = '';

-- (c) Per-rule spot-check.  EXPECTED top: DQ-TXN-AMT-POS=150000, then four
--     rules at 120000 (DQ-TXN-TS-FUTURE / -MERCH-REQ / -ID-DUP / -ACCT-FK).
SELECT rule_id, COUNT(*) AS n
FROM g3_catalog.bronze.defects_manifest
GROUP BY rule_id
ORDER BY n DESC
LIMIT 10;
