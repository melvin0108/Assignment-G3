-- ============================================================================
-- 01_setup.sql
-- Epic 3 · DQ engine scaffolding  (E3-I1 table + E3-I3 table)
-- Creates: g3_catalog.gov (schema)
--          g3_catalog.gov.dq_rules            ← rule registry   (E3-I1)
--          g3_catalog.silver.quarantine_records ← failure sink   (E3-I3)
-- ============================================================================
--
-- ── WHAT THIS IS ────────────────────────────────────────────────────────────
-- The two physical tables the DQ engine needs before any failure query runs:
--   gov.dq_rules              — the rule registry, populated by 02_load_dq_rules.sql
--   silver.quarantine_records — the failure sink, written by 03/04 failure scripts
-- `gov` did not exist yet (pending since E1); it is created here as its first consumer.
--
-- ── PLACEMENT DECISIONS (locked in design review 2026-07-08) ─────────────────
--   * dq_rules            → gov      (governance / metadata — pure catalog of rules)
--   * quarantine_records  → silver   (data product — matches data-model.md §7
--                                     "physically populated in Silver")
--   * defects_manifest    → stays in bronze for now; reconciliation joins across
--                            schemas, and it moves to gov only when convenient.
--
-- ── HOW TO RUN ───────────────────────────────────────────────────────────────
-- SQL editor / notebook attached to a **Serverless or Pro** SQL Warehouse.
-- Full 3-part names throughout, so no default catalog/schema needs to be set.
-- Re-runnable: IF EXISTS clauses make this a safe no-op from a clean state.
-- ============================================================================


-- 1. Create the governance schema (pending since E1; E3 is its first consumer).
CREATE SCHEMA IF NOT EXISTS g3_catalog.gov
  COMMENT 'Governance metadata: DQ rules, results, pipeline runs, lineage, masking/access policies';


-- 2. dq_rules — the rule registry.
--    DROP + CREATE so 02_load_dq_rules.sql always starts from a clean, known
--    state (it is a static, regenerated registry — safe to rebuild wholesale).
DROP TABLE IF EXISTS g3_catalog.gov.dq_rules;

CREATE TABLE g3_catalog.gov.dq_rules (
  rule_id       STRING  COMMENT 'Stable rule identifier, e.g. DQ-TXN-AMT-POS',
  rule_name     STRING  COMMENT 'Human-readable rule description',
  layer         STRING  COMMENT 'Layer the rule evaluates against (bronze)',
  target_table  STRING  COMMENT 'Bronze source table the rule reads',
  target_key    STRING  COMMENT 'Column(s) forming record_key (PK, or a|b for composite)',
  pattern       STRING  COMMENT 'single_row | duplicate | fk_anti_join | text_pii | ai_exclusion',
  severity      STRING  COMMENT 'reject | quarantine | warn (mirrors defects_manifest)',
  expression    STRING  COMMENT 'Human-readable predicate — documentation only, NOT executed',
  enabled       BOOLEAN COMMENT 'Toggle; the failure scripts implement every enabled rule'
) USING DELTA
  COMMENT 'DQ rule registry — one row per executable data-quality rule';


-- 3. quarantine_records — the failure sink (data-model.md §7).
--    CREATE IF NOT EXISTS: history is preserved across runs (keyed by run_id).
--    The failure scripts DELETE the current run_id's rows before re-inserting,
--    so a single run is replaceable while older runs are retained.
CREATE TABLE IF NOT EXISTS g3_catalog.silver.quarantine_records (
  run_id           STRING    COMMENT 'Pipeline run that detected the failure',
  source_table     STRING    COMMENT 'Bronze source table of the failed record',
  source_record_id STRING    COMMENT 'bronze._source_record_id (stable source PK)',
  record_key       STRING    COMMENT 'Natural key matching defects_manifest.record_key',
  rule_id          STRING    COMMENT 'Failing gov.dq_rules.rule_id',
  rule_name        STRING    COMMENT 'Human-readable rule name',
  failure_reason   STRING    COMMENT 'Why it failed (template-filled)',
  severity         STRING    COMMENT 'reject | quarantine | warn',
  disposition      STRING    COMMENT 'rejected | quarantined | masked | allowed_with_warning',
  raw_record       STRING    COMMENT 'JSON snapshot of the raw bronze row (forensics / replay)',
  detected_at      TIMESTAMP COMMENT 'Detection timestamp'
) USING DELTA
  COMMENT 'Failed records — one row per failed record x failed rule (data-model.md §7)';


-- ============================================================================
-- VERIFY — both tables should exist and be empty until the later scripts run.
-- ============================================================================
SELECT 'gov.dq_rules'            AS tbl, COUNT(*) AS row_count FROM g3_catalog.gov.dq_rules
UNION ALL
SELECT 'silver.quarantine_records',        COUNT(*)           FROM g3_catalog.silver.quarantine_records;
-- EXPECTED after this script alone: 0 and 0.
