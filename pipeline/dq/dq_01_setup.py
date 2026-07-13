# Databricks notebook source
# ============================================================================
# DQ scaffolding: create gov schema + gov.dq_rules + silver.quarantine_records
# ----------------------------------------------------------------------------
# PySpark port of pipeline/dq/01_setup.sql. The SQL is embedded VERBATIM; the
# only runtime change is replacing the __CATALOG__ token with the selected catalog. The SQL
# is split into statements with a string/comment-aware splitter and each
# statement is run via spark.sql. This supersedes the earlier hand-chunked
# version whose naive ';' split broke on semicolons inside string literals
# ('Toggle; the failure scripts...', 'exact first pass; fuzzy TODO') and left
# comment fragments as raw SQL.
# ============================================================================

from pyspark.sql import SparkSession

# `spark` is pre-initialized in a Databricks notebook.
spark = SparkSession.builder.getOrCreate()

def _catalog_widget():
    """Create the team-standard catalog widget and return its validated value.

    Mirrors pipeline/bronze/autoloader_common.py: idempotent (reuses an existing
    widget if a parent notebook or job parameter already set one) and validated
    against the team's dev/test/prod catalogs (g3_dev / g3_test / g3_catalog).
    """
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


catalog = _catalog_widget()

def _has_code(stmt):
    """True if the chunk has any SQL after stripping -- line comments, so a
    comment-only fragment is never sent to spark.sql()."""
    for line in stmt.split("\n"):
        if line.split("--", 1)[0].strip():
            return True
    return False


def _statements(sql):
    """Split a SQL script into individual statements, respecting single-quoted
    string literals (with '' escaping) and -- line comments. A ';' inside a
    string (e.g. 'Toggle; the failure scripts...', 'exact first pass; fuzzy
    TODO') or a comment is NOT treated as a statement boundary."""
    out, buf = [], []
    i, n = 0, len(sql)
    in_str = in_cmt = False
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if in_cmt:
            buf.append(c)
            if c == "\n":
                in_cmt = False
            i += 1
            continue
        if in_str:
            buf.append(c)
            if c == "'":
                if nxt == "'":              # escaped '' -> stay in string
                    buf.append("'")
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if c == "-" and nxt == "-":
            in_cmt = True
            buf.append(c)
            i += 1
            continue
        if c == "'":
            in_str = True
            buf.append(c)
            i += 1
            continue
        if c == ";":
            stmt = "".join(buf).strip()
            if _has_code(stmt):
                out.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    stmt = "".join(buf).strip()
    if _has_code(stmt):
        out.append(stmt)
    return out


def _is_query(stmt):
    """True if the statement returns a result set worth showing (SELECT/WITH/
    SHOW/DESCRIBE/EXPLAIN). DDL/DML are side-effecting."""
    for line in stmt.split("\n"):
        code = line.split("--", 1)[0].strip()
        if code:
            return code.split()[0].upper() in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}
    return False


def _head(stmt):
    """First non-comment code line, trimmed — used for the run log."""
    for line in stmt.split("\n"):
        code = line.split("--", 1)[0].strip()
        if code:
            return code[:90]
    return ""


SQL = r"""
-- ============================================================================
-- 01_setup.sql
-- Epic 3 · DQ engine scaffolding  (E3-I1 table + E3-I3 table)
-- Creates: __CATALOG__.gov (schema)
--          __CATALOG__.gov.dq_rules            ← rule registry   (E3-I1)
--          __CATALOG__.silver.quarantine_records ← failure sink   (E3-I3)
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
CREATE SCHEMA IF NOT EXISTS __CATALOG__.gov
  COMMENT 'Governance metadata: DQ rules, results, pipeline runs, lineage, masking/access policies';


-- 2. dq_rules — the rule registry.
--    DROP + CREATE so 02_load_dq_rules.sql always starts from a clean, known
--    state (it is a static, regenerated registry — safe to rebuild wholesale).
DROP TABLE IF EXISTS __CATALOG__.gov.dq_rules;

CREATE TABLE __CATALOG__.gov.dq_rules (
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
CREATE TABLE IF NOT EXISTS __CATALOG__.silver.quarantine_records (
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
SELECT 'gov.dq_rules'            AS tbl, COUNT(*) AS row_count FROM __CATALOG__.gov.dq_rules
UNION ALL
SELECT 'silver.quarantine_records',        COUNT(*)           FROM __CATALOG__.silver.quarantine_records;
-- EXPECTED after this script alone: 0 and 0.

"""


def _run(stmt):
    """Execute one statement with the __CATALOG__ token replaced by the selected catalog."""
    return spark.sql(stmt.replace("__CATALOG__", catalog))


for _stmt in _statements(SQL):
    _df = _run(_stmt)
    if _is_query(_stmt):
        _df.show(truncate=False)
    else:
        _df.collect()                       # force eager DDL/DML execution
        print("  ran: " + _head(_stmt))
