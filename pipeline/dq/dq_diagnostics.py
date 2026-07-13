# Databricks notebook source
# ============================================================================
# Read-only reconciliation diagnostics (run on Serverless/Pro)
# ----------------------------------------------------------------------------
# PySpark port of pipeline/dq/diagnostics.sql. The SQL is embedded VERBATIM; the
# only runtime change is the __CATALOG__ token replaced by the selected catalog. The SQL
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
-- diagnostics.sql
-- Read-only probes to confirm E3 verify root causes (run on Serverless/Pro).
-- All SELECTs — safe to run anytime against the current quarantine run.
-- ============================================================================

-- (1) DQ-CBK-DISP-FK (caught 0/480) — pin the root cause.
--     chargebacks IS in the bronze ingest, so this should be non-empty.
SELECT
  COUNT(*)                                                   AS cbk_rows,
  SUM(CASE WHEN dispute_id = 'DSP-9999' THEN 1 ELSE 0 END)  AS dsp9999_in_chargebacks
FROM __CATALOG__.bronze.chargebacks;
-- EXPECTED: cbk_rows > 0 AND dsp9999_in_chargebacks = 480.
--   cbk_rows = 0            → chargebacks.csv not uploaded/loaded (re-upload + re-COPY).
--   dsp9999 = 0, rows > 0   → loaded CSV is stale vs the current manifest (regenerate/re-upload).

SELECT COUNT(*) AS dsp9999_in_disputes
FROM __CATALOG__.bronze.disputes
WHERE dispute_id = 'DSP-9999';
-- EXPECTED 0. If >0 the anti-join matched and DSP-9999 unexpectedly resolves.


-- (2) CARD-EXPIRED-ACTIVE & CASE-STALE over-fire — confirm recall-on-logged = 100%:
--     every LOGGED defect key is inside our caught set. Proves the extra rows are
--     real-but-unlogged base-data violations, not a query bug. EXPECTED = 540 and 120.
SELECT 'DQ-CARD-EXPIRED-ACTIVE' AS rule_id, COUNT(*) AS logged_keys_caught
FROM (
  SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest
  WHERE rule_id = 'DQ-CARD-EXPIRED-ACTIVE'
    AND record_key IN (
      SELECT DISTINCT record_key FROM __CATALOG__.silver.quarantine_records
      WHERE rule_id = 'DQ-CARD-EXPIRED-ACTIVE' AND run_id = 'RUN-20260708-DQ1')
) z
UNION ALL
SELECT 'DQ-CASE-STALE', COUNT(*)
FROM (
  SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest
  WHERE rule_id = 'DQ-CASE-STALE'
    AND record_key IN (
      SELECT DISTINCT record_key FROM __CATALOG__.silver.quarantine_records
      WHERE rule_id = 'DQ-CASE-STALE' AND run_id = 'RUN-20260708-DQ1')
) z;


-- (3) CASE-STATUS-ENUM (6 short) & CASEPARTY-RESOLVE (4 short) — confirm they are
--     generator defect-OVERLAP artifacts: the injected violation was OVERWRITTEN
--     by a later defect, so bronze no longer shows it (uncatchable from data).

-- STATUS-ENUM: missed cases whose status_code is no longer 'on_hold'
-- (overwritten to a valid value by the STALE or LEGALHOLD defect).
SELECT m.record_key AS case_id, c.status_code
FROM (SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest WHERE rule_id='DQ-CASE-STATUS-ENUM') m
JOIN __CATALOG__.bronze.investigation_cases c ON m.record_key = c.case_id
WHERE c.status_code IN ('open','in_progress','suspended','closed');
-- EXPECTED ~6 rows whose status_code is NOT 'on_hold' (the violation was masked).

-- CASEPARTY-RESOLVE: missed rows whose party_type is no longer 'customer'
-- (overwritten to 'suspect' by the TYPE-ENUM defect).
SELECT m.record_key, cp.party_type, cp.party_id
FROM (SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest WHERE rule_id='DQ-CASEPARTY-RESOLVE') m
JOIN __CATALOG__.bronze.case_parties cp
  ON cp.case_id    = SPLIT(m.record_key,'\\|')[0]
 AND cp.party_type = SPLIT(m.record_key,'\\|')[1]
WHERE cp.party_type <> 'customer';
-- EXPECTED ~4 rows with party_type = 'suspect'.


-- (4) DQ-NOTE-PII-LEAK (caught 0/750) — the identical regex works for
--     DQ-CTL-NOTE-PII (120/120), so the regex is fine; the data differs.
--     Hypothesis: the PII note_text embeds double-quotes (name "Jane Smith")
--     which break COPY INTO's CSV field parsing, so those rows' note_text is
--     mangled/NULL and the RLIKE matches nothing.

-- (a) Are the PII-leak notes present, and what does note_text actually look like?
SELECT m.record_key AS note_id, n.note_text, LENGTH(n.note_text) AS txt_len
FROM (SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest WHERE rule_id='DQ-NOTE-PII-LEAK' LIMIT 5) m
LEFT JOIN __CATALOG__.bronze.investigation_notes n ON m.record_key = n.note_id;
-- If note_text is NULL / truncated / starts mid-quote → CSV quoting broke the row.

-- (b) How many notes still carry the PII prefix at all?
SELECT
  COUNT(*)                                                              AS total_notes,
  SUM(CASE WHEN note_text IS NULL                THEN 1 ELSE 0 END)     AS null_text,
  SUM(CASE WHEN note_text LIKE 'Spoke to customer%' THEN 1 ELSE 0 END)  AS pii_prefix
FROM __CATALOG__.bronze.investigation_notes;
-- If quoting is fine: pii_prefix ≈ 750. If pii_prefix = 0 → the PII text was mangled away.


-- ============================================================================
-- (5) SCD2 guardrail for the duplicate rules (DQ-CUST-ID-DUP, DQ-CARD-DUP).
--     dq_03 partitions those windows by (key, effective_at), so legitimate SCD2
--     version history (same key, later effective_at) must NOT be quarantined —
--     only the genuine injected dup (same key AND same effective_at) is caught.
--     (5a)/(5b) confirm no SCD2 version leaked into quarantine; (5c) confirms
--     the test is non-trivial by showing multi-version keys actually exist.

-- (5a) DQ-CUST-ID-DUP — caught keys NOT explained by a defect = SCD2 leak.
--      EXPECTED 0 rows. Any row here means an SCD2 customer version slipped
--      through the (customer_id, effective_at) partition; re-check dq_03.
SELECT 'DQ-CUST-ID-DUP' AS rule_id, q.record_key
FROM (
  SELECT DISTINCT record_key FROM __CATALOG__.silver.quarantine_records
  WHERE rule_id = 'DQ-CUST-ID-DUP' AND run_id = 'RUN-20260708-DQ1') q
LEFT JOIN (
  SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest
  WHERE rule_id = 'DQ-CUST-ID-DUP') m ON q.record_key = m.record_key
WHERE m.record_key IS NULL;

-- (5b) DQ-CARD-DUP — same SCD2 leak check. EXPECTED 0 rows.
SELECT 'DQ-CARD-DUP' AS rule_id, q.record_key
FROM (
  SELECT DISTINCT record_key FROM __CATALOG__.silver.quarantine_records
  WHERE rule_id = 'DQ-CARD-DUP' AND run_id = 'RUN-20260708-DQ1') q
LEFT JOIN (
  SELECT DISTINCT record_key FROM __CATALOG__.bronze.defects_manifest
  WHERE rule_id = 'DQ-CARD-DUP') m ON q.record_key = m.record_key
WHERE m.record_key IS NULL;

-- (5c) Confirm SCD2 versions exist in bronze for the two dims (so 5a/5b are
--      meaningful). EXPECTED both > 0 once a T1+ snapshot has been ingested;
--      = 0 means only a flat T0 batch was loaded (the guardrail is then trivial).
SELECT 'customers' AS dim, COUNT(*) AS multi_version_keys
FROM (
  SELECT customer_id FROM __CATALOG__.bronze.customers
  GROUP BY customer_id HAVING COUNT(DISTINCT effective_at) > 1)
UNION ALL
SELECT 'cards', COUNT(*)
FROM (
  SELECT card_id FROM __CATALOG__.bronze.cards
  GROUP BY card_id HAVING COUNT(DISTINCT effective_at) > 1);

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
