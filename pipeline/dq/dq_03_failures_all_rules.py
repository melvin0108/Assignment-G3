# Databricks notebook source
# ============================================================================
# DQ failures — ALL 35 rules -> silver.quarantine_records
# ----------------------------------------------------------------------------
# PySpark port of pipeline/dq/04_failures_all_rules.sql. The SQL is embedded VERBATIM; the
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
-- 04_failures_all_rules.sql
-- Epic 3 · E3-I5 — executable failure queries for ALL 35 injected DQ rules
-- ============================================================================
--
-- ── WHAT THIS IS ────────────────────────────────────────────────────────────
-- One INSERT...SELECT block per rule (35 total), grouped by pattern. Each reads
-- bronze, applies the rule's predicate, and writes failing rows into
-- silver.quarantine_records with a record_key that matches bronze.defects_manifest,
-- so E3-I6 reconciliation is a straight (rule_id, record_key) join.
--
-- 35 rules (from mock/generators.py = the manifest's distinct rule_ids):
--   single_row (13) · duplicate (6) · fk_anti_join (12) · text_pii (2) · ai_exclusion (2)
--
-- This script SUPERSEDES 03_failures_first_slice.sql once the slice is verified.
--
-- ── run_id ──────────────────────────────────────────────────────────────────
-- Inlined literal 'RUN-20260708-DQ1' (same convention as 01_ingest_bronze.sql —
-- no SQL variables/widgets). Find-replace across the file to use a new run_id.
--
-- ── IDEMPOTENCY ──────────────────────────────────────────────────────────────
-- DELETE of this run_id's rows first → re-running replaces this run cleanly.
--
-- ── PREREQUISITES ────────────────────────────────────────────────────────────
-- 01_setup.sql + 02_load_dq_rules.sql already run; bronze source tables loaded.
--
-- ── TIME-BASED RULES use a pinned reference date ─────────────────────────────
-- mock/config.py pins RUN_DATE = 2026-07-06; the generator measures future/stale
-- defects against it. These queries use the SAME date literal so reconciliation
-- matches the manifest exactly. Update the literal if config.RUN_DATE changes.
--
-- ── DUPLICATE RULES flag row_number() > 1 (the injected copy, not the original)
--    to match the generator's manifest-logging convention. ⚠️ NEAR-DUP rules
--    (CUST-NEAR-DUP, EMP-NAME-NEAR-DUP) use exact grouping as a first pass and
--    are flagged fuzzy-TODO; see README_dq.md for the reconciliation caveat.
-- ============================================================================


-- 0. Reset this run's quarantine rows so the script is re-runnable.
DELETE FROM __CATALOG__.silver.quarantine_records WHERE run_id = 'RUN-20260708-DQ1';


-- ============================================================================
--  SINGLE_ROW (13)
-- ============================================================================

-- DQ-TXN-AMT-POS — amount must be > 0
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',_source_record_id,transaction_id,
       'DQ-TXN-AMT-POS','amount must be > 0','negative amount','quarantine','quarantined',
       to_json(named_struct('transaction_id',transaction_id,'amount',amount,'txn_ts',txn_ts,'status',status)), current_timestamp()
FROM __CATALOG__.bronze.transactions
WHERE try_cast(amount AS DOUBLE) <= 0;

-- DQ-TXN-MERCH-REQ — merchant_id is required
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',_source_record_id,transaction_id,
       'DQ-TXN-MERCH-REQ','merchant_id is required','missing merchant_id','quarantine','quarantined',
       to_json(named_struct('transaction_id',transaction_id,'merchant_id',merchant_id,'amount',amount)), current_timestamp()
FROM __CATALOG__.bronze.transactions
WHERE merchant_id IS NULL OR merchant_id = '';

-- DQ-TXN-TS-FUTURE — txn_ts must not be in the future
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',_source_record_id,transaction_id,
       'DQ-TXN-TS-FUTURE','txn_ts must not be in the future','future timestamp','quarantine','quarantined',
       to_json(named_struct('transaction_id',transaction_id,'txn_ts',txn_ts)), current_timestamp()
FROM __CATALOG__.bronze.transactions
WHERE try_to_timestamp(replace(replace(txn_ts,'T',' '),'Z','')) > TIMESTAMP '2026-07-06 23:59:59';

-- DQ-ACC-OPENDATE-FUTURE — open_date must not be in the future
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','accounts',_source_record_id,account_id,
       'DQ-ACC-OPENDATE-FUTURE','open_date must not be in the future','future open_date','quarantine','quarantined',
       to_json(named_struct('account_id',account_id,'open_date',open_date,'status',status)), current_timestamp()
FROM __CATALOG__.bronze.accounts
WHERE try_to_date(open_date) > DATE '2026-07-06';

-- DQ-CUST-EMAIL-FMT — email must match pattern if present (catches empty/malformed)
-- NOTE: COPY INTO stores the generator's empty email ("") as NULL, so the
-- predicate must INCLUDE NULL (the original `email IS NOT NULL` guard excluded
-- exactly the 375 injected defect rows → caught 0). All non-defect customers
-- carry a valid email, so catching NULL/invalid does not over-fire.
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','customers',_source_record_id,customer_id,
       'DQ-CUST-EMAIL-FMT','email must match pattern if present','email is empty','quarantine','quarantined',
       to_json(named_struct('customer_id',customer_id,'email',email)), current_timestamp()
FROM __CATALOG__.bronze.customers
WHERE email IS NULL
   OR email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';

-- DQ-CARD-EXPIRED-ACTIVE — active card must not have a past expiry
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','cards',_source_record_id,card_id,
       'DQ-CARD-EXPIRED-ACTIVE','active card must not have a past expiry','expired-but-active','quarantine','quarantined',
       to_json(named_struct('card_id',card_id,'expiry',expiry,'status',status)), current_timestamp()
FROM __CATALOG__.bronze.cards
WHERE status = 'active'
  AND try_to_date(concat(expiry,'-01')) < DATE '2026-07-06';

-- DQ-MERCH-RISK-CASING — risk_rating must be in {low,medium,high}
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','merchants',_source_record_id,merchant_id,
       'DQ-MERCH-RISK-CASING','risk_rating must be in {low,medium,high}','inconsistent casing','quarantine','quarantined',
       to_json(named_struct('merchant_id',merchant_id,'risk_rating',risk_rating,'status',status)), current_timestamp()
FROM __CATALOG__.bronze.merchants
WHERE risk_rating NOT IN ('low','medium','high');

-- DQ-DISP-STATUS-ENUM — status must be a lowercase dispute enum
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','disputes',_source_record_id,dispute_id,
       'DQ-DISP-STATUS-ENUM','status must be a lowercase dispute enum','status casing/unknown','quarantine','quarantined',
       to_json(named_struct('dispute_id',dispute_id,'status',status,'reason_code',reason_code)), current_timestamp()
FROM __CATALOG__.bronze.disputes
WHERE status NOT IN ('open','in_review','resolved','rejected','withdrawn');

-- DQ-DISP-REASON-REQ — reason_code is required
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','disputes',_source_record_id,dispute_id,
       'DQ-DISP-REASON-REQ','reason_code is required','missing reason_code','quarantine','quarantined',
       to_json(named_struct('dispute_id',dispute_id,'reason_code',reason_code,'status',status)), current_timestamp()
FROM __CATALOG__.bronze.disputes
WHERE reason_code IS NULL OR reason_code = '';

-- DQ-ALT-SCORE-RANGE — score must be within [0,1]
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','fraud_alerts',_source_record_id,alert_id,
       'DQ-ALT-SCORE-RANGE','score must be within [0,1]','score out of range','quarantine','quarantined',
       to_json(named_struct('alert_id',alert_id,'transaction_id',transaction_id,'score',score)), current_timestamp()
FROM __CATALOG__.bronze.fraud_alerts
WHERE try_cast(score AS DOUBLE) < 0 OR try_cast(score AS DOUBLE) > 1;

-- DQ-DEV-TYPE-REQ — device_type is required
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transaction_devices',_source_record_id,device_id,
       'DQ-DEV-TYPE-REQ','device_type is required','missing device_type','quarantine','quarantined',
       to_json(named_struct('device_id',device_id,'transaction_id',transaction_id,'device_type',device_type)), current_timestamp()
FROM __CATALOG__.bronze.transaction_devices
WHERE device_type IS NULL OR device_type = '';

-- DQ-CASE-STATUS-ENUM — status_code must be in case_status enum
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','investigation_cases',_source_record_id,case_id,
       'DQ-CASE-STATUS-ENUM','status_code must be in case_status enum','status not in enum','quarantine','quarantined',
       to_json(named_struct('case_id',case_id,'status_code',status_code,'fraud_type_code',fraud_type_code)), current_timestamp()
FROM __CATALOG__.bronze.investigation_cases
WHERE status_code NOT IN ('open','in_progress','suspended','closed');

-- DQ-CASE-STALE — open cases older than 180 days are stale
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','investigation_cases',_source_record_id,case_id,
       'DQ-CASE-STALE','open cases older than 180 days are stale','stale open case','quarantine','quarantined',
       to_json(named_struct('case_id',case_id,'status_code',status_code,'opened_at',opened_at)), current_timestamp()
FROM __CATALOG__.bronze.investigation_cases
WHERE status_code = 'open'
  AND try_to_date(substring(opened_at,1,10)) < date_sub(DATE '2026-07-06', 180);


-- ============================================================================
--  DUPLICATE (6) — flag the injected copy via row_number() > 1
-- ============================================================================

-- DQ-CUST-ID-DUP — (customer_id, effective_at) must be unique
-- NOTE: partition by (customer_id, effective_at) so SCD2 version history (same
-- customer_id, later effective_at) is NOT flagged as a dup. The injected defect
-- is a verbatim copy (same customer_id AND same effective_at) -> still rn > 1.
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','customers',_source_record_id,customer_id,
       'DQ-CUST-ID-DUP','(customer_id, effective_at) must be unique','exact duplicate (customer_id, effective_at)','quarantine','quarantined',
       to_json(named_struct('customer_id',customer_id,'first_name',first_name,'last_name',last_name,'dob',dob)), current_timestamp()
FROM (
  SELECT customer_id, _source_record_id, first_name, last_name, dob,
         row_number() OVER (PARTITION BY customer_id, effective_at ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.customers)
WHERE rn > 1;

-- DQ-TXN-ID-DUP — transaction_id must be unique
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',_source_record_id,transaction_id,
       'DQ-TXN-ID-DUP','transaction_id must be unique','duplicate transaction_id','quarantine','quarantined',
       to_json(named_struct('transaction_id',transaction_id,'amount',amount,'txn_ts',txn_ts,'status',status)), current_timestamp()
FROM (
  SELECT transaction_id, _source_record_id, amount, txn_ts, status,
         row_number() OVER (PARTITION BY transaction_id ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.transactions)
WHERE rn > 1;

-- DQ-CARD-DUP — (card_id, effective_at) must be unique
-- NOTE: partition by (card_id, effective_at) so SCD2 version history (same
-- card_id, later effective_at) is NOT flagged as a dup. The injected defect is
-- a verbatim copy (same card_id AND same effective_at) -> still rn > 1.
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','cards',_source_record_id,card_id,
       'DQ-CARD-DUP','(card_id, effective_at) must be unique','exact duplicate (card_id, effective_at)','quarantine','quarantined',
       to_json(named_struct('card_id',card_id,'account_id',account_id,'expiry',expiry,'status',status)), current_timestamp()
FROM (
  SELECT card_id, _source_record_id, account_id, expiry, status,
         row_number() OVER (PARTITION BY card_id, effective_at ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.cards)
WHERE rn > 1;

-- DQ-EMP-EMAIL-UNIQ — email must be unique
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','employees',_source_record_id,employee_id,
       'DQ-EMP-EMAIL-UNIQ','email must be unique','duplicate email','quarantine','quarantined',
       to_json(named_struct('employee_id',employee_id,'full_name',full_name,'email',email)), current_timestamp()
FROM (
  SELECT employee_id, _source_record_id, full_name, email,
         row_number() OVER (PARTITION BY email ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.employees)
WHERE rn > 1;

-- DQ-CUST-NEAR-DUP — no two customers share name+dob+address+tax_id  ⚠️ exact first pass
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','customers',_source_record_id,customer_id,
       'DQ-CUST-NEAR-DUP','no two customers share name+dob+address+tax_id','near-duplicate customer','quarantine','quarantined',
       to_json(named_struct('customer_id',customer_id,'first_name',first_name,'last_name',last_name,'dob',dob,'address',address,'tax_id',tax_id)), current_timestamp()
FROM (
  SELECT customer_id, _source_record_id, first_name, last_name, dob, address, tax_id,
         row_number() OVER (PARTITION BY first_name,last_name,dob,address,tax_id ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.customers)
WHERE rn > 1;

-- DQ-EMP-NAME-NEAR-DUP — flag near-duplicate employee names  ⚠️ exact first pass
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','employees',_source_record_id,employee_id,
       'DQ-EMP-NAME-NEAR-DUP','flag near-duplicate employee names','near-duplicate employee name','quarantine','quarantined',
       to_json(named_struct('employee_id',employee_id,'full_name',full_name,'email',email)), current_timestamp()
FROM (
  SELECT employee_id, _source_record_id, full_name, email,
         row_number() OVER (PARTITION BY full_name ORDER BY _source_record_id) AS rn
  FROM __CATALOG__.bronze.employees)
WHERE rn > 1;


-- ============================================================================
--  FK_ANTI_JOIN / RELATIONSHIP (12)
-- ============================================================================

-- DQ-ACC-CUST-FK — customer_id must exist in customers
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','accounts',a._source_record_id,a.account_id,
       'DQ-ACC-CUST-FK','customer_id must exist in customers','orphan customer_id','quarantine','quarantined',
       to_json(named_struct('account_id',a.account_id,'customer_id',a.customer_id,'open_date',a.open_date,'status',a.status)), current_timestamp()
FROM __CATALOG__.bronze.accounts a
LEFT JOIN __CATALOG__.bronze.customers c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- DQ-TXN-ACCT-FK — account_id must exist in accounts
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',t._source_record_id,t.transaction_id,
       'DQ-TXN-ACCT-FK','account_id must exist in accounts','orphan account+card','quarantine','quarantined',
       to_json(named_struct('transaction_id',t.transaction_id,'account_id',t.account_id,'card_id',t.card_id,'amount',t.amount)), current_timestamp()
FROM __CATALOG__.bronze.transactions t
LEFT JOIN __CATALOG__.bronze.accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL;

-- DQ-AUTH-TXN-FK — transaction_id must exist in transactions
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','auth_attempts',a._source_record_id,a.attempt_id,
       'DQ-AUTH-TXN-FK','transaction_id must exist in transactions','orphan transaction_id','quarantine','quarantined',
       to_json(named_struct('attempt_id',a.attempt_id,'transaction_id',a.transaction_id,'auth_ts',a.auth_ts)), current_timestamp()
FROM __CATALOG__.bronze.auth_attempts a
LEFT JOIN __CATALOG__.bronze.transactions t ON a.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- DQ-DISP-TXN-FK — transaction_id must exist in transactions
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','disputes',d._source_record_id,d.dispute_id,
       'DQ-DISP-TXN-FK','transaction_id must exist in transactions','orphan transaction_id','quarantine','quarantined',
       to_json(named_struct('dispute_id',d.dispute_id,'transaction_id',d.transaction_id,'status',d.status)), current_timestamp()
FROM __CATALOG__.bronze.disputes d
LEFT JOIN __CATALOG__.bronze.transactions t ON d.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- DQ-CBK-DISP-FK — dispute_id must exist in disputes
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','chargebacks',cb._source_record_id,cb.chargeback_id,
       'DQ-CBK-DISP-FK','dispute_id must exist in disputes','orphan dispute_id','quarantine','quarantined',
       to_json(named_struct('chargeback_id',cb.chargeback_id,'dispute_id',cb.dispute_id,'stage',cb.stage)), current_timestamp()
FROM __CATALOG__.bronze.chargebacks cb
LEFT JOIN __CATALOG__.bronze.disputes d ON cb.dispute_id = d.dispute_id
WHERE d.dispute_id IS NULL;

-- DQ-DEV-TXN-FK — transaction_id must exist in transactions
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transaction_devices',x._source_record_id,x.device_id,
       'DQ-DEV-TXN-FK','transaction_id must exist in transactions','orphan transaction_id','quarantine','quarantined',
       to_json(named_struct('device_id',x.device_id,'transaction_id',x.transaction_id,'device_type',x.device_type)), current_timestamp()
FROM __CATALOG__.bronze.transaction_devices x
LEFT JOIN __CATALOG__.bronze.transactions t ON x.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- DQ-CASETXN-TXN-FK — transaction_id must exist in transactions  (composite record_key)
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','case_transactions',ct._source_record_id,concat_ws('|',ct.case_id,ct.transaction_id),
       'DQ-CASETXN-TXN-FK','transaction_id must exist in transactions','orphan transaction_id','quarantine','quarantined',
       to_json(named_struct('case_id',ct.case_id,'transaction_id',ct.transaction_id)), current_timestamp()
FROM __CATALOG__.bronze.case_transactions ct
LEFT JOIN __CATALOG__.bronze.transactions t ON ct.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- DQ-TXN-CARD-ACTIVE — transaction must use an active card
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','transactions',t._source_record_id,t.transaction_id,
       'DQ-TXN-CARD-ACTIVE','transaction must use an active card','uses closed card','quarantine','quarantined',
       to_json(named_struct('transaction_id',t.transaction_id,'card_id',t.card_id,'amount',t.amount,'card_status',c.status)), current_timestamp()
FROM __CATALOG__.bronze.transactions t
JOIN __CATALOG__.bronze.cards c ON t.card_id = c.card_id
WHERE c.status = 'closed';

-- DQ-AUTH-TS-ORDER — auth_ts must not be later than txn_ts  (cross-table)
-- NOTE: LEFT JOIN (not inner) so auths whose transaction_id is an orphan
-- (TXN-999999, also flagged DQ-AUTH-TXN-FK) are NOT dropped — they are in the
-- AUTH-TS-ORDER manifest because their auth_ts is in the future. COALESCE falls
-- back to RUN_DATE midnight for orphan auths. This is clean because the
-- generator's future_ts is always >= now+1day while past_ts is always <= now
-- (run_now() = 2026-07-06 00:00:00), so no false positives.
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','auth_attempts',a._source_record_id,a.attempt_id,
       'DQ-AUTH-TS-ORDER','auth_ts must not be later than txn_ts','auth after transaction','quarantine','quarantined',
       to_json(named_struct('attempt_id',a.attempt_id,'transaction_id',a.transaction_id,'auth_ts',a.auth_ts,'txn_ts',t.txn_ts)), current_timestamp()
FROM __CATALOG__.bronze.auth_attempts a
LEFT JOIN __CATALOG__.bronze.transactions t ON a.transaction_id = t.transaction_id
WHERE try_to_timestamp(replace(replace(a.auth_ts,'T',' '),'Z',''))
    > COALESCE(try_to_timestamp(replace(replace(t.txn_ts,'T',' '),'Z','')),
               TIMESTAMP '2026-07-06 00:00:00');

-- DQ-CASEPARTY-RESOLVE — party_id must resolve per party_type  (conditional FK; composite record_key)
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','case_parties',cp._source_record_id,concat_ws('|',cp.case_id,cp.party_type,cp.party_id),
       'DQ-CASEPARTY-RESOLVE','party_id must resolve per party_type','unresolvable party_id for party_type','quarantine','quarantined',
       to_json(named_struct('case_id',cp.case_id,'party_type',cp.party_type,'party_id',cp.party_id,'role',cp.role)), current_timestamp()
FROM __CATALOG__.bronze.case_parties cp
WHERE (cp.party_type = 'customer' AND cp.party_id NOT IN (SELECT customer_id FROM __CATALOG__.bronze.customers))
   OR (cp.party_type = 'merchant' AND cp.party_id NOT IN (SELECT merchant_id FROM __CATALOG__.bronze.merchants));

-- DQ-CASEPARTY-TYPE-ENUM — party_type must be in {customer,merchant,third_party}  (composite record_key)
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','case_parties',cp._source_record_id,concat_ws('|',cp.case_id,cp.party_type,cp.party_id),
       'DQ-CASEPARTY-TYPE-ENUM','party_type must be in {customer,merchant,third_party}','invalid party_type','quarantine','quarantined',
       to_json(named_struct('case_id',cp.case_id,'party_type',cp.party_type,'party_id',cp.party_id)), current_timestamp()
FROM __CATALOG__.bronze.case_parties cp
WHERE cp.party_type NOT IN ('customer','merchant','third_party');

-- DQ-CTL-DNC-VIOLATION — no outbound contact when do_not_contact=true
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','customer_contact_logs',cl._source_record_id,cl.contact_id,
       'DQ-CTL-DNC-VIOLATION','no outbound contact when do_not_contact=true','DNC business-rule break','quarantine','quarantined',
       to_json(named_struct('contact_id',cl.contact_id,'customer_id',cl.customer_id,'direction',cl.direction,'do_not_contact',cl.do_not_contact)), current_timestamp()
FROM __CATALOG__.bronze.customer_contact_logs cl
WHERE cl.direction = 'outbound' AND cl.do_not_contact = 'true';


-- ============================================================================
--  TEXT_PII (2)
-- ============================================================================

-- DQ-NOTE-PII-LEAK — note_text must not contain raw PII/PAN
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','investigation_notes',_source_record_id,note_id,
       'DQ-NOTE-PII-LEAK','note_text must not contain raw PII/PAN','leaked PII and PAN in free text','quarantine','quarantined',
       to_json(named_struct('note_id',note_id,'case_id',case_id,'note_text',note_text)), current_timestamp()
FROM __CATALOG__.bronze.investigation_notes
WHERE note_text RLIKE '([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})|(\\+\\d{6,15})|(\\b\\d{13,19}\\b)|(\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b)';

-- DQ-CTL-NOTE-PII — note must not contain raw PII/PAN
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','customer_contact_logs',_source_record_id,contact_id,
       'DQ-CTL-NOTE-PII','note must not contain raw PII/PAN','leaked PII in contact note','quarantine','quarantined',
       to_json(named_struct('contact_id',contact_id,'customer_id',customer_id,'note',note)), current_timestamp()
FROM __CATALOG__.bronze.customer_contact_logs
WHERE note RLIKE '([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})|(\\+\\d{6,15})|(\\b\\d{13,19}\\b)|(\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b)';


-- ============================================================================
--  AI_EXCLUSION (2) — disposition = allowed_with_warning (valid data, excluded from AI)
-- ============================================================================

-- DQ-CASE-LEGALHOLD — legal_hold cases excluded from AI output
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','investigation_cases',_source_record_id,case_id,
       'DQ-CASE-LEGALHOLD','legal_hold cases excluded from AI output','legal_hold=true (must-not-expose)','quarantine','allowed_with_warning',
       to_json(named_struct('case_id',case_id,'status_code',status_code,'fraud_type_code',fraud_type_code,'legal_hold',legal_hold)), current_timestamp()
FROM __CATALOG__.bronze.investigation_cases
WHERE legal_hold = 'true';

-- DQ-NOTE-LEGALHOLD — notes on legal_hold cases must not reach AI  (cross-table)
INSERT INTO __CATALOG__.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name, failure_reason, severity, disposition, raw_record, detected_at)
SELECT 'RUN-20260708-DQ1','investigation_notes',n._source_record_id,n.note_id,
       'DQ-NOTE-LEGALHOLD','notes on legal_hold cases must not reach AI','note on legal_hold case','quarantine','allowed_with_warning',
       to_json(named_struct('note_id',n.note_id,'case_id',n.case_id,'note_text',n.note_text)), current_timestamp()
FROM __CATALOG__.bronze.investigation_notes n
JOIN __CATALOG__.bronze.investigation_cases c ON n.case_id = c.case_id
WHERE c.legal_hold = 'true';


-- ============================================================================
-- VERIFY — per-rule recall vs the manifest (caught vs expected record_keys).
-- Recall misses (expected_keys - caught_keys) > 0 mean a rule under-fires.
-- Precision (extra caught keys not in the manifest) is checked in E3-I6.
-- ============================================================================
SELECT
  m.rule_id,
  m.expected_keys,
  COALESCE(q.caught_keys, 0) AS caught_keys,
  m.expected_keys - COALESCE(q.caught_keys, 0) AS recall_misses
FROM (
  SELECT rule_id, COUNT(DISTINCT record_key) AS expected_keys
  FROM __CATALOG__.bronze.defects_manifest GROUP BY rule_id
) m
LEFT JOIN (
  SELECT rule_id, COUNT(DISTINCT record_key) AS caught_keys
  FROM __CATALOG__.silver.quarantine_records
  WHERE run_id = 'RUN-20260708-DQ1'
  GROUP BY rule_id
) q ON m.rule_id = q.rule_id
ORDER BY m.rule_id;

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
