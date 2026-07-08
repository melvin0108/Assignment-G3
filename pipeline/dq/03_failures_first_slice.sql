-- ============================================================================
-- 03_failures_first_slice.sql
-- Epic 3 · E3-I4 — prove the failure-query template on ONE rule per pattern
-- ============================================================================
--
-- ── WHAT THIS IS ────────────────────────────────────────────────────────────
-- Five failure queries — one per pattern shape — to prove the template works
-- before committing to all 35 (04_failures_all_rules.sql). Once you've verified
-- these five reconcile against the manifest, 04 supersedes this script.
--
--   single_row    → DQ-TXN-AMT-POS    (predicate on one table)
--   duplicate     → DQ-TXN-ID-DUP     (row_number() window)
--   fk_anti_join  → DQ-TXN-ACCT-FK    (LEFT JOIN ... IS NULL)
--   text_pii      → DQ-NOTE-PII-LEAK  (RLIKE email/phone/PAN)
--   ai_exclusion  → DQ-CASE-LEGALHOLD (policy predicate; disposition differs)
--
-- ── run_id ──────────────────────────────────────────────────────────────────
-- Inlined as the literal 'RUN-20260708-DQ1' (same convention as 01_ingest_bronze.sql,
-- which deliberately avoids SQL variables/widgets). To run a fresh run_id,
-- find-replace 'RUN-20260708-DQ1' across this file.
--
-- ── IDEMPOTENCY ──────────────────────────────────────────────────────────────
-- The DELETE wipes this run_id's prior quarantine rows first, so re-running the
-- whole script replaces this run cleanly. Changing the run_id accumulates history.
--
-- ── PREREQUISITES ────────────────────────────────────────────────────────────
-- 01_setup.sql and 02_load_dq_rules.sql already run; bronze source tables loaded.
-- ============================================================================


-- 0. Reset this run's quarantine rows so the script is re-runnable.
DELETE FROM g3_catalog.silver.quarantine_records WHERE run_id = 'RUN-20260708-DQ1';


-- ============================================================================
-- 1/5  single_row · DQ-TXN-AMT-POS — amount must be > 0
-- ============================================================================
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name,
   failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260708-DQ1',
  'transactions',
  _source_record_id,
  transaction_id,
  'DQ-TXN-AMT-POS',
  'amount must be > 0',
  'negative amount',
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'transaction_id', transaction_id, 'account_id', account_id, 'card_id', card_id,
    'merchant_id', merchant_id, 'amount', amount, 'txn_ts', txn_ts, 'status', status)),
  current_timestamp()
FROM g3_catalog.bronze.transactions
WHERE try_cast(amount AS DOUBLE) <= 0;


-- ============================================================================
-- 2/5  duplicate · DQ-TXN-ID-DUP — transaction_id must be unique
--     Flags the duplicate occurrence (row_number > 1), not the original — this
--     matches the generator's convention of logging the injected duplicate.
-- ============================================================================
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name,
   failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260708-DQ1',
  'transactions',
  _source_record_id,
  transaction_id,
  'DQ-TXN-ID-DUP',
  'transaction_id must be unique',
  'duplicate transaction_id',
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'transaction_id', transaction_id, 'account_id', account_id, 'amount', amount,
    'txn_ts', txn_ts, 'status', status)),
  current_timestamp()
FROM (
  SELECT transaction_id, _source_record_id, account_id, amount, txn_ts, status,
         row_number() OVER (PARTITION BY transaction_id ORDER BY _source_record_id) AS rn
  FROM g3_catalog.bronze.transactions
)
WHERE rn > 1;


-- ============================================================================
-- 3/5  fk_anti_join · DQ-TXN-ACCT-FK — account_id must exist in accounts
-- ============================================================================
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name,
   failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260708-DQ1',
  'transactions',
  t._source_record_id,
  t.transaction_id,
  'DQ-TXN-ACCT-FK',
  'account_id must exist in accounts',
  'orphan account+card',
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'transaction_id', t.transaction_id, 'account_id', t.account_id,
    'card_id', t.card_id, 'amount', t.amount)),
  current_timestamp()
FROM g3_catalog.bronze.transactions t
LEFT JOIN g3_catalog.bronze.accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL;


-- ============================================================================
-- 4/5  text_pii · DQ-NOTE-PII-LEAK — note_text must not contain raw PII/PAN
--     RLIKE matches email, +phone, or a PAN (16 digits, optionally grouped).
--     Injected leak notes contain email+phone+PAN together, so any branch fires.
-- ============================================================================
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name,
   failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260708-DQ1',
  'investigation_notes',
  _source_record_id,
  note_id,
  'DQ-NOTE-PII-LEAK',
  'note_text must not contain raw PII/PAN',
  'leaked PII and PAN in free text',
  'quarantine',
  'quarantined',
  to_json(named_struct(
    'note_id', note_id, 'case_id', case_id, 'note_text', note_text)),
  current_timestamp()
FROM g3_catalog.bronze.investigation_notes
WHERE note_text RLIKE '([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})|(\\+\\d{6,15})|(\\b\\d{13,19}\\b)|(\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b)';


-- ============================================================================
-- 5/5  ai_exclusion · DQ-CASE-LEGALHOLD — legal_hold cases excluded from AI
--     disposition = 'allowed_with_warning': the case is valid data, just
--     excluded from the AI surface downstream (E6). Other rules use 'quarantined'.
-- ============================================================================
INSERT INTO g3_catalog.silver.quarantine_records
  (run_id, source_table, source_record_id, record_key, rule_id, rule_name,
   failure_reason, severity, disposition, raw_record, detected_at)
SELECT
  'RUN-20260708-DQ1',
  'investigation_cases',
  _source_record_id,
  case_id,
  'DQ-CASE-LEGALHOLD',
  'legal_hold cases excluded from AI output',
  'legal_hold=true (must-not-expose)',
  'quarantine',
  'allowed_with_warning',
  to_json(named_struct(
    'case_id', case_id, 'status_code', status_code,
    'fraud_type_code', fraud_type_code, 'legal_hold', legal_hold)),
  current_timestamp()
FROM g3_catalog.bronze.investigation_cases
WHERE legal_hold = 'true';


-- ============================================================================
-- VERIFY — quarantine vs manifest for these 5 rules.
-- ============================================================================

-- (a) Rows written this run, per rule.
SELECT rule_id, COUNT(*) AS quarantined
FROM g3_catalog.silver.quarantine_records
WHERE run_id = 'RUN-20260708-DQ1'
GROUP BY rule_id
ORDER BY rule_id;

-- (b) Side-by-side: how many record_keys we caught vs the manifest expects.
--     EXPECTED: caught = expected for all 5 (recall 100%, precision 100%).
--     (Full precision/recall with exception lists is E3-I6's job.)
SELECT
  m.rule_id,
  m.expected_keys,
  q.caught_keys,
  m.expected_keys - q.caught_keys AS recall_misses
FROM (
  SELECT rule_id, COUNT(DISTINCT record_key) AS expected_keys
  FROM g3_catalog.bronze.defects_manifest
  WHERE rule_id IN ('DQ-TXN-AMT-POS','DQ-TXN-ID-DUP','DQ-TXN-ACCT-FK','DQ-NOTE-PII-LEAK','DQ-CASE-LEGALHOLD')
  GROUP BY rule_id
) m
JOIN (
  SELECT rule_id, COUNT(DISTINCT record_key) AS caught_keys
  FROM g3_catalog.silver.quarantine_records
  WHERE run_id = 'RUN-20260708-DQ1'
  GROUP BY rule_id
) q ON m.rule_id = q.rule_id
ORDER BY m.rule_id;
