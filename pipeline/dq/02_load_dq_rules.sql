-- ============================================================================
-- 02_load_dq_rules.sql
-- Epic 3 · E3-I1 — populate the rule registry (gov.dq_rules)
-- ============================================================================
--
-- ── WHAT THIS IS ────────────────────────────────────────────────────────────
-- One row per executable DQ rule. The 35 rule_ids are derived from
-- mock/generators.py — i.e. the exact set of defects the generator injects,
-- which is the distinct rule_id set in bronze.defects_manifest. This is the
-- authoritative inventory (NOT the "~36" estimate in PRODUCT-BACKLOG.md §6).
--
-- `expression` is a human-readable predicate for reviewers. The actual SQL
-- lives in 03/04_failures_*.sql (we chose block-per-rule, not data-driven), so
-- expression is documentation only and is never executed.
--
-- severity = 'quarantine' for every row: the mock generator (mock/defects.py)
-- defaults every injected defect to severity="quarantine", and defects_manifest
-- carries that value. disposition (per-record) is assigned at failure time.
--
-- ── PREREQUISITE ─────────────────────────────────────────────────────────────
-- Run 01_setup.sql first (creates gov + dq_rules + quarantine_records).
--
-- ── IDEMPOTENCY ──────────────────────────────────────────────────────────────
-- TRUNCATE + INSERT: safe to re-run; always reproduces the same 35 rows.
-- ============================================================================

TRUNCATE TABLE g3_catalog.gov.dq_rules;

INSERT INTO g3_catalog.gov.dq_rules
  (rule_id, rule_name, layer, target_table, target_key, pattern, severity, expression, enabled)
VALUES
  -- ── single_row (13) ────────────────────────────────────────────────────────
  ('DQ-TXN-AMT-POS',         'amount must be > 0',                       'bronze','transactions',         'transaction_id',            'single_row',  'quarantine', 'CAST(amount AS DOUBLE) <= 0', true),
  ('DQ-TXN-MERCH-REQ',       'merchant_id is required',                  'bronze','transactions',         'transaction_id',            'single_row',  'quarantine', 'merchant_id IS NULL OR merchant_id = ''''', true),
  ('DQ-TXN-TS-FUTURE',       'txn_ts must not be in the future',         'bronze','transactions',         'transaction_id',            'single_row',  'quarantine', 'txn_ts > RUN_DATE (2026-07-06)', true),
  ('DQ-ACC-OPENDATE-FUTURE', 'open_date must not be in the future',      'bronze','accounts',             'account_id',                'single_row',  'quarantine', 'open_date > RUN_DATE (2026-07-06)', true),
  ('DQ-CUST-EMAIL-FMT',      'email must match pattern if present',      'bronze','customers',            'customer_id',               'single_row',  'quarantine', 'email NOT RLIKE email-pattern (catches empty/malformed)', true),
  ('DQ-CARD-EXPIRED-ACTIVE', 'active card must not have a past expiry',  'bronze','cards',                'card_id',                   'single_row',  'quarantine', 'status = ''active'' AND expiry < RUN_DATE', true),
  ('DQ-MERCH-RISK-CASING',   'risk_rating must be in {low,medium,high}', 'bronze','merchants',           'merchant_id',               'single_row',  'quarantine', 'risk_rating NOT IN (''low'',''medium'',''high'')', true),
  ('DQ-DISP-STATUS-ENUM',    'status must be a lowercase dispute enum',  'bronze','disputes',             'dispute_id',                'single_row',  'quarantine', 'status NOT IN dispute_status enum', true),
  ('DQ-DISP-REASON-REQ',     'reason_code is required',                  'bronze','disputes',             'dispute_id',                'single_row',  'quarantine', 'reason_code IS NULL OR reason_code = ''''', true),
  ('DQ-ALT-SCORE-RANGE',     'score must be within [0,1]',               'bronze','fraud_alerts',         'alert_id',                  'single_row',  'quarantine', 'CAST(score AS DOUBLE) NOT BETWEEN 0 AND 1', true),
  ('DQ-DEV-TYPE-REQ',        'device_type is required',                  'bronze','transaction_devices',  'device_id',                 'single_row',  'quarantine', 'device_type IS NULL OR device_type = ''''', true),
  ('DQ-CASE-STATUS-ENUM',    'status_code must be in case_status enum',  'bronze','investigation_cases',  'case_id',                   'single_row',  'quarantine', 'status_code NOT IN case_status enum', true),
  ('DQ-CASE-STALE',          'open cases older than 180 days are stale', 'bronze','investigation_cases',  'case_id',                   'single_row',  'quarantine', 'status_code = ''open'' AND opened_at < RUN_DATE - 180 days', true),

  -- ── duplicate (6) ───────────────────────────────────────────────────────────
  ('DQ-CUST-ID-DUP',         'customer_id must be unique',               'bronze','customers',            'customer_id',               'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY customer_id) > 1', true),
  ('DQ-TXN-ID-DUP',          'transaction_id must be unique',            'bronze','transactions',         'transaction_id',            'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY transaction_id) > 1', true),
  ('DQ-CARD-DUP',            'card_id must be unique',                   'bronze','cards',                'card_id',                   'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY card_id) > 1', true),
  ('DQ-EMP-EMAIL-UNIQ',      'email must be unique',                     'bronze','employees',            'employee_id',               'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY email) > 1', true),
  ('DQ-CUST-NEAR-DUP',       'no two customers share name+dob+address+tax_id', 'bronze','customers',      'customer_id',               'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY first_name,last_name,dob,address,tax_id) > 1  (exact first pass; fuzzy TODO)', true),
  ('DQ-EMP-NAME-NEAR-DUP',   'flag near-duplicate employee names',       'bronze','employees',            'employee_id',               'duplicate',   'quarantine', 'row_number() OVER (PARTITION BY full_name) > 1  (exact first pass; fuzzy TODO)', true),

  -- ── fk_anti_join / relationship (12) ───────────────────────────────────────
  ('DQ-ACC-CUST-FK',         'customer_id must exist in customers',      'bronze','accounts',             'account_id',                'fk_anti_join','quarantine', 'accounts.customer_id NOT IN customers.customer_id', true),
  ('DQ-TXN-ACCT-FK',         'account_id must exist in accounts',        'bronze','transactions',         'transaction_id',            'fk_anti_join','quarantine', 'transactions.account_id NOT IN accounts.account_id', true),
  ('DQ-AUTH-TXN-FK',         'transaction_id must exist in transactions','bronze','auth_attempts',        'attempt_id',                'fk_anti_join','quarantine', 'auth_attempts.transaction_id NOT IN transactions.transaction_id', true),
  ('DQ-DISP-TXN-FK',         'transaction_id must exist in transactions','bronze','disputes',             'dispute_id',                'fk_anti_join','quarantine', 'disputes.transaction_id NOT IN transactions.transaction_id', true),
  ('DQ-CBK-DISP-FK',         'dispute_id must exist in disputes',        'bronze','chargebacks',          'chargeback_id',             'fk_anti_join','quarantine', 'chargebacks.dispute_id NOT IN disputes.dispute_id', true),
  ('DQ-DEV-TXN-FK',          'transaction_id must exist in transactions','bronze','transaction_devices',  'device_id',                 'fk_anti_join','quarantine', 'transaction_devices.transaction_id NOT IN transactions.transaction_id', true),
  ('DQ-CASETXN-TXN-FK',      'transaction_id must exist in transactions','bronze','case_transactions',    'case_id|transaction_id',    'fk_anti_join','quarantine', 'case_transactions.transaction_id NOT IN transactions.transaction_id', true),
  ('DQ-TXN-CARD-ACTIVE',     'transaction must use an active card',      'bronze','transactions',         'transaction_id',            'fk_anti_join','quarantine', 'JOIN cards ON card_id WHERE cards.status = ''closed''', true),
  ('DQ-AUTH-TS-ORDER',       'auth_ts must not be later than txn_ts',    'bronze','auth_attempts',        'attempt_id',                'fk_anti_join','quarantine', 'JOIN transactions: auth_ts > txn_ts', true),
  ('DQ-CASEPARTY-RESOLVE',   'party_id must resolve per party_type',     'bronze','case_parties',         'case_id|party_type|party_id','fk_anti_join','quarantine', 'conditional FK: party_id resolved against customers/merchants by party_type', true),
  ('DQ-CASEPARTY-TYPE-ENUM', 'party_type must be in {customer,merchant,third_party}', 'bronze','case_parties','case_id|party_type|party_id','fk_anti_join','quarantine', 'party_type NOT IN (''customer'',''merchant'',''third_party'')', true),
  ('DQ-CTL-DNC-VIOLATION',   'no outbound contact when do_not_contact=true', 'bronze','customer_contact_logs','contact_id',            'fk_anti_join','quarantine', 'direction = ''outbound'' AND do_not_contact = ''true''', true),

  -- ── text_pii (2) ────────────────────────────────────────────────────────────
  ('DQ-NOTE-PII-LEAK',       'note_text must not contain raw PII/PAN',    'bronze','investigation_notes',   'note_id',                   'text_pii',    'quarantine', 'note_text RLIKE email|phone|PAN', true),
  ('DQ-CTL-NOTE-PII',        'note must not contain raw PII/PAN',         'bronze','customer_contact_logs', 'contact_id',                'text_pii',    'quarantine', 'note RLIKE email|phone|PAN', true),

  -- ── ai_exclusion (2) ────────────────────────────────────────────────────────
  ('DQ-CASE-LEGALHOLD',      'legal_hold cases excluded from AI output',  'bronze','investigation_cases',   'case_id',                   'ai_exclusion','quarantine', 'legal_hold = ''true''', true),
  ('DQ-NOTE-LEGALHOLD',      'notes on legal_hold cases must not reach AI','bronze','investigation_notes',  'note_id',                   'ai_exclusion','quarantine', 'JOIN investigation_cases ON case_id WHERE legal_hold = ''true''', true);


-- ============================================================================
-- VERIFY
-- ============================================================================

-- (a) Total rule count.  EXPECTED: 35
SELECT COUNT(*) AS rule_count FROM g3_catalog.gov.dq_rules;

-- (b) Counts per pattern.  EXPECTED: single_row=13, duplicate=6, fk_anti_join=12,
--     text_pii=2, ai_exclusion=2.
SELECT pattern, COUNT(*) AS n
FROM g3_catalog.gov.dq_rules
GROUP BY pattern
ORDER BY n DESC;

-- (c) Sanity: every rule_id here should exist as a distinct rule_id in the
--     defects manifest. Any mismatch means the registry drifted from the
--     generator.  EXPECTED: 0 unmatched.
SELECT r.rule_id
FROM g3_catalog.gov.dq_rules r
LEFT JOIN (SELECT DISTINCT rule_id FROM g3_catalog.bronze.defects_manifest) m
       ON r.rule_id = m.rule_id
WHERE m.rule_id IS NULL;
