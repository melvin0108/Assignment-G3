# Gold Dimensional Mart and Semantic Routing

Gold publishes a current-state, AI-safe investigation mart to
`<catalog>.gold`. `gold_all_tables.py` refuses to start unless every required
Silver input has the same single `_batch_id` and `_run_id`; it then writes all
models by overwrite for that snapshot.

## Models and joins

`dim_case` is the centre of the mart and is keyed by `case_id`. Facts carry
`case_id` plus their complete natural grains and join dimensions through
`merchant_id`, `channel_code`, `currency_code`, and `reason_code`. Optional
enrichment resolves to a documented `UNKNOWN` member. `date_key` remains the
non-hashed `yyyyMMdd` key and `0` is its unknown member. Legal-hold and failed
cases are excluded before any fact is built.

`investigation_context` is the only retrieval document: one row for every
Gold case, with typed arrays for transactions, disputes/chargebacks, alerts,
safe notes, and party summaries. It carries no surrogate keys and uses a
deterministic case summary; it must not infer guilt or a fraud conclusion.

## Output policy

All 14 PII-safe Gold outputs, including `investigation_context`, are
`ai_allowed`. Bronze, Silver, and quarantine records are operational outputs
and are never AI retrieval sources. This prototype documents the policy with
`usage_restrictions`; it does not provision Databricks users, groups, or grants.

## AI use

The internal AI consumer may answer questions such as “Which case-linked
transactions were disputed?”, “What alerts were triggered?”, and “What are
the safe investigation notes?”. It must refuse requests for customer,
employee, account, card, party, device/IP, PAN, contact details, legal-hold
records, or conclusions not present in the context.

## Run and validate

Run `pipeline/gold/gold_all_tables.py` after Silver, then
`pipeline/validation/validate_m3_gold.py`. The runner publishes output policy
labels only; it does not manage Databricks principals or grants. Model-level
grain, source, key, safety, and use-case contracts are in `docs/models/gold/`.

## Natural grains

The fact grains are `(case_id, transaction_id)`, `(case_id, attempt_id)`,
`(case_id, dispute_id)`, `(case_id, chargeback_id)`, `(case_id, alert_id)`,
`(case_id, note_id)`, and `(case_id, party_type, role)`. No SHA-256 surrogate
or fact keys are published.

## Semantic routing

Every model YAML declares its physical columns and types, business-key
relationships, dimensions, synonyms, AI access, and metrics. Monetary metrics
must group or filter by `currency_code`; amounts from different currencies
must not be combined.

`docs/models/gold/questions-to-metrics.yaml` routes Vietnamese and English
questions for case overview, transactions, authorization outcomes, disputes,
chargebacks, fraud alerts, safe notes, party composition, and case-detail
lookup. Analytics routes reference model metric IDs. Detail lookup routes to
`gold.investigation_context` without an artificial metric or embedded SQL.

## Breaking migration

This is a breaking schema migration. Legacy `*_key` columns and SHA-256 helpers
are removed with no compatibility columns or views. Downstream queries must use
the natural joins above and rebuild Gold cleanly before consuming the new
contracts. M3 validates exact YAML types, natural-grain uniqueness,
business-key referential integrity, documented UNKNOWN members, one context row
per `case_id`, absence of legacy hashed keys, and `ai_allowed` on every row.
