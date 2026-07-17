# Gold Dimensional Mart

Gold publishes a current-state, AI-safe investigation mart to
`<catalog>.gold`. `gold_all_tables.py` refuses to start unless every required
Silver input has the same single `_batch_id` and `_run_id`; it then writes all
models by overwrite for that snapshot.

## Models and joins

`dim_case` is the centre of the mart. Facts join through `case_key`; transaction
facts additionally join merchant, channel, currency, and date dimensions.
Optional enrichment resolves to a documented `UNKNOWN` member. Legal-hold and
failed cases are excluded before any fact is built.

`investigation_context` is the only retrieval document: one row for every
Gold case, with typed arrays for transactions, disputes/chargebacks, alerts,
safe notes, and party summaries. It carries no surrogate keys and uses a
deterministic case summary; it must not infer guilt or a fraud conclusion.

## Output policy

`investigation_context` is the AI-allowed retrieval output. Gold dimensions
and facts are `internal_only` investigation-support outputs. Bronze, Silver,
and quarantine records are operational outputs and are never AI retrieval
sources. This prototype documents the policy with `usage_restrictions`; it
does not provision Databricks users, groups, or grants.

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
