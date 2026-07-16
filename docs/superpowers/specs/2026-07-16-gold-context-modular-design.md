# Modular Gold Investigation Context Design

## Goal

Refactor the Gold implementation so `gold.investigation_context` remains one
current-state Delta output, while its transformation logic is split into small,
debuggable Python modules.

## Structure

`pipeline/gold/01_gold_investigation_context.py` is the only notebook entry
point and table writer. It creates the catalog widget, loads the shared Silver
snapshot, calls the domain builders, joins their pre-aggregated results by
`case_id`, writes Gold, records counts, and refreshes field-level lineage.

The runner imports:

- `common.py`: catalog and snapshot validation, typed empty arrays, and shared
  source-reference helpers.
- `case_context.py`: safe case base, deterministic case summary, and warning
  flags from current-run quarantine rows.
- `transaction_context.py`: transaction, payment, merchant, authorization,
  dispute/chargeback, and fraud-alert collections. Every collection is grouped
  by `case_id` before it reaches the runner.
- `supporting_context.py`: safe notes, party-role context, and distinct source
  references for all included records.
- `lineage.py`: targeted rewrite of `gov.metadata_lineage` rows for the Gold
  table.
- `contract.py`: runtime-independent public-contract constants and helpers.

## Data flow

1. Validate that every required Silver input has one identical `_batch_id` and
   `_run_id`.
2. Build the eligible case base, excluding null, legal-hold, and current-run
   quarantined cases.
3. Build each nested domain collection independently and pre-aggregate it by
   `case_id` using deterministic ordering.
4. Build warning flags, safe notes, party roles, and source references from
   included records only.
5. The runner left-joins the aggregates to the case base, supplies typed empty
   arrays, derives `quality_status`, and adds Gold metadata.
6. Write the sole Gold table with overwrite semantics and replace only its
   lineage rows.

## Constraints

- No Gold staging tables or generic framework.
- No `select("*")` in Gold output construction.
- No direct customer, employee, account/card/party ID, device/IP, or PAN
  fields in the Gold schema.
- `validate_m3_gold.py`, model metadata, sample output, and job sequence stay
  aligned with the unchanged public Gold contract.

## Verification

- Unit-test runtime-independent contract helpers locally when Python is
  available.
- Run `validate_m3_gold.py` after a clean `g3_test` Bronze → DQ → Silver →
  Gold sequence.
- Confirm a rebuild from the same Silver run preserves all fields except
  `last_refreshed_at`.
