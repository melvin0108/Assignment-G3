# Gold Dimensional Mart Design

## Scope

Implement `docs/gold-layer-implementation-plan.md` as the authoritative
contract. The Gold layer is a current-state, overwrite-based Delta mart in
`<catalog>.gold`, sourced only from one consistent latest Silver snapshot and
the matching DQ quarantine evidence.

## Components

- `pipeline/gold/gold_common.py` supplies catalog widgets, snapshot checks,
  deterministic SHA-256 surrogate keys, unknown members, standard metadata,
  safe overwrite writes, and forbidden-field checks.
- Gold build modules create the six dimensions, seven facts, and the
  case-grain `investigation_context` table defined in the implementation plan.
- `gold_all_tables.py` runs the modules in dependency order and stops when the
  Silver or Gold snapshot contracts are inconsistent.
- `validate_m3_gold.py` verifies model inventory, schemas, grain keys,
  referential integrity, snapshot consistency, safety restrictions, context
  reconciliation, and access grants.
- Each Gold table has one YAML contract; architecture guidance and reviewed,
  non-sensitive sample contexts document supported and refused AI questions.

## Data and Safety Rules

Eligible cases exclude legal holds and failed cases. Facts begin from eligible
case links and use documented `UNKNOWN` dimensions for optional missing
enrichment. Customer, employee, account, card, party identifiers, device/IP,
PAN, contact details, and author identity never enter Gold. Notes are included
only after PII screening. Case warnings incorporate matching structured
quarantine evidence, and context collections are typed empty arrays when no
records exist.

## Execution and Verification

The job runs Bronze, DQ, Silver, Gold, then M3 validation with a catalog
parameter. The Gold task applies idempotent `g3_ai_consumers` catalog/schema/
select grants only for Gold. Local tests cover static contracts and pure helper
logic; Databricks validation supplies the data-level and grant evidence.

## Non-goals

No chatbot, vector index, LLM-generated summaries, dashboard, SCD2 Gold
history, full transaction-domain publication, or restoration of the reverted
context-only implementation.
