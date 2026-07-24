# Deliverable 6 — AI-Ready Context Output

## Purpose

This deliverable explains the context that may be used by an AI consumer in
the transaction-investigation prototype. Its purpose is to give an AI assistant
trusted, relevant information for investigation questions without giving it
unrestricted access to operational or sensitive data.

The AI-ready context has two parts:

1. `g3_dev.gold.investigation_context` is the curated data output that an AI
   consumer can retrieve for a specific investigation case.
2. The YAML files in `docs/models/gold/` are the data contracts and query
   guidance. They describe approved tables, fields, relationships, metrics,
   access labels, and supported question patterns.

Together, these assets provide context for accurate answers and SQL generation
based on defined contracts instead of assumptions about the database.

## AI-ready data output

`g3_dev.gold.investigation_context` contains one deterministic, AI-safe
document for each eligible `case_id`. It is the only Gold table labelled
`ai_allowed` and is the approved retrieval source for case-detail questions.

Each context document brings together the safe, case-related information that
an investigator may need, including:

- case priority, status, fraud type, and open/close dates;
- a deterministic case summary that does not infer guilt or a fraud outcome;
- linked transactions, disputes and chargebacks, fraud alerts, and
  authorisation attempts;
- PII-screened investigation notes; and
- safe party counts by party type and role.

The document also includes the context category, quality status, warning flags,
source references, masking status, usage restriction, context version, pipeline
run ID, batch ID, and last-refresh timestamp. These fields help a human or an
AI consumer understand how current, complete, and safe the result is before
using it.

## Safety and access boundaries

The pipeline applies privacy and quality controls before the context is made
available. Cases on legal hold and failed cases are excluded before Gold models
are created. Investigation notes are screened so that only safe note text can
appear in the context. The context contract records `masking_status` as
`masked` or `partial`, and `quality_status` as `pass` or `partial`.

The following data must not be retrieved directly by an AI consumer from this
output: customer, employee, account, card, party, device/IP, PAN, contact
details, legal-hold records, and raw Bronze, Silver, or quarantine records.
Those layers may contain operational or sensitive information and are not
AI retrieval sources.

Gold dimensions and fact tables are PII-safe but labelled `internal_only`.
They are therefore not general AI retrieval sources. They may support approved
analytics through a future trusted query broker, which validates the request
and returns a bounded result rather than providing unrestricted table access.

## Gold contracts and routing guidance

The files in `docs/models/gold/` define the data contracts for the Gold
dimension, fact, and context models. A model contract identifies the table,
grain, primary key, source models, available columns, data types,
relationships, quality and access metadata, business dimensions, and supported
metrics. This gives developers a stable reference for maintaining the pipeline
and gives an AI agent approved facts to use when it generates a query.

`docs/models/gold/investigation_context.yml` is the contract for the direct
case-detail context. `docs/models/gold/questions-to-metrics.yaml` maps
Vietnamese and English question patterns to supported analytics or detail
routes. The remaining model YAML files supply the contracts that are needed by
the approved analytics routes.

The routing file supports case overview, transaction activity, authorisation
outcomes, dispute activity, chargeback activity, fraud-alert activity, safe
notes, party composition, and case-detail lookup. It also specifies the
primary table, supporting tables, allowed dimensions and filters, and relevant
metric IDs for each route.

## How a developer or AI agent should use the context

For a case-detail request, use `g3_dev.gold.investigation_context` and filter
by the requested `case_id`. Use the attached quality, warning, source, and
refresh metadata when presenting or verifying the answer.

For an analytics request, the following controlled process should be followed:

1. Match the question to a pattern in `questions-to-metrics.yaml`.
2. Read only the primary and supporting model contracts named by that pattern.
3. Use the documented metric expression, dimensions, filters, grain, and join
   relationships to construct the SQL.
4. For monetary metrics, group or filter by `currency_code`; amounts from
   different currencies must not be combined.
5. Return generated SQL for review, or send it to a trusted query broker that
   validates and executes only the approved query.

This process narrows the available schema to the tables and fields relevant to
the question. It reduces incorrect joins, unsupported metrics, and guessed
SQL while preserving the access boundary between AI-safe context and internal
operational data.

## Unsupported, restricted, or incomplete requests

The AI consumer must not attempt to fill gaps with assumptions. It should ask
for clarification or decline the request when:

- the question does not match a documented route or is ambiguous;
- the request needs a restricted field or a legal-hold record;
- the available context does not contain the requested information; or
- a requested conclusion, such as a determination of guilt or fraud, cannot
  be supported by the recorded context.

Where a context record has `quality_status: partial` or warning flags, the
response should state that the information has a known limitation and should
use `source_references` for verification. A context record labelled
`ai_allowed` does not remove the need to follow these restrictions.

## Requirement coverage

| Assignment requirement | Evidence in this output |
|---|---|
| Curated AI-ready context output | `g3_dev.gold.investigation_context`, one context document per eligible case |
| Cleaned and transformed fields | Case, transaction, dispute, chargeback, alert, authorisation, note, and party-summary fields are assembled from Gold models |
| PII protection | PII-screened safe notes, masking status, excluded sensitive fields, and legal-hold exclusion |
| Source traceability | `source_references`, `pipeline_run_id`, `batch_id`, and `last_refreshed_at` |
| Quality information | `quality_status` and `warning_flags` |
| Context category and access restriction | `context_category` and `usage_restrictions` |
| Safe AI use | Only the context table is `ai_allowed`; routing contracts constrain analytics SQL and unsupported requests are refused or clarified |

## Related repository assets

- Data output contract: `docs/models/gold/investigation_context.yml`
- Question-routing contract: `docs/models/gold/questions-to-metrics.yaml`
- Supporting Gold model contracts: `docs/models/gold/*.yml`
- Gold-layer policy and implementation overview: `docs/gold-layer.md`
- Gold validation: `pipeline/validation/validate_m3_gold.py`

The catalog prefix may differ between environments. This deliverable uses
`g3_dev` because it is the submission environment; the model contracts use
`gold.<table_name>` as the reusable schema-level name.
