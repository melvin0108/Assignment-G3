# Investigation-context metric view design

## Scope

Add `mv_investigation_context_metrics`, sourced from
`g3_catalog.gold.investigation_context`, to the governed metric-view
deployment. The source table is the only Gold model classified `ai_allowed`.

## Definition

The view exposes the context's scalar retrieval and governance fields,
including `case_id`, `context_category`, selected `case_detail` attributes,
quality and access status, and refresh metadata. It provides a `case_count`
measure using `COUNT(DISTINCT source.case_id)`.

Nested arrays and structs representing transactions, disputes, fraud alerts,
authorizations, safe notes, party summaries, warnings, and source references
remain available through the direct `gold.investigation_context` Genie source.
They are not metric-view fields because this view is for aggregate SQL.

## Delivery and verification

Register the YAML definition in `14_create_metric_views.py`, document the new
fourteenth view in `metrics_view/README.md`, and update the Genie deliverable
to state that the agent receives fourteen metric views plus the direct context
table. Extend the local contract test to require the definition and deployment
registration, then run that test and the full local suite.
