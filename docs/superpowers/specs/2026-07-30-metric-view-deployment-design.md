# Metric view deployment correction

## Scope

Correct the Party metric-view YAML so its names are unique, and make the
catalog selection possible before metric-view DDL is run.

## Design

- Rename the raw `party_count` field in `08_party_metrics.yaml` to
  `party_count_group`. This preserves the scalar Gold source column for
  grouping while reserving `party_count` for the `SUM(party_count)` measure.
- Make the first Databricks notebook cell create/reuse the `catalog` widget.
  Place the schema creation and view deployment in a following cell, so the
  user can run the first cell, select `g3_dev`, `g3_test`, or `g3_catalog`, and
  then run deployment.
- Add a local regression test that parses all definitions and rejects names
  shared by `fields` and `measures`.
- Update the metric-view README to name the correct runner and describe the
  two-cell execution sequence.

## Verification

Run the new metric-view contract test and the full local pytest suite.
