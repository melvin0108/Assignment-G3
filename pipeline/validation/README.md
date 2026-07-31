# Validation Checks

These checks are lightweight Databricks validation scripts for completed backlog work.
They validate the recreated tables in the `g3_test` catalog.

Run order after Bronze and DQ notebooks:

1. Run all M1 Bronze ingestion notebooks.
2. Run `validate_m1_bronze.py`.
3. Run M2 DQ setup, rule load, and failure generation notebooks.
4. Run `pipeline/silver/silver_all_tables.py`.
5. Run `validate_m2_dq.py`.

`validate_m1_bronze.py` checks:

- All contract fields remain present and all source/evolved Bronze fields are `STRING`.
- Required Bronze metadata is present and file/batch lineage is valid.
- Rows with `_rescued_data` are reported as schema/type mismatches, while rows
  with `_corrupt_record` are reported as malformed CSV warnings.
- M1 Bronze tables exist.
- Required Bronze metadata columns exist on ingested Bronze source tables.
- Important Bronze tables have rows.
- Bronze `_record_hash` duplicate samples are reported as warnings.
- `bronze.defects_manifest` is loaded cleanly.

`validate_m2_dq.py` checks:

- Every quarantine rule, including `DQ-*-TYPE`, exists in `gov.dq_rules`.
- Type-cast failures are absent from the corresponding clean Silver table.
- M2 DQ rule registry has enabled rules loaded.
- Manifest rule IDs exist in the DQ registry; registry rules without manifest seeds are reported as warnings.
- `silver.quarantine_records` has required fields populated.
- The latest quarantine run is selected automatically.
- Quarantine recall/precision gaps are reported as warnings.

The script raises an exception if any blocking check fails. Save the Databricks
cell output as evidence for E2-I5, E3-I6, and E7-I1/E7-I3.

After Gold, run `pipeline/validation/validate_gold.py`. If the authenticated
Consumer semantic layer is deployed, also run
`consumer_metrics_view/validate_consumer_metric_views.py` and save that output
with the M3 evidence. Local tests do not establish that the parameterized
views were deployed or that runtime grants are correct.
