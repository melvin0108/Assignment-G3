# Validation Checks

These checks are lightweight Databricks validation scripts for completed backlog work.
They validate the recreated tables in the `g3_test` catalog.

Run order after Bronze and DQ notebooks:

1. Run all M1 Bronze ingestion notebooks.
2. Run `validate_m1_bronze.py`.
3. Run M2 DQ setup, rule load, and failure generation notebooks.
4. Run `validate_m2_dq.py`.

`validate_m1_bronze.py` checks:

- M1 Bronze tables exist.
- Required Bronze metadata columns exist on ingested Bronze source tables.
- Important Bronze tables have rows.
- Bronze `_record_hash` duplicate samples are reported as warnings.
- `bronze.defects_manifest` is loaded cleanly.

`validate_m2_dq.py` checks:

- M2 DQ rule registry has enabled rules loaded.
- DQ rules match the manifest rule IDs.
- `silver.quarantine_records` has required fields populated.
- Quarantine catches manifest defects and reports recall/precision gaps.

The script raises an exception if any blocking check fails. Save the Databricks
cell output as evidence for E2-I5, E3-I6, and E7-I1/E7-I3.
