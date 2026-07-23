# Assignment-G3: Databricks Free Edition Runbook

This runbook executes the complete Transaction Investigation pipeline in a
Databricks Free Edition workspace. Run every file below as a Databricks notebook
from the repository root; do not run the orchestration files in a local terminal.

The pipeline produces synthetic CSV data, ingests it to Bronze, applies DQ and
Silver transformations, builds Gold models, then validates each completed layer.

## Prerequisites

1. Create or open a Databricks Free Edition workspace.
2. Add this repository to the workspace as a Git folder, or import the repository
   files while preserving the directory structure. The notebook paths must remain
   unchanged because the orchestration notebooks import sibling modules and call
   child notebooks by relative path.
3. Use one catalog for the entire run. The repository supports `g3_dev`,
   `g3_test`, and `g3_catalog`; use `g3_dev` for a development run.
4. Confirm that the selected catalog is available and that you can create schemas,
   tables, and volumes in it. Each notebook exposes a `catalog` widget; select
   the same value in every notebook.
5. Open notebooks from the repository root in the Free Edition workspace and run
   them on the available serverless compute. The generator installs Faker in its
   first cell and restarts Python; after that restart, continue with **Run all**.

> Start from an empty catalog for a first run. Re-running the generator creates a
> new numbered source batch. Bronze ingests source files it has not processed, so
> a rerun is an additional batch rather than a replacement of the prior batch.

## Execution order

Run the notebooks in this order. The validation notebooks are placed immediately
after their dependencies so a failure is isolated to the layer that produced it.

| Order | Notebook | Purpose | Expected result |
|---:|---|---|---|
| 1 | `generate_mock_databricks.py` | Runs the `mock/` generator in Databricks. | Numbered CSV batches are written to `/Volumes/<catalog>/bronze/raw_data/<table>/`, including `defects_manifest`. |
| 2 | `pipeline/bronze/bronze_all_tables.py` | Orchestrates every Bronze ingestion notebook in `pipeline/bronze/`. | Source CSVs are ingested to `<catalog>.bronze.*` with batch and file lineage metadata. |
| 3 | `pipeline/validation/validate_m1_bronze.py` | Validates the completed Bronze layer. | Ends with `PASS: M1 Bronze validation completed with no blocking failures.` |
| 4 | `pipeline/dq/dq_01_setup.py` | Creates `<catalog>.gov` and initializes `<catalog>.gov.dq_rules` and `<catalog>.silver.quarantine_records`. | Both DQ tables exist. |
| 5 | `pipeline/dq/dq_02_load_dq_rules.py` | Loads the enabled DQ-rule registry. | `<catalog>.gov.dq_rules` contains enabled rules. |
| 6 | `pipeline/dq/dq_03_failures_all_rules.py` | Evaluates Bronze data against all DQ rules and writes the current run's quarantine records. | `<catalog>.silver.quarantine_records` contains the current DQ run. |
| 7 | `pipeline/silver/silver_all_tables.py` | Orchestrates every Silver transformation notebook in `pipeline/silver/` in dependency-safe order. | Clean, typed, protected Silver tables are created for the same Bronze snapshot. |
| 8 | `pipeline/validation/validate_m2_dq.py` | Validates the DQ registry, quarantine records, and their use by Silver. | All blocking M2 DQ checks pass; recall/precision differences, if any, are warnings. |
| 9 | `pipeline/validation/validate_m2_silver.py` | Validates Silver contracts, integrity, business rules, privacy protection, and Gold inputs. | Ends with `PASS: M2 Silver validation completed with no blocking failures.` |
| 10 | `pipeline/gold/gold_all_tables.py` | Orchestrates every Gold model in `pipeline/gold/` in dependency-safe order. | `<catalog>.gold.*` dimensional models and `investigation_context` are created. |
| 11 | `pipeline/validation/validate_m3_gold.py` | Validates Gold contracts, natural grains, metadata, AI policy, and referential integrity. | Ends with `M3 Gold validation passed...`. |

## How to run each notebook

For each row in the execution order:

1. In the Databricks workspace browser, open the listed `.py` file from the
   repository folder. Databricks recognizes the `# Databricks notebook source`
   format as a notebook.
2. Set the `catalog` widget to `g3_dev` (or the one catalog chosen for the run).
3. Select **Run all** and wait for the notebook to finish successfully before
   starting the next row.
4. Keep the completed cell output as execution evidence, especially for the four
   validation notebooks.

The all-tables notebooks are the only Bronze, Silver, and Gold entry points
needed for the standard run. They invoke the individual layer notebooks in their
defined dependency order; do not manually run every child notebook as well.

## Why the DQ substeps are required

`dq_01_setup.py` creates the DQ table structure, including
`<catalog>.gov.dq_rules`, but it does not populate the rule registry. Therefore
`dq_02_load_dq_rules.py` and `dq_03_failures_all_rules.py` are mandatory before
running Silver. Skipping them leaves Silver without the DQ rules and quarantine
results required for a valid pipeline run.

## Completion checklist

- The generator reports a published batch for every source table and
  `defects_manifest`.
- Bronze reports completion for all configured tables, and M1 validation passes.
- DQ setup, rule loading, and failure generation complete without errors.
- Silver reports completion for all transformation notebooks; both M2 validation
  notebooks pass their blocking checks.
- Gold reports completion, and M3 Gold validation passes.

For mock-generator configuration, row counts, defect rates, and the generated
source-table contract, see [mock/README.md](mock/README.md).
