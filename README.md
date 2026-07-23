# Assignment-G3: Databricks Free Edition Runbook

This runbook executes the complete Transaction Investigation pipeline in a
Databricks Free Edition workspace. Run every file below as a Databricks notebook
from the repository root; do not run the orchestration files in a local terminal.

The pipeline produces synthetic CSV data, ingests it to Bronze, applies DQ and
Silver transformations, builds Gold models, then validates each completed layer.
The source repository is [melvin0108/Assignment-G3](https://github.com/melvin0108/Assignment-G3).

## Prerequisites

1. A Databricks Free Edition account and workspace.
2. Access to GitHub and this repository:
   `https://github.com/melvin0108/Assignment-G3`.
3. Permission to create or use one of the catalog names supported by the
   notebooks: `g3_dev`, `g3_test`, or `g3_catalog`. Use `g3_dev` for the normal
   development run.
4. Serverless compute available in the Free Edition workspace.

## Setup steps

1. Create a Databricks Free Edition account, sign in, and create or open your
   workspace.
2. In the workspace, create a Git folder and connect your GitHub account when
   prompted. Use the repository URL
   `https://github.com/melvin0108/Assignment-G3`, select the required branch,
   and clone it into the workspace.
3. Open the cloned repository folder. Preserve its directory structure: the
   orchestration notebooks import sibling modules and call child notebooks using
   relative paths.
4. In Catalog Explorer, create or select `g3_dev`. The pipeline creates the
   Bronze, Silver, Gold, and governance schemas and volumes it needs inside the
   selected catalog.
5. Open any listed notebook from the cloned repository. Set its `catalog` widget
   to `g3_dev` and use that same catalog value for every notebook in the run.
6. Run `generate_mock_databricks.py` first. Its first cell installs Faker and
   restarts Python; after the restart, select **Run all** again to complete the
   generator.

> For a fully isolated rerun, use an unused supported catalog such as `g3_test`,
> then execute the full sequence below with every `catalog` widget set to that
> catalog. A normal rerun in the same catalog creates a new numbered source batch;
> Bronze ingests previously unseen files and Silver/Gold use the latest matching
> snapshot.

## How to run the pipeline

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

## How to run tests and validation checks

The executable validation notebooks are already included in the pipeline order.
Run them after the relevant layer, using the same catalog widget value:

| Layer | Validation notebook | What it proves |
|---|---|---|
| Bronze | `pipeline/validation/validate_m1_bronze.py` | Bronze tables, metadata, lineage, contract fields, and source data checks. |
| DQ | `pipeline/validation/validate_m2_dq.py` | Enabled rules, quarantine records, DQ-to-Silver handling, and manifest reconciliation. |
| Silver | `pipeline/validation/validate_m2_silver.py` | Silver schema, integrity, business rules, masking, lineage, and Gold inputs. |
| Gold | `pipeline/validation/validate_m3_gold.py` | Gold contracts, natural grains, metadata, AI policy, and relationships. |

Each notebook raises an exception for a blocking failure. Save the completed
notebook output as test evidence. `validate_m2_dq.py` also prints a JSON summary
containing passed checks, warnings, quarantined-record counts, and sample failed
records.

## Where outputs are generated

Replace `<catalog>` below with the catalog selected in the widgets (normally
`g3_dev`).

| Layer or output | Location |
|---|---|
| Generated source CSVs | `/Volumes/<catalog>/bronze/raw_data/<table>/` |
| Raw ingested tables | `<catalog>.bronze.*` |
| DQ rule registry | `<catalog>.gov.dq_rules` |
| Quarantined records | `<catalog>.silver.quarantine_records` |
| Clean, masked Silver tables | `<catalog>.silver.*` |
| Masking policy and lineage metadata | `<catalog>.gov.masking_policies` and `<catalog>.gov.metadata_lineage` |
| Curated Gold models | `<catalog>.gold.*` |
| AI-ready investigation context | `<catalog>.gold.investigation_context` |

`<catalog>.gold.investigation_context` is the curated context output intended
for AI retrieval. It includes quality status, warning flags, source references,
refresh metadata, and usage restrictions.

## How to inspect quarantined records

Open a SQL editor or a notebook cell in Databricks and replace `g3_dev` with the
catalog used for the run.

```sql
-- Recent failed or restricted records, with the reason and disposition
SELECT
  run_id,
  source_table,
  source_record_id,
  record_key,
  rule_id,
  rule_name,
  failure_reason,
  severity,
  disposition,
  detected_at
FROM g3_dev.silver.quarantine_records
ORDER BY detected_at DESC
LIMIT 100;
```

```sql
-- Quarantine volume by rule for a specific run ID from the validation output
SELECT
  rule_id,
  rule_name,
  disposition,
  COUNT(*) AS record_count
FROM g3_dev.silver.quarantine_records
WHERE run_id = '<run_id>'
GROUP BY rule_id, rule_name, disposition
ORDER BY record_count DESC, rule_id;
```

The `raw_record` column retains a JSON snapshot for forensic inspection. Treat
it as restricted: it is for pipeline troubleshooting, not for AI consumption.

## Known limitations

- The data is synthetic mock banking data only; it is not suitable for real
  customer, production, or regulated decision-making use.
- The pipeline is operated manually through notebooks in Databricks Free Edition;
  it does not provide production scheduling, alerting, or operational support.
- Free Edition capacity can make the full two-million-transaction mock baseline
  slow. The generator defaults to a smaller 200,000-transaction development run.
- The notebooks only accept `g3_dev`, `g3_test`, or `g3_catalog` as catalog
  widget values unless the source code is changed.
- Quarantine records are retained for auditability. The pipeline detects and
  isolates failures; it does not automatically correct source data.

## Troubleshooting notes

| Symptom | Action |
|---|---|
| `ModuleNotFoundError` for `mock` or `pipeline` | Open and run the notebook from the cloned repository root. Do not move individual `.py` files outside their folders. |
| Catalog validation or permission error | Confirm the same supported catalog is selected everywhere. Create or obtain access to `g3_dev` (or use `g3_test`) in Catalog Explorer. |
| Bronze finds no source files | Run `generate_mock_databricks.py` first and confirm the files exist under `/Volumes/<catalog>/bronze/raw_data/`. Check that both notebooks use the same catalog. |
| Silver reports a snapshot mismatch | Run `bronze_all_tables.py` to completion, then rerun the three DQ notebooks before running `silver_all_tables.py`. |
| DQ registry or quarantine table is missing | Run `dq_01_setup.py`, `dq_02_load_dq_rules.py`, and `dq_03_failures_all_rules.py` in that order. |
| Generator stops after installing Faker | The first cell intentionally restarts Python. After it reconnects, select **Run all** again. |
| Validation raises an exception | Read the failing check and inspect the affected Bronze, Silver, or quarantine table. Correct the upstream issue, then rerun from the affected layer onward using the same catalog. |

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
