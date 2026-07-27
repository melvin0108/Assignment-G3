# Assignment-G3: AI-ready Transaction Investigation Context Pipeline

This repository is a Databricks Free Edition prototype that turns synthetic
banking data into governed, AI-ready transaction-investigation context. It
creates mock CSV files, ingests them to Bronze, applies data-quality (DQ)
checks and quarantine handling, transforms protected Silver tables, and builds
Gold models including `investigation_context`.

Use one of these operating paths:

1. **Databricks UI** — best for a first run and for seeing notebook output.
2. **Databricks CLI** — triggers and monitors the same two persistent jobs
   after their one-time workspace setup.

The data is synthetic only. Do not use real customer, account, or NAB data.

## Repository guide

| Location | What it contains |
|---|---|
| `mock/` and `generate_mock_databricks.py` | Deterministic synthetic source-data generator and intentional defects. |
| `pipeline/bronze/` | CSV ingestion to raw Bronze tables. |
| `pipeline/dq/` | DQ-rule registry and failed-record quarantine processing. |
| `pipeline/silver/` | Type casting, quarantine filtering, masking, and lineage transformations. |
| `pipeline/gold/` | Curated dimensions, facts, and the AI-ready `investigation_context` table. |
| `pipeline/validation/` | Executable M1, M2, and M3 validation notebooks. |
| `tests/` | Local source-contract tests. |
| `docs/` | Requirements, source contracts, data dictionary, and Gold-model contracts. |
| `pipeline/jobs.yaml` | Reference definition for the main Databricks job. Its notebook paths are workspace-specific, so do not deploy it unchanged to another user’s workspace. |
| `deliverables/` | Submission-ready evidence and documentation samples. |

## Deliverables and evidence

The following supporting deliverables provide the implementation and evidence
behind this runbook:

| Deliverable | Description |
|---|---|
| [Mock data logic and data contract](deliverables/mock-data-logic-and-data-contract.md) | Source-dataset purposes, schemas, keys, relationships, sensitive-field classifications, and intentional quality defects. |
| [Data pipeline](deliverables/data_pipeline.md) | Pipeline lineage and implementation walkthrough from mock sources through the governed AI-ready output. |
| [Data quality evidence](deliverables/data-quality-evidence.md) | Validation summary, quarantine evidence, sample failures, and the documented handling of non-blocking warnings. |
| [AI-ready context output](deliverables/ai-ready-context-output.md) | Approved retrieval output, access boundaries, metadata, traceability, and safe-use guidance. |

## Prerequisites

1. A Databricks Free Edition account and workspace with Serverless compute.
2. Access to this GitHub repository:
   `https://github.com/melvin0108/Assignment-G3`.
3. Permission to use one of the supported Unity Catalog names: `g3_dev`,
   `g3_test`, or `g3_catalog`. Use `g3_dev` for a normal development run.
4. For the CLI option only: Databricks CLI v0.292.0 or later and a browser to
   complete OAuth sign-in. On Windows, install the current CLI with:

   ```powershell
   winget install Databricks.DatabricksCLI
   databricks --version
   ```

> Every notebook validates the `catalog` parameter. Do not substitute an
> arbitrary catalog name; use `g3_dev`, `g3_test`, or `g3_catalog` consistently
> throughout one run.

## One-time workspace setup

Complete these steps once for each Databricks workspace and repository branch.

1. In Databricks, open **Workspace** and create a **Git folder**. Connect
   GitHub when prompted, clone the repository URL above, and select the branch
   to run.
2. Keep the cloned directory structure unchanged. Parent notebooks import
   sibling Python modules and call child notebooks with relative paths.
3. In **Catalog**, create or select `g3_dev`. The notebooks create the Bronze,
   Silver, Gold, and governance schemas and required volumes within that
   catalog.
4. The **DQ Rules Setup** job publishes the synthetic source batch as its first
   task. Do not run `generate_mock_databricks.py` separately during the normal
   job-based workflow.

### Persistent jobs used by this project

The workspace uses these job names:

| Job | When to run it | Purpose |
|---|---|---|
| `DQ Rules Setup` | Once for the initial run in a catalog, and again only when DQ rules change or a new catalog is bootstrapped. | Generates mock data, bootstraps Bronze/M1, then creates the DQ structures and loads the enabled rule registry. |
| `AI-ready transaction investigation context pipeline` | After DQ Rules Setup has succeeded; run again for each ordinary pipeline execution. | Runs the Bronze, DQ, Silver, Gold, and validation workflow to produce the AI-ready context. |

For a new workspace, configure each job in **Jobs & Pipelines** to run
notebooks from **your cloned Git-folder path**, not the path embedded in
`pipeline/jobs.yaml`. Configure the one-time DQ setup job with this order so it
can bootstrap the initial Bronze data required by the rule registry:

| Order | DQ-setup job task notebook |
|---:|---|
| 1 | `generate_mock_databricks.py` |
| 2 | `pipeline/bronze/bronze_all_tables.py` |
| 3 | `pipeline/validation/validate_m1_bronze.py` |
| 4 | `pipeline/dq/dq_01_setup.py` |
| 5 | `pipeline/dq/dq_02_load_dq_rules.py` |

The main pipeline job must run `pipeline/dq/dq_03_failures_all_rules.py` after
Bronze/M1 and before any Silver task. This rule evaluation is per source batch;
it is not replaced by the one-time rule-registry setup.

Configure the main job with the following dependency order. Pass the job
parameter `catalog` to every task.

| Order | Main-job task notebook |
|---:|---|
| 1 | `pipeline/bronze/bronze_all_tables.py` |
| 2 | `pipeline/validation/validate_m1_bronze.py` |
| 3 | `pipeline/dq/dq_03_failures_all_rules.py` |
| 4 | `pipeline/silver/silver_all_tables.py` |
| 5 | `pipeline/validation/validate_m2_dq.py` |
| 6 | `pipeline/validation/validate_m2_silver.py` |
| 7 | `pipeline/gold/gold_all_tables.py` |
| 8 | `pipeline/validation/validate_m3_gold.py` |

For the **first** job-based execution, run **DQ Rules Setup** from **Jobs &
Pipelines**, then run the main pipeline job from the same page. Later runs can
start the main job directly because its third task evaluates DQ failures for the
Bronze batch.

If the two jobs are not already available, select **Jobs & Pipelines** >
**Job**, add the tasks in the two tables above, set `catalog` as a job
parameter, and use Serverless compute. The repository does not contain a
portable bundle configuration that can create workspace-specific jobs
automatically.

## Option 1: Run from the Databricks UI

### Primary path: run from Jobs & Pipelines

1. Go to **Jobs & Pipelines**.
2. For the first run only, open **DQ Rules Setup**, select **Run now**, set
   `catalog` to `g3_dev`, and wait for a successful run. This job generates the
   mock data, completes Bronze/M1, and loads the DQ rule registry.
3. Open **AI-ready transaction investigation context pipeline**.
4. Select **Run now**, set the job parameter `catalog` to `g3_dev`, and start
   the job.
5. Open the run details and confirm that every task succeeds. Preserve the M1,
   M2, and M3 validation output as execution evidence.

The supplied screenshot shows where both persistent jobs appear: **Jobs &
Pipelines**. A green tick means the displayed run completed; open the run for
task-level results rather than relying on the list view alone.

### Last resort: run individual notebooks

Use this path only when a job cannot be configured or to isolate a failed
stage. The normal UI path is to run the two persistent jobs from **Jobs &
Pipelines**. For every notebook, select the same `catalog` value and wait for
completion before starting the next one.

| Order | Notebook | Expected result |
|---:|---|---|
| 1 | `generate_mock_databricks.py` | CSV source batch and `defects_manifest` under `/Volumes/<catalog>/bronze/raw_data/`. |
| 2 | `pipeline/bronze/bronze_all_tables.py` | Raw source tables in `<catalog>.bronze`. |
| 3 | `pipeline/validation/validate_m1_bronze.py` | `PASS: M1 Bronze validation completed with no blocking failures.` |
| 4 | `pipeline/dq/dq_01_setup.py` | Governance and quarantine table structures exist. |
| 5 | `pipeline/dq/dq_02_load_dq_rules.py` | Enabled rules are stored in `<catalog>.gov.dq_rules`. |
| 6 | `pipeline/dq/dq_03_failures_all_rules.py` | Current-run failed records are written to quarantine. |
| 7 | `pipeline/silver/silver_all_tables.py` | Clean, typed, protected Silver tables are created. |
| 8 | `pipeline/validation/validate_m2_dq.py` | Blocking DQ checks pass; precision/recall differences can be warnings. |
| 9 | `pipeline/validation/validate_m2_silver.py` | `PASS: M2 Silver validation completed with no blocking failures.` |
| 10 | `pipeline/gold/gold_all_tables.py` | Curated Gold models and investigation context are created. |
| 11 | `pipeline/validation/validate_m3_gold.py` | M3 Gold validation passes. |

For a `.py` notebook, open it from the Git folder; Databricks recognises the
`# Databricks notebook source` format. Select **Run all**. Do not copy an
orchestration file outside the repository before running it.

`bronze_all_tables.py`, `silver_all_tables.py`, and `gold_all_tables.py` are
the normal layer entry points. They call their child notebooks in the required
dependency order; do not also run every child notebook manually.

## Option 2: Run the same jobs with the Databricks CLI

This option uses the same configured workspace jobs as Option 1. It does not
upload notebooks or create a new job definition. Complete the one-time
workspace setup first.

### Authenticate

1. Run `databricks auth profiles` to view existing profiles and choose the
   profile for the target workspace.
2. If no appropriate profile exists, authenticate with OAuth. Replace the
   placeholders; choose a descriptive profile name rather than `DEFAULT`.

   ```powershell
   databricks auth login --host <workspace-url> --profile <profile-name>
   databricks auth profiles
   ```

3. In every command below, replace `<profile-name>` with the selected profile.

### Find the job IDs

Job IDs are unique to each workspace. Retrieve them by their exact names:

```powershell
databricks jobs list --name "DQ Rules Setup" --profile <profile-name> --output json
databricks jobs list --name "AI-ready transaction investigation context pipeline" --profile <profile-name> --output json
```

Copy the returned `job_id` values. In the following commands, use
`<dq-job-id>` and `<pipeline-job-id>` rather than an ID from another workspace.

### Trigger and monitor a run

For the first run in the catalog, run the DQ setup job once:

```powershell
databricks jobs run-now <dq-job-id> --profile <profile-name> --output json
```

Record the returned `run_id`, then inspect it until it reaches a terminal
state:

```powershell
databricks jobs get-run <dq-run-id> --profile <profile-name> --output json
```

After the DQ setup run succeeds, trigger the main pipeline job. The request
passes the supported catalog to the job as a parameter.

```powershell
databricks jobs run-now <pipeline-job-id> --profile <profile-name> --json '{"job_parameters":{"catalog":"g3_dev"}}' --output json
databricks jobs get-run <pipeline-run-id> --profile <profile-name> --output json
```

The `run-now` command waits for completion by default. Add `--no-wait` only
when you intend to monitor the returned run ID separately. A run is successful
only when its lifecycle state is terminal and its result state is `SUCCESS`.

## Validation and local tests

The Databricks validation notebooks are part of the execution order. They are
the main acceptance evidence:

| Check | Run after | Confirms |
|---|---|---|
| `validate_m1_bronze.py` | Bronze | Bronze tables, required metadata, lineage, contracts, and source checks. |
| `validate_m2_dq.py` | DQ and Silver | Rule registry, quarantine records, and DQ-to-Silver handling. |
| `validate_m2_silver.py` | Silver | Schema, integrity, business rules, masking, lineage, and Gold inputs. |
| `validate_m3_gold.py` | Gold | Gold contracts, grains, AI policy, metadata, and relationships. |

Each validation notebook raises an exception for a blocking failure. Save its
completed output. `validate_m2_dq.py` also prints a JSON summary with check
counts, warnings, quarantined-record totals, and sample failures.

You can also run the repository’s local source-contract tests. These do not run
the Databricks pipeline; they validate the local implementation and mock-data
contracts.

```powershell
python -m pip install -r requirements.txt
python -m pytest tests
```

## Outputs and inspection queries

Replace `<catalog>` with the catalog used for the run.

| Output | Location |
|---|---|
| Mock source CSVs | `/Volumes/<catalog>/bronze/raw_data/<table>/` |
| Raw ingested data | `<catalog>.bronze.*` |
| DQ rule registry | `<catalog>.gov.dq_rules` |
| Quarantine records | `<catalog>.silver.quarantine_records` |
| Clean and masked tables | `<catalog>.silver.*` |
| Masking policy and lineage | `<catalog>.gov.masking_policies`, `<catalog>.gov.metadata_lineage` |
| Curated models | `<catalog>.gold.*` |
| AI-ready output | `<catalog>.gold.investigation_context` |

Run the following in a Databricks SQL editor or notebook cell to inspect recent
quarantined records. `raw_record` is restricted troubleshooting data and must
not be supplied to an AI consumer.

```sql
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

`<catalog>.gold.investigation_context` is the intended AI-retrieval output. It
includes quality status, warning flags, source references, refresh metadata,
and usage restrictions. It must not be replaced with raw Bronze or restricted
quarantine data.

## Rerun from a clean state

For a fully isolated rerun, select an unused supported catalog such as
`g3_test`, run **DQ Rules Setup** once with that catalog, then run the main
pipeline job with the same value. This produces a fresh demonstration without
mixing previous tables or source batches.

A normal rerun in the same catalog starts the main pipeline job again. If the
DQ rules change, run **DQ Rules Setup** again before the main pipeline job.

## Limitations and troubleshooting

| Situation | What to do |
|---|---|
| `ModuleNotFoundError` for `mock` or `pipeline` | Run the notebook from its cloned Git folder; do not move individual files. |
| Catalog validation or permission error | Use the same supported catalog in every task and confirm access in Catalog Explorer. |
| Bronze finds no source files | Confirm that **DQ Rules Setup** completed, then inspect `/Volumes/<catalog>/bronze/raw_data/`. In the manual fallback only, rerun `generate_mock_databricks.py`. |
| `dq_02_load_dq_rules.py` cannot find `bronze.defects_manifest` | The Bronze/M1 tasks in **DQ Rules Setup** did not complete. Fix that job run, then rerun it. |
| DQ registry or quarantine table missing | Rerun **DQ Rules Setup** to create/load the registry. The main job creates current-batch quarantine records in its DQ task. |
| Silver reports a snapshot mismatch | Rerun the main pipeline job. It runs Bronze, the current-batch DQ evaluation, then Silver in order. |
| The generator stops after Faker installation | In the job flow, inspect the generator task and wait for its restart to finish. In the manual fallback, reconnect and select **Run all** again. |
| A job has the author’s workspace path | Update every task to the reader’s cloned Git-folder path before running it. |
| CLI reports `cannot configure default credentials` | Authenticate with `databricks auth login --host <workspace-url> --profile <profile-name>` and use that `--profile` on every command. |
| Validation raises an exception | Inspect the failed check, correct the upstream stage, then rerun from that stage onward using the same catalog. |

Known limits: this is a manual, synthetic-data prototype. Databricks Free
Edition capacity can make the 200,000-transaction development baseline slow;
it has no production scheduler, alerting, or operational-support model.

## Completion checklist

- Mock source data and `defects_manifest` have been published.
- M1 Bronze validation passes.
- The DQ rule registry is loaded and the current batch has quarantine results.
- M2 DQ and M2 Silver validations pass their blocking checks.
- M3 Gold validation passes.
- `<catalog>.gold.investigation_context` exists and contains only approved,
  AI-allowed context.

For mock-generator row counts, defect rates, and source contracts, see
[mock/README.md](mock/README.md).
