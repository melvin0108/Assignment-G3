# Mock Data Generator

This package creates deterministic, synthetic Transaction Investigation source
data for the pipeline. It produces valid and intentionally invalid records so
the Bronze, DQ, Silver, and Gold layers can demonstrate ingestion, quarantine,
masking, lineage, and AI-ready context generation.

Use the [root runbook](../README.md) for the complete Databricks Free Edition
pipeline. This document explains the mock generator only.

## Databricks Free Edition workflow

Run `generate_mock_databricks.py` from the repository root as the first notebook
in the pipeline.

1. Open the repository Git folder in Databricks Free Edition.
2. Open `generate_mock_databricks.py`.
3. Set the widgets:
   - `catalog`: `g3_dev` for a development run, or `g3_test` for an isolated run.
   - `transactions`: `200000` by default; set `2000000` for the full baseline.
   - `seed`: `42` by default.
   - `defect_rate`: `0.05` by default.
   - `scd_rate`: `0.02` by default for later batches.
4. Select **Run all**. The first cell installs Faker and restarts Python; after
   the restart, select **Run all** again if necessary.

The notebook creates `<catalog>.bronze` and the
`<catalog>.bronze.raw_data` volume when they do not already exist.

## Databricks output layout

The generator stages output privately, then publishes it only after successful
generation. Files are stored per source table and batch:

```text
/Volumes/<catalog>/bronze/raw_data/
  customers/customer01.csv
  transactions/transaction01.csv
  defects_manifest/defects_manifest01.csv
  scd_changes_manifest/scd_changes_manifest01.csv
```

Each new generator run receives the next common batch number (`02`, `03`, and
so on). `pipeline/bronze/bronze_all_tables.py` reads these folders directly and
ingests the source files that have not already been processed.

The first batch is a complete baseline snapshot. From batch 2 onward, the
generator derives a new snapshot from the prior batch and applies legitimate
SCD Type 2 changes to `customers`, `cards`, and `merchants`. The cumulative
`scd_changes_manifestNN.csv` records these expected changes. These SCD changes
are not data-quality defects.

## Generated files

Each batch contains 25 source-table CSV files plus two control files:

- `<table>NN.csv`: one CSV for each source table, using the schema and column
  order defined in `mock/config.py`.
- `defects_manifestNN.csv`: one row for every intentionally injected defect:
  `source_table`, `record_key`, `rule_id`, `rule_name`, `failure_reason`, and
  `severity`.
- `scd_changes_manifestNN.csv`: the expected SCD Type 2 changes across batches.

`defects_manifest` is the DQ ground truth. Compare it with
`<catalog>.silver.quarantine_records` after the DQ and Silver stages to assess
whether injected failures were detected and quarantined.

## Local development usage (optional)

The project demo runs the generator in Databricks. Local commands are useful
only for development or inspecting a small sample. Install dependencies first:

```bash
pip install -r requirements.txt
```

Run commands from the repository root:

```bash
# Full baseline: 2,000,000 transactions and 5,000 customers
python -m mock.generate

# Small development sample
python -m mock.generate --transactions 2000 --customers 150 --out data/raw

# Change defect density
python -m mock.generate --defect-rate 0.15 --out data/raw

# Produce two local SCD snapshots and their change manifest
python -m mock.generate --snapshots 2 --transactions 2000 --customers 150 --out data/raw
```

With one snapshot (the default), files are written flat under `--out`. With two
or more snapshots, complete datasets are written under `snapshot_T0/`,
`snapshot_T1/`, and so on; `scd_changes_manifest.csv` is written at the output
root.

## Configuration options

| Option | Default | Meaning |
|---|---:|---|
| `--seed` | `42` | Random seed. The same inputs and seed produce the same local dataset. |
| `--out` | `data/raw` | Local output directory. |
| `--customers` | base volume | Overrides the customer count and drives account/card volumes. |
| `--transactions` | base volume (`2000000`) | Overrides the transaction count and drives fact-table volumes. |
| `--scale` | `1.0` | Multiplies base volumes before explicit overrides are applied. |
| `--defect-rate` | `0.05` | Fraction of eligible rows that receive intentional defects; must be from `0` to `1`. |
| `--snapshots` | `1` | Number of local snapshots. Values of `2` or more enable SCD Type 2 output. |
| `--scd-rate` | `0.02` | Fraction of eligible SCD dimension keys changed in each later snapshot. |
| `--stress` | off | Sets transactions to `2000000` when `--transactions` was not supplied. |
| `--tables` | all | Comma-separated subset of source tables to generate. |
| `--no-manifest` | off | Does not write the defect or SCD manifests. |
| `--quiet` | off | Suppresses per-table progress output. |

## Defect model

Rows are generated in dependency order so foreign keys resolve by default:

```text
reference data -> customers/employees -> accounts/cards/merchants
-> transactions -> dependent facts -> investigation data
```

The generator then injects controlled defects, including missing fields,
invalid values, duplicate IDs, orphan foreign keys, future timestamps, invalid
status casing, out-of-range scores, raw PAN or PII in free text, legal-hold
records, and do-not-contact violations. Every injected defect is recorded in
`defects_manifest` with its DQ rule ID and reason.

The precise source schemas, generation order, reference values, volumes, and
SCD definitions are maintained in [config.py](config.py). The defect-to-rule
implementation is in [generators.py](generators.py).

## Package structure

```text
mock/
  config.py       schemas, generation order, volumes, enums, and SCD settings
  helpers.py      seeded Faker and value helpers
  defects.py      defect manifest writer
  generators.py   per-table generators and defect injection
  scd.py          snapshot derivation and SCD change manifest
  generate.py     local command-line entry point
```

## Notes and limitations

- All generated data is synthetic and must not be treated as real banking data.
- A Databricks rerun is a new numbered batch, not a byte-for-byte overwrite of
  the previous batch. Later batches intentionally model SCD changes.
- The full baseline can be resource-intensive in Databricks Free Edition; use
  the default 200,000 transactions for a quicker development demonstration.
