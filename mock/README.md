# Mock Data Generator (Transaction Investigation)

Generates the Bronze-layer source CSVs defined in `docs/data-model.md` and
`docs/bronze-layer.md`, with **intentional data-quality defects** baked in for
the Zero-Trust-AI pipeline to catch. Deterministic (seeded) and streaming-safe
so `transactions` can scale to millions of rows.

## Install

```bash
pip install -r requirements.txt   # just: Faker
```

## Run on Databricks (no local step)

Instead of running locally and uploading CSVs, generate straight into the bronze landing
Volume from a Databricks notebook. It calls the *same* `mock/` generator with the same
seed, so the output is byte-identical to a local run and bronze's `_record_hash` dedup
treats a re-run as a no-op.

1. In Databricks Repos, open `generate_mock_databricks.py` (repo root) on your personal
   branch and **Pull**.
2. **Run All** — the first cell installs Faker and restarts Python.
3. Pick the catalog from the dropdown (`g3_dev` DEV / `g3_test` TEST / `g3_catalog` PROD)
   and set `transactions` (default `200000`; `2000000` for the full baseline).

CSVs (including `defects_manifest.csv`) are written flat to
`/Volumes/{catalog}/bronze/raw_data/` — exactly where the bronze notebooks read.
Design/detail: `docs/superpowers/specs/2026-07-10-mock-databricks-notebook-design.md`.

## Usage

```bash
# default assignment baseline (transactions=2,000,000)
python -m mock.generate

# fast demo / test run
python -m mock.generate --transactions 2000 --customers 150

# scale down for a smaller local sample
python -m mock.generate --scale 0.25

# explicit stress test (~2,000,000 transactions, streaming)
python -m mock.generate --stress

# generate only some tables
python -m mock.generate --tables customers,accounts,transactions

# heavier defect density (e.g. 15%)
python -m mock.generate --defect-rate 0.15

# reproducibility
python -m mock.generate --seed 42 --out data/raw
```

### Arguments
| arg | default | meaning |
|---|---|---|
| `--seed` | `42` | RNG seed; same seed → identical data + defects |
| `--out` | `data/raw` | output directory (one CSV per table) |
| `--customers` | `5000` | override customer count (drives accounts/cards) |
| `--transactions` | `2000000` | override transaction count (drives facts) |
| `--scale` | `1.0` | multiplier on base volumes |
| `--defect-rate` | `0.05` | fraction of eligible rows that get a defect |
| `--stress` | off | explicitly sets `transactions=2,000,000` |
| `--tables` | all | comma-separated subset to generate |
| `--no-manifest` | off | skip the defects manifest |

## Output
- `<table>.csv` for each of the 25 tables (column order matches the contracts).
- `defects_manifest.csv` — **every intentionally-injected bad record** with
  `source_table, record_key, rule_id, rule_name, failure_reason, severity`. Use
  it to validate the Silver `quarantine_records` output (expected vs actual).

## How defects are injected
1. **Parents first**: customers → accounts/cards → transactions → disputes/cases
   → bridges/notes. Clean rows are generated so foreign keys resolve by default.
2. **Defect injection**: each generator then mutates/inserts a controlled number
   of rows (driven by `--defect-rate`) to reproduce the documented defects, e.g.
   duplicate `transaction_id`, negative `amount`, orphan FKs, future timestamps,
   raw PAN requiring downstream masking, PII in free text, `legal_hold` cases, DNC violations, enum
   casing. Every injected row is logged to the manifest.

Defect → rule mapping is defined inline in `mock/generators.py` (rule ids match
the brief's DQ requirements and `docs/data-model.md` §7).

## Architecture
```
mock/
  config.py     # enums, ID prefixes, table schemas, generation order, volumes
  helpers.py    # Faker/RNG setup + value helpers (full_pan, tax_id, ts, ...)
  defects.py    # DefectManifest (writes defects_manifest.csv)
  generators.py # one generator per table + the GENERATORS registry
  generate.py   # CLI: argparse, count derivation, CSV streaming, summary
```

## Uploading to Databricks
Generate locally to `data/raw/`, then copy into the Unity Catalog landing Volume:
```
/Volumes/tx_inv/landing/<table>/
```
and ingest with `COPY INTO` / Auto Loader / DLT as described in
`docs/bronze-layer.md` §5.
