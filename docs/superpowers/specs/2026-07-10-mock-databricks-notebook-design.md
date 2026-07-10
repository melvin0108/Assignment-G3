# Design: Mock generation in a Databricks notebook

- **Date:** 2026-07-10
- **Status:** Approved — approach A (lift into notebook)
- **Scope:** Transaction-Investigation mock source data (`mock/`)

## Problem

The mock generator runs only locally (`python -m mock.generate` → `data/raw/`); the CSVs
are then uploaded to a Unity Catalog Volume for bronze to ingest. Goal: produce the mock
source data **inside Databricks** so generation and ingestion share one environment — no
local Python step, no manual upload. Bronze must remain untouched.

## Decision

Run the existing local `mock/` generator from a Databricks notebook that calls
`mock.generate.main(argv)`. The entry point already accepts an optional argv list
(`mock/generate.py:89`), so this requires **zero changes to `mock/`**. The local package
stays the canonical, graded generator; the notebook is an additive runner.

Because the notebook runs the *same code* with the *same seed*, re-running it produces
byte-identical output, so bronze's `_record_hash` dedup (sha256 over the source columns)
makes a re-run a no-op — no duplicates. Cross-environment identity (local↔Databricks)
additionally requires the **same Faker version**: `requirements.txt` only floors it
(`>=24.0.0`), so when first switching generators, regenerate from a clean bronze state
(or pin the exact Faker version) rather than mixing local- and Databricks-generated loads.

## The notebook

New file `generate_mock_databricks.py` at the **repo root**. Placed at root so
`import mock.generate` resolves via the notebook's directory on `sys.path`; a commented
`sys.path.insert(...)` covers relocating it (e.g. under `pipeline/mock/`).

1. `%pip install faker` + `dbutils.library.restartPython()` — Faker is the only dependency.
2. Widgets (team convention, `G3_Workflow.md`): `catalog` dropdown
   (`g3_dev` default / `g3_test` / `g3_catalog`), `transactions` (default `200000`;
   `2000000` for the full baseline), `seed` (`42`), `snapshots` (`1`), `defect_rate`
   (`0.05`).
3. `CREATE SCHEMA/VOLUME IF NOT EXISTS {catalog}.bronze.raw_data`, then call
   `mock.generate.main([...])` with `--out /Volumes/{catalog}/bronze/raw_data`.
4. Lists the written files for a quick sanity check.

## Contracts preserved (verified against bronze)

The bronze notebooks do **not** use `COPY INTO` or the `tx_inv/landing/` path described in
`docs/bronze-layer.md`. They read flat CSVs (`bronze_*.py`):

- **Path:** `/Volumes/{catalog}/bronze/raw_data/<table>.csv` — a single `raw_data/` volume,
  one file per table.
- **Format:** `spark.read.format("csv")` with `header=true`, `inferSchema=false`
  (every column cast to STRING), comma delimiter, default quoting.
- **Filename:** must not start with `_` (Databricks hides those → silent 0-row reads).
- **`defects_manifest.csv`** (6 cols: `source_table, record_key, rule_id, rule_name,
  failure_reason, severity`) read flat from the same directory.

All 35 rule_ids, the authoritative one-defect-per-row manifest discipline, seed-42
determinism, and pinned `RUN_DATE = 2026-07-06` carry through unchanged because the
generator code is unchanged.

## Defaults / assumptions

- Local `mock/` is **kept** (not replaced/deleted); `python -m mock.generate` still works.
- `snapshots` defaults to `1` (flat layout = what bronze expects). `≥2` is opt-in SCD2 and
  writes the nested `snapshot_T*/` layout, which diverges from bronze's flat path —
  exploration only.
- 2M transactions runs single-threaded on the driver (accepted trade-off of approach A).
  The default widget is smaller for DEV iteration.

## Out of scope

- Spark / `pandas_udf` rewrite (approaches B/C — deferred).
- Any change to `mock/`, bronze, or silver.
- Wiring catalog widgets into the bronze/silver notebooks (separate, already-planned
  refactor).

## Net change

- **New:** `generate_mock_databricks.py` (repo root); "Run on Databricks" section in
  `mock/README.md`.
- **Modified:** none.
