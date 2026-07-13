# Bronze Auto Loader

Run `generate_mock_databricks.py` first, then run each
`pipeline/bronze/bronze_<table>.py` notebook with the same catalog selected.
Each notebook processes all unseen files and stops using an AvailableNow
trigger.

## Paths

- Source: `/Volumes/<catalog>/bronze/raw_data/<table>/*.csv`
- Target: `<catalog>.bronze.<table>`
- Schema state: `/Volumes/<catalog>/bronze/autoloader_state/<table>/schema`
- Checkpoint: `/Volumes/<catalog>/bronze/autoloader_state/<table>/checkpoint`

The checkpoint makes reruns file-idempotent. New numbered snapshot files are
appended in full; Bronze intentionally does not perform row-level
deduplication.

## First DEV run

Legacy loaders used obsolete flat paths. Before the first Auto Loader run in
DEV, execute `pipeline/bronze/reset_bronze_autoloader_dev.py`, set
`confirm_reset` to `RESET g3_dev`, and run it once. This drops only DEV
Bronze target tables and their Auto Loader state; raw source files are retained.

After all 27 loaders finish, run
`pipeline/validation/validate_m1_bronze.py`.
