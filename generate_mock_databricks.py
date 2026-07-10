# Databricks notebook source
# MAGIC %md
# MAGIC # Mock data generator (Databricks)
# MAGIC
# MAGIC Runs the existing local `mock/` Faker generator **inside Databricks**, so the mock
# MAGIC source CSVs are produced in the same environment that ingests them — no local Python
# MAGIC run, no manual upload. Bronze is untouched: it already reads the flat CSVs this
# MAGIC notebook writes to `/Volumes/{catalog}/bronze/raw_data/`.
# MAGIC
# MAGIC This calls the *same* generator with the *same* seed as `python -m mock.generate`,
# MAGIC so the output is byte-identical to a local run and bronze's `_record_hash` dedup
# MAGIC treats a re-run as a no-op.
# MAGIC
# MAGIC Design: `docs/superpowers/specs/2026-07-10-mock-databricks-notebook-design.md`

# COMMAND ----------
# MAGIC %pip install "faker>=24.0.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configure & run
# MAGIC Catalog dropdown follows the team convention (DEV=`g3_dev`, TEST=`g3_test`,
# MAGIC PROD=`g3_catalog`). Defaults are DEV-friendly; set `transactions=2000000` for the
# MAGIC full assignment baseline.

# COMMAND ----------
# Catalog dropdown — per G3_Workflow.md
dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
dbutils.widgets.text("transactions", "200000")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("snapshots", "1")
dbutils.widgets.text("defect_rate", "0.05")

catalog = dbutils.widgets.get("catalog")
transactions = int(dbutils.widgets.get("transactions"))
seed = int(dbutils.widgets.get("seed"))
snapshots = int(dbutils.widgets.get("snapshots"))
defect_rate = float(dbutils.widgets.get("defect_rate"))

OUT_DIR = f"/Volumes/{catalog}/bronze/raw_data"

# Ensure the UC schema + volume exist (bronze reads from `raw_data`).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_data")

# This notebook lives at the repo root, so `import mock.generate` resolves via the
# notebook's own directory on sys.path. If you move this file (e.g. under pipeline/),
# uncomment these two lines:
# import sys
# sys.path.insert(0, "/Workspace/Repos/<user>/Assignment-G3")
import mock.generate

argv = [
    "--out", OUT_DIR,
    "--transactions", str(transactions),
    "--seed", str(seed),
    "--snapshots", str(snapshots),
    "--defect-rate", str(defect_rate),
]
print(f"Generating mock data -> {OUT_DIR}  "
      f"(catalog={catalog}, transactions={transactions:,}, seed={seed}, snapshots={snapshots})")
rc = mock.generate.main(argv)
if rc:
    raise RuntimeError(f"mock.generate.main returned non-zero exit code: {rc}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify the data + injected defects
# MAGIC The run summary above already prints per-table `defects=N` and a total.
# MAGIC `defects_manifest.csv` is the **ground truth**: one row per intentionally-injected
# MAGIC bad row (`source_table, record_key, rule_id, rule_name, failure_reason, severity`).
# MAGIC Its row count = the number of DQ defects the Silver layer must catch & quarantine.

# COMMAND ----------
files = dbutils.fs.ls(OUT_DIR)
print(f"{len(files)} entries in {OUT_DIR}:")
for f in sorted(files, key=lambda x: x.name):
    print(f"  {f.name:<32} {f.size:>12,} bytes")

# defects_manifest.csv sits at the T0 dir (flat when snapshots=1; under snapshot_T0/ otherwise)
t0_dir = f"{OUT_DIR}/snapshot_T0" if snapshots >= 2 else OUT_DIR
manifest_path = f"{t0_dir}/defects_manifest.csv"

defects_df = spark.read.option("header", True).csv(manifest_path)
total = defects_df.count()
print(f"\nInjected defects (defects_manifest.csv): {total:,}")

print("\nDefects by table + rule:")
display(defects_df.groupBy("source_table", "rule_id", "rule_name").count()
        .orderBy("source_table", "rule_id"))

print("\nSample injected defects:")
display(defects_df.select("source_table", "record_key", "rule_id", "failure_reason").limit(20))
