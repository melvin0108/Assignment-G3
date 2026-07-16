# Databricks notebook source
# MAGIC %md
# MAGIC # Mock data generator (Databricks)
# MAGIC
# MAGIC Runs the existing local `mock/` Faker generator **inside Databricks**, so the mock
# MAGIC source CSVs are produced in the same environment that ingests them — no local Python
# MAGIC run, no manual upload. Every run is retained as a numbered file inside its table
# MAGIC folder, for example `raw_data/customers/customer01.csv`, then `customer02.csv`.
# MAGIC
# MAGIC This calls the *same* generator with the *same* seed as `python -m mock.generate`,
# MAGIC Files are generated in a private staging folder and published only after generation
# MAGIC succeeds, avoiding partially-written source files.
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
# MAGIC full assignment baseline. `transactions`, `seed`, and `defect_rate` create batch 1;
# MAGIC later runs derive from the previous batch and use `scd_rate` for dimension changes.

# COMMAND ----------
# Catalog dropdown — per G3_Workflow.md
dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
dbutils.widgets.text("transactions", "200000")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("defect_rate", "0.05")
dbutils.widgets.text("scd_rate", "0.02")

catalog = dbutils.widgets.get("catalog")
transactions = int(dbutils.widgets.get("transactions"))
seed = int(dbutils.widgets.get("seed"))
defect_rate = float(dbutils.widgets.get("defect_rate"))
scd_rate = float(dbutils.widgets.get("scd_rate"))

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
import mock.scd
from mock import config as mock_config
from datetime import timedelta
import os
import re
import shutil
import uuid


def singular_name(table_name):
    """Return the readable file prefix used inside a table's folder."""
    if table_name.endswith("ies"):
        return table_name[:-3] + "y"
    if table_name.endswith(("ches", "shes", "xes", "zes")):
        return table_name[:-2]
    if table_name.endswith("s") and not table_name.endswith("ss"):
        return table_name[:-1]
    return table_name


def next_batch_number(root_dir):
    """Use one batch number for every table produced by this notebook run."""
    highest = 0
    if not os.path.isdir(root_dir):
        return 1
    for table in os.listdir(root_dir):
        table_dir = os.path.join(root_dir, table)
        if not os.path.isdir(table_dir) or table.startswith("_"):
            continue
        pattern = re.compile(
            rf"^{re.escape(singular_name(table))}(\d+)\.csv$"
        )
        for filename in os.listdir(table_dir):
            match = pattern.match(filename)
            if match:
                highest = max(highest, int(match.group(1)))
    return highest + 1


def publish_csvs(staging_dir, root_dir, batch_number):
    """Move staged flat CSVs into `<table>/<singular><NN>.csv`."""
    publish_plan = []
    for filename in sorted(os.listdir(staging_dir)):
        if not filename.endswith(".csv"):
            continue
        table = filename[:-4]
        table_dir = os.path.join(root_dir, table)
        os.makedirs(table_dir, exist_ok=True)
        target = os.path.join(
            table_dir, f"{singular_name(table)}{batch_number:02d}.csv"
        )
        if os.path.exists(target):
            raise FileExistsError(f"Refusing to overwrite existing batch file: {target}")
        publish_plan.append((os.path.join(staging_dir, filename), target))

    published = []
    for source, target in publish_plan:
        os.replace(source, target)
        published.append(target)
    return published


def batch_file(root_dir, table, batch_number):
    return os.path.join(
        root_dir, table, f"{singular_name(table)}{batch_number:02d}.csv"
    )


def restore_previous_batch(root_dir, destination, batch_number):
    """Rebuild the prior flat snapshot expected by mock.scd."""
    tables = list(mock_config.GENERATION_ORDER) + ["defects_manifest"]
    for table in tables:
        source = batch_file(root_dir, table, batch_number)
        if not os.path.exists(source):
            raise FileNotFoundError(
                f"Batch {batch_number:02d} is incomplete; missing {source}"
            )
        shutil.copy2(source, os.path.join(destination, f"{table}.csv"))


if not 0 <= scd_rate <= 1:
    raise ValueError("scd_rate must be between 0 and 1")

batch_number = next_batch_number(OUT_DIR)
work_dir = os.path.join(OUT_DIR, "_staging", uuid.uuid4().hex)
staging_dir = os.path.join(work_dir, "output")
previous_dir = os.path.join(work_dir, "previous")
os.makedirs(staging_dir, exist_ok=False)

print(f"Preparing batch {batch_number:02d} -> {OUT_DIR}/<table>/  "
      f"(catalog={catalog}, seed={seed}, scd_rate={scd_rate:.2%})")
try:
    if batch_number == 1:
        argv = [
            "--out", staging_dir,
            "--transactions", str(transactions),
            "--seed", str(seed),
            "--snapshots", "1",
            "--defect-rate", str(defect_rate),
        ]
        rc = mock.generate.main(argv)
        if rc:
            raise RuntimeError(f"mock.generate.main returned non-zero exit code: {rc}")
        mock.scd.ScdManifest().write(
            os.path.join(staging_dir, "scd_changes_manifest.csv")
        )
    else:
        os.makedirs(previous_dir, exist_ok=False)
        restore_previous_batch(OUT_DIR, previous_dir, batch_number - 1)
        prior_oracle = batch_file(
            OUT_DIR, "scd_changes_manifest", batch_number - 1
        )
        scd_manifest = mock.scd.ScdManifest.read(prior_oracle)
        as_of = mock_config.SNAPSHOT_BASE_DATE + timedelta(
            days=(batch_number - 1) * mock_config.SNAPSHOT_INTERVAL_DAYS
        )
        mutations = mock.scd.derive_snapshot(
            previous_dir,
            staging_dir,
            batch_number - 1,
            as_of,
            scd_rate,
            seed,
            scd_manifest,
        )
        scd_manifest.write(os.path.join(staging_dir, "scd_changes_manifest.csv"))
        print(f"Applied {mutations:,} SCD2 changes as of {as_of.isoformat()}")

    corrected_links, removed_stale_defects = (
        mock.scd.repair_transaction_account_links(staging_dir)
    )
    print(
        f"Normalized {corrected_links:,} transaction card/account links; "
        f"removed {removed_stale_defects:,} stale orphan manifest rows"
    )

    published_files = publish_csvs(staging_dir, OUT_DIR, batch_number)
finally:
    shutil.rmtree(work_dir, ignore_errors=True)

print(f"Published {len(published_files)} files for batch {batch_number:02d}:")
for path in published_files:
    print(f"  {path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify the data + injected defects
# MAGIC The run summary above already prints per-table `defects=N` and a total.
# MAGIC `defects_manifest.csv` is the **ground truth**: one row per intentionally-injected
# MAGIC bad row (`source_table, record_key, rule_id, rule_name, failure_reason, severity`).
# MAGIC Its row count = the number of DQ defects the Silver layer must catch & quarantine.
# MAGIC From batch 2 onward, customers/cards/merchants contain legitimate SCD2 changes and
# MAGIC `scd_changes_manifestNN.csv` is the cumulative oracle for those changes.

# COMMAND ----------
files = [f for f in dbutils.fs.ls(OUT_DIR) if not f.name.startswith("_")]
print(f"{len(files)} table folders in {OUT_DIR}:")
for f in sorted(files, key=lambda x: x.name):
    batch_files = dbutils.fs.ls(f.path)
    print(f"  {f.name:<32} {len(batch_files):>4} batch file(s)")

manifest_path = (
    f"{OUT_DIR}/defects_manifest/"
    f"defects_manifest{batch_number:02d}.csv"
)

defects_df = spark.read.option("header", True).csv(manifest_path)
total = defects_df.count()
print(f"\nInjected defects (defects_manifest.csv): {total:,}")

print("\nDefects by table + rule:")
display(defects_df.groupBy("source_table", "rule_id", "rule_name").count()
        .orderBy("source_table", "rule_id"))

print("\nSample injected defects:")
display(defects_df.select("source_table", "record_key", "rule_id", "failure_reason").limit(20))

if batch_number >= 2:
    previous_customer_path = batch_file(OUT_DIR, "customers", batch_number - 1)
    current_customer_path = batch_file(OUT_DIR, "customers", batch_number)
    demo_history = (
        spark.read.option("header", True).csv(previous_customer_path)
        .unionByName(spark.read.option("header", True).csv(current_customer_path))
        .filter("customer_id = 'CUST-0001'")
        .select("customer_id", "address", "effective_at")
        .orderBy("effective_at")
    )
    print("\nSCD2 demonstration for CUST-0001:")
    display(demo_history)
