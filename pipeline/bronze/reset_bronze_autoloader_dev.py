# Databricks notebook source
dbutils.widgets.text("confirm_reset", "RESET g3_dev")

# COMMAND ----------

"""One-time guarded reset for a clean DEV Auto Loader migration."""

from pipeline.bronze.table_registry import ALL_TABLE_CONFIGS


dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev"])
dbutils.widgets.text("confirm_reset", "")

catalog = dbutils.widgets.get("catalog")
confirmation = dbutils.widgets.get("confirm_reset")
if catalog != "g3_dev":
    raise ValueError("This destructive bootstrap is restricted to g3_dev")
if confirmation != "RESET g3_dev":
    raise ValueError('Set confirm_reset exactly to "RESET g3_dev" to continue')

for table_name in ALL_TABLE_CONFIGS:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.{table_name}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.autoloader_state")
for table_name in ALL_TABLE_CONFIGS:
    dbutils.fs.rm(
        f"/Volumes/{catalog}/bronze/autoloader_state/{table_name}",
        recurse=True,
    )

print(
    f"Reset {len(ALL_TABLE_CONFIGS)} Bronze tables and Auto Loader state in {catalog}"
)
