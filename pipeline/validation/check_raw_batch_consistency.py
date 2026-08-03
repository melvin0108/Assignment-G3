# Databricks notebook source
"""Check whether raw-data table folders contain consistent batch numbers."""

import re
from collections import Counter

from pyspark.sql import types as T


try:
    dbutils.widgets.get("catalog")
except Exception:
    dbutils.widgets.dropdown(
        "catalog",
        "g3_dev",
        ["g3_dev", "g3_test", "g3_catalog"],
    )

catalog = dbutils.widgets.get("catalog")
if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
    raise ValueError(f"Unsupported catalog: {catalog}")

raw_root = f"/Volumes/{catalog}/bronze/raw_data"
batch_pattern = re.compile(r"(\d+)\.csv$", re.IGNORECASE)


def list_table_batches(table_folder):
    """Return numbered CSV batches and unrecognized CSV names in one folder."""
    batches = set()
    invalid_files = []
    for file_info in dbutils.fs.ls(table_folder.path):
        file_name = file_info.name.rstrip("/")
        if not file_name.lower().endswith(".csv"):
            continue
        match = batch_pattern.search(file_name)
        if match:
            batches.add(int(match.group(1)))
        else:
            invalid_files.append(file_name)
    return batches, invalid_files


table_batches = {}
invalid_files_by_table = {}

for folder in dbutils.fs.ls(raw_root):
    table_name = folder.name.rstrip("/")
    if table_name.startswith("_") or not folder.path.endswith("/"):
        continue
    batches, invalid_files = list_table_batches(folder)
    table_batches[table_name] = batches
    invalid_files_by_table[table_name] = invalid_files

if not table_batches:
    raise ValueError(f"No table folders found under {raw_root}")

all_batches = set().union(*table_batches.values())
common_batches = set.intersection(*table_batches.values())
highest_batch = max(all_batches) if all_batches else None
layout_counts = Counter(
    tuple(sorted(batches)) for batches in table_batches.values()
)
expected_layout = max(
    layout_counts,
    key=lambda layout: (layout_counts[layout], len(layout), layout),
)
expected_batches = set(expected_layout)

rows = []
for table_name in sorted(table_batches):
    batches = table_batches[table_name]
    rows.append(
        (
            table_name,
            ", ".join(f"{batch:02d}" for batch in sorted(batches)),
            max(batches) if batches else None,
            ", ".join(
                f"{batch:02d}" for batch in sorted(expected_batches - batches)
            ),
            ", ".join(
                f"{batch:02d}" for batch in sorted(batches - expected_batches)
            ),
            ", ".join(invalid_files_by_table[table_name]),
        )
    )

result_schema = T.StructType(
    [
        T.StructField("table_folder", T.StringType(), False),
        T.StructField("batches", T.StringType(), False),
        T.StructField("latest_batch", T.IntegerType(), True),
        T.StructField("missing_expected_batches", T.StringType(), False),
        T.StructField("unexpected_batches", T.StringType(), False),
        T.StructField("unrecognized_csv_files", T.StringType(), False),
    ]
)

result_df = spark.createDataFrame(rows, result_schema)

print(f"Raw root: {raw_root}")
print(
    "Batches found anywhere: "
    + ", ".join(f"{batch:02d}" for batch in sorted(all_batches))
)
print(
    "Batches present in every folder: "
    + (
        ", ".join(f"{batch:02d}" for batch in sorted(common_batches))
        or "<none>"
    )
)
print(
    "Expected batch layout (most common across folders): "
    + (", ".join(f"{batch:02d}" for batch in expected_layout) or "<none>")
    + f"; used by {layout_counts[expected_layout]} folder(s)"
)

if highest_batch is not None:
    highest_batch_folders = sorted(
        table_name
        for table_name, batches in table_batches.items()
        if highest_batch in batches
    )
    print(
        f"Highest batch is {highest_batch:02d}; found in: "
        + ", ".join(highest_batch_folders)
    )

display(result_df.orderBy("latest_batch", "table_folder"))

inconsistent_tables = [
    table_name
    for table_name, batches in table_batches.items()
    if batches != expected_batches or invalid_files_by_table[table_name]
]

if inconsistent_tables:
    print(
        "INCONSISTENT raw folders: " + ", ".join(sorted(inconsistent_tables))
    )
    print("Review missing_expected_batches and unexpected_batches above.")
else:
    print(f"PASS: all {len(table_batches)} raw folders contain the same batches.")
