# Databricks notebook source
# ---------------------------------------------------------------------------
# Template: Create a table in PySpark (Databricks)
# ---------------------------------------------------------------------------
# This template shows a few common patterns for creating a table:
#   1. From a DataFrame built in code / read from a source
#   2. Writing it out as a managed Delta table (recommended default)
#   3. Creating it via SQL DDL instead, if you prefer explicit schemas
#
# Delete/adjust the sections you don't need.
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)

# In a Databricks notebook, `spark` is already available.
# This line is only needed if you're running outside a notebook context.
# spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIG — edit these
# ---------------------------------------------------------------------------
CATALOG = "main"                 # Unity Catalog catalog name (or omit if not using UC)
SCHEMA = "default"               # database / schema name
TABLE_NAME = "my_table"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

SOURCE_PATH = "/mnt/path/to/source/data"   # e.g. a CSV/Parquet/JSON path, or None
FILE_FORMAT = "csv"                        # csv, json, parquet, delta, etc.

# ---------------------------------------------------------------------------
# 1. Load or build the DataFrame
# ---------------------------------------------------------------------------

# Option A: Read from a source file
# df = (
#     spark.read
#     .format(FILE_FORMAT)
#     .option("header", "true")
#     .option("inferSchema", "true")
#     .load(SOURCE_PATH)
# )

# Option B: Define an explicit schema and build/read with it (recommended for prod)
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])

# df = spark.read.format(FILE_FORMAT).schema(schema).load(SOURCE_PATH)

# ---------------------------------------------------------------------------
# 2. (Optional) Transformations before saving
# ---------------------------------------------------------------------------
df = (
    df
    .filter(F.col("id").isNotNull())
    # .withColumn("value", F.round(F.col("value"), 2))
    # .dropDuplicates(["id"])
)

# ---------------------------------------------------------------------------
# 3. Create the schema/database if it doesn't exist
# ---------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# ---------------------------------------------------------------------------
# 4. Write the DataFrame out as a managed Delta table
# ---------------------------------------------------------------------------
(
    df.write
    .format("delta")
    .mode("overwrite")            # overwrite | append | ignore | error
    .option("overwriteSchema", "true")   # only needed if schema changes on overwrite
    .partitionBy()                # e.g. .partitionBy("some_date_col")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# 5. Alternative: create the table via SQL DDL instead of saveAsTable
# ---------------------------------------------------------------------------
# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
#         id INT NOT NULL,
#         name STRING,
#         value DOUBLE,
#         created_at TIMESTAMP
#     )
#     USING DELTA
# """)
#
# df.createOrReplaceTempView("tmp_view")
# spark.sql(f"INSERT OVERWRITE TABLE {FULL_TABLE_NAME} SELECT * FROM tmp_view")

# ---------------------------------------------------------------------------
# 6. Verify
# ---------------------------------------------------------------------------
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10").show()
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)