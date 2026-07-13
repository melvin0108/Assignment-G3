# Databricks notebook source
# ============================================================================
# GOVERNANCE PIPELINE: masking_policies
# ============================================================================
# Defines and populates the masking policy registry under gov.masking_policies.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CATALOG = "g3_dev"
SCHEMA = "gov"
TABLE_NAME = "masking_policies"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# Define explicit schema matching the DDL
schema = StructType([
    StructField("table_name", StringType(), nullable=True),
    StructField("field_name", StringType(), nullable=True),
    StructField("classification", StringType(), nullable=True),
    StructField("protection_method", StringType(), nullable=True),
    StructField("allowed_role", StringType(), nullable=True),
    StructField("owner", StringType(), nullable=True)
])

# Define policy registry rows
data = [
    ("customers", "first_name", "direct id", "tokenize (FPE)", "unprivileged", "M3"),
    ("customers", "last_name", "direct id", "tokenize (FPE)", "unprivileged", "M3"),
    ("customers", "email", "contact", "mask (j***@***.com)", "unprivileged", "M3"),
    ("customers", "phone", "contact", "mask (******1234)", "unprivileged", "M3"),
    ("customers", "address", "sensitive", "hash (SHA256)", "unprivileged", "M3"),
    ("customers", "dob", "sensitive", "generalise (age band)", "unprivileged", "M3"),
    ("customers", "tax_id", "sensitive", "hash (SHA256)", "unprivileged", "M3"),
    ("cards", "pan", "payment", "mask (XXXX-XXXX-XXXX-1234)", "unprivileged", "M3"),
    ("employees", "full_name", "staff", "hash (SHA256)", "unprivileged", "M3"),
    ("employees", "email", "staff", "hash (SHA256)", "unprivileged", "M3")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Write to target Delta table
print(f"Writing registry to Table: {FULL_TABLE_NAME}")
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"Table created/updated successfully: {FULL_TABLE_NAME}")

# ---------------------------------------------------------------------------
# VERIFY & DESCRIBE
# ---------------------------------------------------------------------------
print("\nVerifying Masking Policies Registry:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
