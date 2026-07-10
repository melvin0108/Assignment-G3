# Databricks notebook source
# ============================================================================
# GOVERNANCE PIPELINE: metadata_lineage
# ============================================================================
# Defines and populates the metadata lineage registry under gov.metadata_lineage.
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
TABLE_NAME = "metadata_lineage"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# Define explicit schema matching the DDL
schema = StructType([
    StructField("source_catalog", StringType(), nullable=True),
    StructField("source_schema", StringType(), nullable=True),
    StructField("source_table", StringType(), nullable=True),
    StructField("source_field", StringType(), nullable=True),
    StructField("target_catalog", StringType(), nullable=True),
    StructField("target_schema", StringType(), nullable=True),
    StructField("target_table", StringType(), nullable=True),
    StructField("target_field", StringType(), nullable=True),
    StructField("transformation_logic", StringType(), nullable=True)
])

# Define metadata lineage rows
data = [
    ("g3_dev", "bronze", "customers", "customer_id", "g3_dev", "silver", "customers", "customer_id", "Direct copy"),
    ("g3_dev", "bronze", "customers", "first_name", "g3_dev", "silver", "customers", "first_name", "Tokenized with SHA256 and salt"),
    ("g3_dev", "bronze", "customers", "last_name", "g3_dev", "silver", "customers", "last_name", "Tokenized with SHA256 and salt"),
    ("g3_dev", "bronze", "customers", "dob", "g3_dev", "silver", "customers", "dob", "Generalized into age bands based on RUN_DATE"),
    ("g3_dev", "bronze", "customers", "email", "g3_dev", "silver", "customers", "email", "Masked first character + domain replace"),
    ("g3_dev", "bronze", "customers", "phone", "g3_dev", "silver", "customers", "phone", "Masked keeping last 4 digits only"),
    ("g3_dev", "bronze", "customers", "address", "g3_dev", "silver", "customers", "address", "Hashed with SHA256 and salt"),
    ("g3_dev", "bronze", "customers", "tax_id", "g3_dev", "silver", "customers", "tax_id", "Hashed with SHA256 and salt"),
    ("g3_dev", "bronze", "cards", "pan", "g3_dev", "silver", "cards", "pan", "Masked showing last 4 digits only"),
    ("g3_dev", "bronze", "employees", "full_name", "g3_dev", "silver", "employees", "full_name", "Hashed with SHA256 and salt"),
    ("g3_dev", "bronze", "employees", "email", "g3_dev", "silver", "employees", "email", "Hashed with SHA256 and salt")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Ensure schema/database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Write to target Delta table
print(f"Writing lineage to Table: {FULL_TABLE_NAME}")
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
print("\nVerifying Metadata Lineage Registry:")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}").show(truncate=False)
spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}").show(truncate=False)
