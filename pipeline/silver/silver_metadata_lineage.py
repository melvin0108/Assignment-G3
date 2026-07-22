# Databricks notebook source
# ============================================================================
# GOVERNANCE PIPELINE: metadata_lineage
# ============================================================================
# Defines and populates the metadata lineage registry under gov.metadata_lineage.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.dbutils import DBUtils

# In a Databricks environment, `spark` is pre-initialized.
# This line gets the existing session or initializes one.
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
def _catalog_widget():
    try:
        dbutils.widgets.get("catalog")
    except Exception:
        dbutils.widgets.dropdown("catalog", "g3_dev", ["g3_dev", "g3_test", "g3_catalog"])
    catalog = dbutils.widgets.get("catalog")
    if catalog not in {"g3_dev", "g3_test", "g3_catalog"}:
        raise ValueError(f"Unsupported catalog: {catalog}")
    return catalog


CATALOG = _catalog_widget()
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
    (CATALOG, "bronze", "customers", "customer_id", CATALOG, "silver", "customers", "customer_id", "Direct copy"),
    (CATALOG, "bronze", "customers", "first_name", CATALOG, "silver", "customers", "first_name", "Tokenized with SHA256 and salt"),
    (CATALOG, "bronze", "customers", "last_name", CATALOG, "silver", "customers", "last_name", "Tokenized with SHA256 and salt"),
    (CATALOG, "bronze", "customers", "dob", CATALOG, "silver", "customers", "dob", "Generalized into age bands based on RUN_DATE"),
    (CATALOG, "bronze", "customers", "email", CATALOG, "silver", "customers", "email", "Masked first character + domain replace"),
    (CATALOG, "bronze", "customers", "phone", CATALOG, "silver", "customers", "phone", "Masked keeping last 4 digits only"),
    (CATALOG, "bronze", "customers", "address", CATALOG, "silver", "customers", "address", "Hashed with SHA256 and salt"),
    (CATALOG, "bronze", "customers", "tax_id", CATALOG, "silver", "customers", "tax_id", "Hashed with SHA256 and salt"),
    (CATALOG, "bronze", "cards", "pan", CATALOG, "silver", "cards", "pan", "Masked showing last 4 digits only"),
    (CATALOG, "bronze", "employees", "full_name", CATALOG, "silver", "employees", "full_name", "Hashed with SHA256 and salt"),
    (CATALOG, "bronze", "employees", "email", CATALOG, "silver", "employees", "email", "Hashed with SHA256 and salt"),
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
