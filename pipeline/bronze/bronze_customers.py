# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.customers."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "customers"
SOURCE_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "dob",
    "email",
    "phone",
    "address",
    "tax_id",
    "created_at",
    "effective_at",
]
RECORD_ID_COLUMNS = ["customer_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
