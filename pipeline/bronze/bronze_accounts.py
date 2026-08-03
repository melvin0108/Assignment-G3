# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.accounts."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "accounts"
SOURCE_COLUMNS = [
    "account_id",
    "customer_id",
    "product_type",
    "open_date",
    "status",
    "currency",
]
RECORD_ID_COLUMNS = ["account_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
