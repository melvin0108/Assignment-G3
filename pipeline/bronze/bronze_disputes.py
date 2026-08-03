# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.disputes."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "disputes"
SOURCE_COLUMNS = [
    "dispute_id",
    "transaction_id",
    "reason_code",
    "amount",
    "status",
    "raised_at",
]
RECORD_ID_COLUMNS = ["dispute_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
