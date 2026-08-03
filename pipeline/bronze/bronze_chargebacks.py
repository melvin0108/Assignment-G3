# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.chargebacks."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "chargebacks"
SOURCE_COLUMNS = [
    "chargeback_id",
    "dispute_id",
    "scheme",
    "amount",
    "stage",
    "processed_at",
]
RECORD_ID_COLUMNS = ["chargeback_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
