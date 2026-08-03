# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.cards."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "cards"
SOURCE_COLUMNS = [
    "card_id",
    "account_id",
    "card_type",
    "pan",
    "expiry",
    "status",
    "effective_at",
]
RECORD_ID_COLUMNS = ["card_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
