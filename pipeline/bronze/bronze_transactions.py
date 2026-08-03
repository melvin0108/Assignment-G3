# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.transactions."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "transactions"
SOURCE_COLUMNS = [
    "transaction_id",
    "account_id",
    "card_id",
    "merchant_id",
    "channel",
    "amount",
    "currency",
    "txn_ts",
    "status",
]
RECORD_ID_COLUMNS = ["transaction_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
