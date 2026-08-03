# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.auth_attempts."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "auth_attempts"
SOURCE_COLUMNS = [
    "attempt_id",
    "transaction_id",
    "decision",
    "decline_reason",
    "auth_ts",
]
RECORD_ID_COLUMNS = ["attempt_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
