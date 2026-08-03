# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.case_transactions."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "case_transactions"
SOURCE_COLUMNS = ["case_id", "transaction_id", "linked_at"]
RECORD_ID_COLUMNS = ["case_id", "transaction_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
