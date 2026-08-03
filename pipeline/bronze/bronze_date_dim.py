# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.date_dim."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "date_dim"
SOURCE_COLUMNS = ["date_id", "year", "month", "quarter", "is_weekend"]
RECORD_ID_COLUMNS = ["date_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
