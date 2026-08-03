# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.currencies."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "currencies"
SOURCE_COLUMNS = ["currency_code", "name", "decimals"]
RECORD_ID_COLUMNS = ["currency_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
