# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.dispute_reason_codes."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "dispute_reason_codes"
SOURCE_COLUMNS = ["reason_code", "description"]
RECORD_ID_COLUMNS = ["reason_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
