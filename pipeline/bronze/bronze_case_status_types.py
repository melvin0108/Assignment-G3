# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.case_status_types."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "case_status_types"
SOURCE_COLUMNS = ["status_code", "description"]
RECORD_ID_COLUMNS = ["status_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
