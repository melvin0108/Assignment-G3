# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.branches."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "branches"
SOURCE_COLUMNS = ["branch_code", "name", "country", "region", "status"]
RECORD_ID_COLUMNS = ["branch_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
