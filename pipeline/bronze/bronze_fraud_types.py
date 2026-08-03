# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.fraud_types."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "fraud_types"
SOURCE_COLUMNS = ["fraud_type_code", "description", "severity"]
RECORD_ID_COLUMNS = ["fraud_type_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
