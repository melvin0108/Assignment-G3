# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.defects_manifest."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "defects_manifest"
SOURCE_COLUMNS = [
    "source_table",
    "record_key",
    "rule_id",
    "rule_name",
    "failure_reason",
    "severity",
]
RECORD_ID_COLUMNS = ["source_table", "record_key", "rule_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
