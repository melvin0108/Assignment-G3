# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.investigation_notes."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "investigation_notes"
SOURCE_COLUMNS = [
    "note_id",
    "case_id",
    "author_employee_id",
    "note_text",
    "created_at",
]
RECORD_ID_COLUMNS = ["note_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
