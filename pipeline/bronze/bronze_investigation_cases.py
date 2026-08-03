# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.investigation_cases."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "investigation_cases"
SOURCE_COLUMNS = [
    "case_id",
    "priority",
    "status_code",
    "fraud_type_code",
    "owner_employee_id",
    "opened_at",
    "closed_at",
    "legal_hold",
]
RECORD_ID_COLUMNS = ["case_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
