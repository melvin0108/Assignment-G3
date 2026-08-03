# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.customer_contact_logs."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "customer_contact_logs"
SOURCE_COLUMNS = [
    "contact_id",
    "customer_id",
    "direction",
    "contact_method",
    "do_not_contact",
    "contacted_at",
    "employee_id",
    "note",
]
RECORD_ID_COLUMNS = ["contact_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
