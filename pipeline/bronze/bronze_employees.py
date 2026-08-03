# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.employees."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "employees"
SOURCE_COLUMNS = ["employee_id", "full_name", "email", "team", "role"]
RECORD_ID_COLUMNS = ["employee_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
