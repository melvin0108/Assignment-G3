# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.transaction_devices."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "transaction_devices"
SOURCE_COLUMNS = [
    "device_id",
    "transaction_id",
    "device_type",
    "ip",
    "geo_country",
]
RECORD_ID_COLUMNS = ["device_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
