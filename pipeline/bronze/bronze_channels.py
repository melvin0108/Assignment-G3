# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.channels."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "channels"
SOURCE_COLUMNS = ["channel_code", "channel_name"]
RECORD_ID_COLUMNS = ["channel_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
