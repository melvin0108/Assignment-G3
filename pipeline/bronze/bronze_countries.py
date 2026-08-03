# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.countries."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "countries"
SOURCE_COLUMNS = ["iso_code", "name", "region"]
RECORD_ID_COLUMNS = ["iso_code"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
