# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.merchant_categories."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "merchant_categories"
SOURCE_COLUMNS = ["mcc", "category_name", "category_group"]
RECORD_ID_COLUMNS = ["mcc"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
