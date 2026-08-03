# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.merchants."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "merchants"
SOURCE_COLUMNS = [
    "merchant_id",
    "name",
    "mcc",
    "country",
    "risk_rating",
    "status",
    "effective_at",
]
RECORD_ID_COLUMNS = ["merchant_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
