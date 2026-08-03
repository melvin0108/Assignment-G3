# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.fraud_alerts."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "fraud_alerts"
SOURCE_COLUMNS = [
    "alert_id",
    "transaction_id",
    "rule_name",
    "score",
    "triggered_at",
    "disposition",
]
RECORD_ID_COLUMNS = ["alert_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
