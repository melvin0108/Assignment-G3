# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.case_parties."""

from pipeline.bronze.autoloader_common import ingest_table

TABLE_NAME = "case_parties"
SOURCE_COLUMNS = ["case_id", "party_type", "party_id", "role"]
RECORD_ID_COLUMNS = ["case_id", "party_type", "party_id"]


if __name__ == "__main__":
    ingest_table(TABLE_NAME, SOURCE_COLUMNS, RECORD_ID_COLUMNS)
