# Databricks notebook source
"""Run AvailableNow Auto Loader ingestion for every Bronze table."""

from pipeline.bronze.autoloader_common import ingest_table
from pipeline.bronze.table_registry import ALL_TABLE_CONFIGS


table_configs = list(ALL_TABLE_CONFIGS.items())

for position, (table_name, config) in enumerate(table_configs, start=1):
    source_columns, record_id_columns = config
    print(f"[{position}/{len(table_configs)}] Starting bronze.{table_name}")
    ingest_table(table_name, source_columns, record_id_columns)

print(f"Completed Auto Loader ingestion for all {len(table_configs)} Bronze tables")
