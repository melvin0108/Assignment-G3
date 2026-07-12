# Databricks notebook source
"""Run AvailableNow Auto Loader ingestion for every configured Bronze table."""

from pipeline.bronze.autoloader_common import TABLE_CONFIG, ingest_table


table_names = list(TABLE_CONFIG)

for position, table_name in enumerate(table_names, start=1):
    print(f"[{position}/{len(table_names)}] Starting bronze.{table_name}")
    ingest_table(table_name)

print(f"Completed Auto Loader ingestion for all {len(table_names)} Bronze tables")
