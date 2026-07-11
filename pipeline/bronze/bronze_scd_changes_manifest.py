# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.scd_changes_manifest."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("scd_changes_manifest")
