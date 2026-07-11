# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.case_status_types."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("case_status_types")
