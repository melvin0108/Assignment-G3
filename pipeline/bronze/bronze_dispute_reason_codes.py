# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.dispute_reason_codes."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("dispute_reason_codes")
