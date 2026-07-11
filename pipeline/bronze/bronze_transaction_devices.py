# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.transaction_devices."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("transaction_devices")
