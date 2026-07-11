# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.merchant_categories."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("merchant_categories")
