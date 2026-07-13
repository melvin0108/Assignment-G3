# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.case_transactions."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("case_transactions")
