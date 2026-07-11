# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.customers."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("customers")
