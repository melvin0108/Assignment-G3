# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.defects_manifest."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("defects_manifest")
