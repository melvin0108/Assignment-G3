# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.date_dim."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("date_dim")
