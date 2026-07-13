# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.countries."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("countries")
