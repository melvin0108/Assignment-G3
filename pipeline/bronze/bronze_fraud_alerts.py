# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.fraud_alerts."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("fraud_alerts")
