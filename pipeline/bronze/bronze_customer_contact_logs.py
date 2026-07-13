# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.customer_contact_logs."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("customer_contact_logs")
