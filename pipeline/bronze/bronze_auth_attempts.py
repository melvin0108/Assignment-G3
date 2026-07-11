# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.auth_attempts."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("auth_attempts")
