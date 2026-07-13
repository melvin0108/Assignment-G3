# Databricks notebook source
"""AvailableNow Auto Loader entry point for bronze.cards."""

from pipeline.bronze.autoloader_common import ingest_table


ingest_table("cards")
