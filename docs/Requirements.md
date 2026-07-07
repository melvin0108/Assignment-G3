**TAC@NABVNSC22 - Technical Assignment - Data Pipeline for Zero Trust AI Context**

# Executive Summary

**Item**

**Description**

Learning

Goals

As a Starcamper in the Data Engineering cohort, I can build a small but realistic data pipeline that turns raw mock banking data into trusted

AI-ready context.

As a Starcamper, I can implement practical data engineering controls, including schema validation, data quality checks, quarantine

handling, PII masking, access rules, lineage, metadata, and test evidence.

As a Starcamper, I can explain how Zero Trust AI principles are enforced through the data pipeline before any AI experience is allowed to

consume the context.

Assignme

nt Type

Technical group assignment

Hands-on data pipeline prototype

Mock data only

Engineering walkthrough and code review

Expected

Output

A working repository, sample input data, pipeline code, quality checks, masked AI-ready output, technical documentation, and a short

demo.

# Description

## Context

Banking AI experiences such as **Banker.AI** and **Customer.AI** are only useful when they are grounded in trusted, governed, and explainable data. If an AI system consumes unclear, outdated, incomplete, duplicated, or sensitive data incorrectly, it can create hallucinated answers, misleading recommendations, privacy risks, or unsafe customer outcomes.

This assignment is **not an ideation exercise** and **not a solution design proposal**. Your team will build a practical data engineering prototype that prepares trusted context for an AI consumer.

The focus is the pipeline:

How mock source data is ingested

How invalid data is detected

How failed records are quarantined

How sensitive data is masked or removed

How lineage and metadata are captured

How AI-ready context is produced

How test evidence proves that the pipeline is reliable

## Assignment Scenario

Your team is part of a data engineering squad asked to prepare a trusted context dataset for an internal AI assistant.

Choose **one** of the following technical scenarios:

**Customer service context**: customer profile, recent service cases, contact preferences, and support history

**Product knowledge context**: product catalogue, eligibility rules, fees, feature descriptions, and update history

**Application status context**: mock application records, stage history, missing documents, and status changes

**Transaction investigation context**: mock transactions, dispute cases, merchant categories, and investigation notes

Your pipeline must process at least **three source datasets** and produce one or more **AI-ready context outputs**.

The AI-ready output should be safe for an AI consumer to retrieve. You do **not** need to build a chatbot or user interface.

# Scope

## In Scope

Create mock source datasets

Define source schemas and data contracts

Build an ingestion process

Build raw, cleaned, and curated data layers

Implement transformation logic

Implement data quality checks

Quarantine failed records with failure reasons

Implement PII detection, masking, redaction, or tokenisation

Produce AI-ready context output in JSON, CSV, Parquet, or database table format

Add metadata, lineage, and source traceability fields

Implement basic role-based output separation where relevant

Add automated tests or executable validation checks

Produce a technical runbook explaining how to run and verify the pipeline

## Out of Scope

No product ideation phase is required.

No user research is required.

No SDVF assessment is required.

No design principle assessment is required.

No Figma prototype or wireframe is required.

No production AI integration is required.

No real customer data is allowed.

No confidential NAB data is allowed.

No regulated financial advice should be produced.

# Technical Requirements

## Minimum Pipeline Requirements

Your pipeline must include:

At least **three input datasets**

At least **one raw layer**

At least **one cleaned layer**

At least **one curated AI-ready context output**

At least **eight data quality rules**

At least **one quarantine output** for failed records

At least **one PII masking or redaction step**

At least **one metadata or lineage output**

At least **one automated test or executable validation command**

A clear way to rerun the pipeline from a clean state

## Suggested Technical Stack

Your team may choose a stack that matches your current capability. Examples:

Python with Pandas, PySpark, DuckDB, or Polars

SQL with a local database such as DuckDB, SQLite, or PostgreSQL dbt with seed data and tests

Great Expectations, Soda, Deequ, or custom validation scripts

Docker is optional, but recommended if your setup is more complex

The stack is less important than the engineering quality. Your pipeline should be easy to run, inspect, and explain.

## Mock Data Requirements

Create mock data that includes both valid and invalid records.

Your input data should include examples of:

Missing required fields

Invalid values

Duplicate records

Stale or outdated records

Inconsistent status values

Referential integrity issues

Sensitive fields that need masking

Records that should not be exposed to an AI consumer

Do not use real customer names, real account numbers, real phone numbers, real email addresses, or confidential NAB information.

## Data Quality Requirements

Define and implement quality rules such as:

Required field completeness

Valid value checks

Date range checks

Duplicate detection

Referential integrity checks

Freshness checks

Business rule consistency

Sensitive data leakage checks

Record count reconciliation Allowed status transition checks

For each failed rule, your pipeline should show:

Which record failed

Which rule failed

Why the record failed

Whether the record is rejected, quarantined, masked, or allowed with warning

## Privacy and Access Requirements

Your pipeline must show how sensitive information is protected before AI consumption.

Include:

List of sensitive fields

Masking, redaction, hashing, or tokenisation approach

Fields removed from AI-ready output

Fields allowed only for internal users

Fields safe for customer-facing use, if applicable

Simple access rule assumptions

Audit fields such as created timestamp, pipeline run ID, or source reference

## Metadata and Lineage Requirements

Your curated output must include enough traceability to explain where the context came from.

Include metadata such as:

Source dataset name

Source record ID

Pipeline run ID

Processing timestamp

Quality status

Masking status

Context version

Last refreshed date

Known limitation or warning flag, where relevant

## Zero Trust AI Requirements

Your AI-ready context output must enforce Zero Trust AI thinking through the data layer.

Include:

Only approved source fields in the AI-ready output

No unmasked sensitive data

Quality status attached to each context record

Records below quality threshold excluded or flagged

Source references available for answer verification

Clear handling for unsupported or missing information

Example prompts or questions the AI could answer from the context

Example prompts or questions the AI must refuse because the context is missing, restricted, or unsafe

# Deliverables

## 1\. Git Repository

Provide a working repository that includes:

Source code

Mock input data

Configuration files

Tests or validation scripts

Generated output samples

README or runbook

## 2\. Mock Source Data

Include at least three input datasets.

For each dataset, document:

Dataset purpose

Key fields

Primary key or unique identifier

Example quality problems

Sensitive fields Relationship to other datasets

## 3\. Data Contract

Create a simple data contract for each source dataset.

Include:

Field name

Data type

Required or optional

Accepted values or pattern

Example value

Quality rule Sensitive data classification

## 4\. Pipeline Implementation

Implement the pipeline stages:

Ingest mock source data

Store raw data

Validate schema and data quality

Quarantine failed records

Transform valid records

Apply masking or redaction

Create curated AI-ready context Write metadata and lineage output

## 5\. Data Quality Evidence

Provide evidence that your checks run successfully.

Include:

Validation summary

Passed rule count

Failed rule count

Quarantined record count

Sample failed records Explanation of how failure handling works

## 6\. AI-Ready Context Output

Provide the final context output in a readable format.

The output should show:

Cleaned and transformed fields

Masked or redacted sensitive fields

Source references

Quality status

Context category

Last updated timestamp Usage restriction, if applicable

## 7\. Technical Runbook

Create a runbook that explains:

Prerequisites

Setup steps

How to run the pipeline

How to run tests or validation checks

Where outputs are generated

How to inspect quarantined records

Known limitations

Troubleshooting notes

# Acceptance Criteria

The repository can be opened and understood by another engineer.

The pipeline can be run from source data to curated output.

Mock data includes both valid and invalid records.

Data contracts are clear and match the implemented pipeline.

Data quality checks are executable, not only described.

Failed records are quarantined with useful failure reasons.

Sensitive fields are masked, redacted, tokenised, or removed before AI-ready output.

The curated output contains metadata and source traceability.

The AI-ready output does not expose unsafe or unsupported data.

Tests or validation commands can be run and produce evidence.

The runbook is clear enough for another team to reproduce the demo.

# Assessment

## Submission

A Confluence page is created by the team under the Assignment page.

The GitHub repository link is included.

All generated sample outputs are attached or linked.

All diagrams, if used, are attached or linked.

The README or runbook is included in the repository and linked from Confluence.