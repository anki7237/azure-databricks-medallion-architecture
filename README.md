# Azure Databricks End-to-End Data Engineering Project

This project demonstrates an end-to-end Data Engineering pipeline built on Azure Databricks using the Medallion Architecture (Bronze, Silver, and Gold layers).

The pipeline covers data ingestion, transformation, data quality checks, incremental processing, and business-ready data modeling using Azure Databricks, PySpark, Delta Lake, Auto Loader, and Databricks Workflows.

## Key Features

* Bronze Layer: Raw data ingestion using Auto Loader
* Silver Layer: Data cleansing, deduplication, schema validation, and Delta MERGE-based upserts
* Gold Layer: Business-ready aggregations and analytics datasets
* Delta Lake features including Time Travel, OPTIMIZE, and ZORDER
* Incremental and idempotent processing patterns
* Databricks Workflows for orchestration

## Tech Stack

* Azure Databricks
* PySpark
* Delta Lake
* Auto Loader
* Azure Data Lake Storage (ADLS)
* Databricks Workflows
* SQL

This project was built as part of my Data Engineering learning journey to gain hands-on experience with production-style Lakehouse architectures and modern data pipeline design.
