# Zomato-Style Event-Driven Data Platform

This project implements an industry-grade, event-driven batch ETL platform using AWS Glue and Step Functions.

## Architecture
S3 → EventBridge → Step Functions → Glue (Bronze → Silver → Gold → DQ)

## Key Features
- Immutable raw ingestion
- Deterministic deduplication
- Late-arriving data handling
- Business-rule enforcement
- Data quality quarantine
- Idempotent processing

## Tech Stack
- AWS Glue (PySpark)
- Amazon S3
- AWS Step Functions
- Glue Data Catalog

This design follows real-world patterns used in food delivery and e-commerce companies.
