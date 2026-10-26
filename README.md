# Automated Financial Lakehouse & Data Quality Pipeline

An enterprise-grade, cost-optimized data lakehouse built with Python, PySpark/DuckDB, Apache Airflow, and AWS Athena DDL implementing the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**.

## Architecture Overview

```text
[ Raw Financial Streams ]
          │
          ▼
   ┌──────────────┐
   │ Bronze Layer │  Raw JSON staging with realistic data anomalies
   └──────┬───────┘
          │ (Schema Enforcement, Quality Filters, Window Deduplication)
          ▼
   ┌──────────────┐
   │ Silver Layer │  Cleaned Snappy Parquet partitioned by trade_date
   └──────┬───────┘
          │ (Automated pytest Quality Suite Assertions)
          ▼
   ┌──────────────┐
   │  Gold Layer  │  Aggregated KPIs (Running Volume, Moving Averages)
   └──────┬───────┘
          │
   ┌──────┴─────────────────────────────────┐
   │ Downstream Query & Governance          │
   │ • AWS Athena Serverless SQL Analytics  │
   │ • Automated Retention & Cost Cleanup   │
   └────────────────────────────────────────┘