# OTT Streaming Analytics Platform on AWS

![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Python](https://img.shields.io/badge/Python-3.8+-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.0+-red)
![Status](https://img.shields.io/badge/Status-Complete-green)

A **production-style Data Engineering project** built entirely on AWS,
demonstrating end-to-end data pipeline from raw data ingestion to
business intelligence dashboards.

---

## Architecture

```
Data Generation (Python)
         |
    S3 Raw Layer
    (CSV / JSON)
         |
   AWS Glue ETL
   (PySpark Jobs)
         |
  S3 Bronze Layer
    (Parquet)
         |
   AWS Glue ETL
   (PySpark Jobs)
         |
  S3 Silver Layer
    (Parquet)
         |
   AWS Glue ETL
   (PySpark Jobs)
         |
   S3 Gold Layer
    (Parquet)
         |
  AWS Glue Data Catalog
         |
   Amazon Athena (SQL)
         |
  Power BI Dashboard
  (Athena ODBC Connector)
```

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Cloud | Amazon Web Services (AWS) |
| Storage | Amazon S3 |
| ETL | AWS Glue + Apache Spark (PySpark) |
| Catalog | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Orchestration | AWS Glue Workflows + EventBridge |
| Monitoring | Amazon CloudWatch + SNS |
| Dashboard | Power BI + Athena ODBC Connector |
| Language | Python, PySpark, SQL |
| Version Control | GitHub |

---

## Data Pipeline — Medallion Architecture

| Layer | Format | Records | Purpose |
|-------|--------|---------|---------|
| Raw | CSV / JSON | 121,000 | Original landing zone — never modified |
| Bronze | Parquet | 121,000 | Cleaned, typed, deduped, metadata added |
| Silver | Parquet | 100,000 | Joined across domains, business logic applied |
| Gold | Parquet | ~500 | Pre-aggregated KPIs for dashboards |

---

## Datasets

| Dataset | Records | Format | Description |
|---------|---------|--------|-------------|
| users.csv | 10,000 | CSV | User profiles across 10 Indian regions |
| subscriptions.csv | 10,000 | CSV | Subscription plans (free/basic/standard/premium) |
| content_catalog.csv | 1,000 | CSV | Movies and series across 10 genres |
| watch_events.json | 100,000 | JSON | Watch activity with device, quality, completion |

---

## Project Structure

```
ott-streaming-analytics-aws/
│
├── README.md
│
├── data_generation/
│   └── generate_ott_data.py        # Generates all 4 datasets
│
├── scripts/
│   ├── glue/
│   │   ├── 01_raw_to_bronze.py     # Raw → Bronze ETL
│   │   ├── 02_bronze_to_silver.py  # Bronze → Silver ETL
│   │   ├── 03_silver_to_gold.py    # Silver → Gold ETL
│   │   └── 04_data_quality.py      # 11 Data Quality Checks
│   │
│   └── sql/
│       └── gold_analytics.sql      # Business Analytics Queries
│
└── docs/
    ├── architecture.md             # Architecture decisions
    └── cost_analysis.md            # AWS cost breakdown
```

---

## Key Metrics Built

| Metric | Description |
|--------|-------------|
| DAU | Daily Active Users trend |
| MAU | Monthly Active Users |
| Revenue | By subscription plan and month |
| Top Content | By watch hours and completion rate |
| Device Share | Mobile vs Smart TV vs Desktop vs Laptop vs Tablet |
| Genre Performance | Completion rates by genre |

---

## Data Quality Framework

**11 automated checks** across 3 tables:

| Table | Check | Type |
|-------|-------|------|
| users | No NULL user_id | NULL check |
| users | Age between 18-100 | Range check |
| users | Email contains @ | Format check |
| users | No duplicate user_ids | Duplicate check |
| watch_events | watched_mins > 0 | Range check |
| watch_events | completion_pct 0-100 | Range check |
| watch_events | No NULL user_id | NULL check |
| watch_events | No NULL content_id | NULL check |
| subscriptions | amount_paid >= 0 | Range check |
| subscriptions | Valid plan values | Business rule |
| subscriptions | end_date > start_date | Logic check |

Failed records are quarantined to:
`s3://bucket/data_quality/failed_records/table/date/`

---

## Pipeline Orchestration

Automated daily pipeline via **AWS Glue Workflows**:

```
1:00 AM Daily (Scheduled Trigger)
         |
Data Quality Checks
         | (on success)
Raw → Bronze ETL
         | (on success)
Bronze → Silver ETL
         | (on success)
Silver → Gold ETL
         |
Dashboard auto-refreshes
```

---

## Monitoring & Alerts

- **CloudWatch Alarms** on Glue job failures
- **SNS Email Alerts** to data engineering team
- **S3 Quality Reports** saved daily
- **Glue Job Run History** for audit trail

---

## Architecture Decision — Data Lakehouse

**Chose:** S3 + Athena (Lakehouse pattern)
**Over:** Amazon Redshift (Traditional Warehouse)

**Reasons:**
- Serverless — zero cluster management
- Pay per query — cost efficient
- Modern pattern used by Uber, Airbnb, Databricks
- Infinitely scalable
- Same SQL interface as any warehouse

---

## Dashboard — Power BI + Athena ODBC

Connected **Power BI Desktop** to **Amazon Athena** via ODBC connector.
Queries Gold layer Parquet files directly from S3.
No data movement or duplication required.

**Visuals built:**
- DAU trend line chart
- Revenue by plan bar chart
- Device market share pie chart
- Top content performance table
- Monthly subscriber growth chart
- Genre completion rate bar chart

---

## AWS Cost Analysis

| Service | Usage | Cost |
|---------|-------|------|
| Amazon S3 | ~35 MB stored | $0.001 |
| AWS Glue ETL | 5 jobs × ~1.5 min | $0.090 |
| AWS Glue Crawlers | 3 crawlers | $0.015 |
| Amazon Athena | 10 queries | $0.002 |
| CloudWatch | Basic monitoring | $0.000 |
| SNS | Email alerts | $0.000 |
| **Total** | **Complete project** | **~$0.11** |

> Built using $100 AWS promotional credits. 99.9% of credits remaining.

---

## Interview Highlights

- Built complete **Medallion Architecture** on AWS from scratch
- Processed **121,000 records** through 4-layer pipeline
- Implemented **11 data quality checks** with quarantine pattern
- Automated pipeline with **event-driven orchestration**
- Cross-tool integration: **AWS + Power BI**
- Total cost: **$0.11** demonstrating cost optimization
- Handled real production errors (duplicate columns, JSON arrays)
- Made architecture decisions with documented reasoning

---

## Setup Guide

### Prerequisites
- AWS Account
- Python 3.8+
- Power BI Desktop
- Simba Athena ODBC Driver

### Steps
1. Clone this repository
2. Run `data_generation/generate_ott_data.py`
3. Upload files to S3 raw layer
4. Create Glue IAM Role with S3 access
5. Run Glue Crawler on raw layer
6. Execute Glue jobs in order (01 → 04)
7. Run Gold layer crawler
8. Query with Athena
9. Connect Power BI via ODBC

---

## Author

**Akash** | Data Engineering Portfolio Project
Built on AWS | June 2026
