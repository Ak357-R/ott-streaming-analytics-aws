# OTT Streaming Analytics Platform on AWS

![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Python](https://img.shields.io/badge/Python-3.8+-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.0+-red)
![Status](https://img.shields.io/badge/Status-Complete-green)

> **Project 1** of the OTT Platform series.
> Project 2 (Real-Time Engine): [ott-realtime-engine-aws](https://github.com/Ak357-R/ott-realtime-engine-aws)

A **production-style Data Engineering project** built entirely on AWS demonstrating end-to-end data pipeline from raw data ingestion to business intelligence dashboards.

---

## Full OTT Platform — Two Projects

```
PROJECT 1 (This repo)              PROJECT 2
────────────────────               ─────────────────────
Data Lake + Analytics         +    Real-Time Processing
                                   Engine

S3 → Glue → Athena → Power BI      EC2 + Lambda + Step Functions
                                   + SQS + API Gateway
                                   + DynamoDB + ECS Fargate
```

---

## Project 1 Architecture

```
Data Generation (Python)
         |
    S3 Raw Layer (CSV/JSON)
         |
   AWS Glue ETL (PySpark)
         |
   S3 Bronze Layer (Parquet)
         |
   AWS Glue ETL (PySpark)
         |
   S3 Silver Layer (Parquet)
         |
   AWS Glue ETL (PySpark)
         |
    S3 Gold Layer (Parquet)
         |
   AWS Glue Data Catalog
         |
    Amazon Athena (SQL)
         |
  Power BI Dashboard (ODBC)
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
| Orchestration | AWS Glue Workflows |
| Monitoring | Amazon CloudWatch + SNS |
| Dashboard | Power BI + Athena ODBC Connector |
| Language | Python, PySpark, SQL |
| Version Control | GitHub |

---

## Data Pipeline — Medallion Architecture

| Layer | Format | Records | Purpose |
|-------|--------|---------|---------|
| Raw | CSV / JSON | 121,000 | Original landing zone |
| Bronze | Parquet | 121,000 | Cleaned + typed + metadata |
| Silver | Parquet | 100,000 | Joined + business logic |
| Gold | Parquet | ~500 | Pre-aggregated KPIs |

---

## Datasets

| Dataset | Records | Format | Description |
|---------|---------|--------|-------------|
| users.csv | 10,000 | CSV | User profiles across 10 Indian regions |
| subscriptions.csv | 10,000 | CSV | Plans (free/basic/standard/premium) |
| content_catalog.csv | 1,000 | CSV | Movies and series, 10 genres |
| watch_events.json | 100,000 | JSON | Watch activity with device, quality |

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
│   │   └── 04_data_quality.py      # 11 data quality checks
│   │
│   └── sql/
│       └── gold_analytics.sql      # 10 business queries
│
└── docs/
    ├── architecture.md             # Architecture decisions
    └── cost_analysis.md            # AWS cost breakdown
```

---

## Key Metrics Built

| Metric | Query |
|--------|-------|
| DAU | Daily Active Users trend |
| MAU | Monthly Active Users |
| Revenue | By plan and month |
| Top Content | By watch hours |
| Device Share | All 5 devices breakdown |
| Genre Performance | Completion rates |

---

## Data Quality Framework

**11 automated checks** across 3 tables:

| Table | Check | Type |
|-------|-------|------|
| users | No NULL user_id | NULL |
| users | Age 18-100 | Range |
| users | Email contains @ | Format |
| users | No duplicates | Duplicate |
| watch_events | watched_mins > 0 | Range |
| watch_events | completion_pct 0-100 | Range |
| watch_events | No NULL user_id | NULL |
| watch_events | No NULL content_id | NULL |
| subscriptions | amount_paid >= 0 | Range |
| subscriptions | Valid plan values | Business rule |
| subscriptions | end_date > start_date | Logic |

---

## Sample Athena Queries

```sql
-- Revenue by subscription plan
SELECT plan,
       SUM(monthly_revenue) AS total_revenue,
       SUM(new_subscribers) AS total_subscribers
FROM ott_gold_db.revenue_summary
GROUP BY plan
ORDER BY total_revenue DESC;

-- Device market share
SELECT watch_device,
       total_sessions,
       ROUND(total_sessions * 100.0 /
           SUM(total_sessions) OVER(), 1) AS share_pct
FROM ott_gold_db.device_analytics
ORDER BY total_sessions DESC;
```

---

## Pipeline Orchestration

```
1 AM Daily (Scheduled)
      ↓
Data Quality Checks
      ↓ (if passed)
Raw → Bronze
      ↓
Bronze → Silver
      ↓
Silver → Gold
      ↓
Dashboard auto-refreshes
```

---

## Architecture Decision — Data Lakehouse

Chose S3 + Athena over Amazon Redshift:
- Serverless — zero cluster management
- Pay per query — cost efficient
- Modern pattern (Uber, Airbnb, Databricks)
- Same SQL interface

---

## AWS Cost

```
S3 Storage:    $0.001
Glue ETL Jobs: $0.090
Crawlers:      $0.015
Athena:        $0.002
CloudWatch:    $0.000
Total:         ~$0.11
```

---

## Setup Guide

1. Clone repository
2. Run `data_generation/generate_ott_data.py`
3. Upload files to S3 raw layer
4. Create Glue IAM Role
5. Run Glue Crawler on raw layer
6. Execute Glue jobs: 01 → 04
7. Run Gold crawler
8. Query with Athena
9. Connect Power BI via ODBC

---

## Interview Highlights

- Complete Medallion Architecture on AWS
- 121,000 records through 4-layer pipeline
- 11 data quality checks with quarantine
- Automated event-driven orchestration
- Cross-tool: AWS + Power BI
- Total cost: **$0.11**
- Real debugging (duplicate columns, JSON arrays)
- Architecture decisions with documented reasoning

---

## Author

**Akash** | Data Engineering & Cloud Portfolio
Built on AWS | June 2026

**Other Projects:**
- [OTT Real-Time Engine](https://github.com/Ak357-R/ott-realtime-engine-aws) — EC2, Lambda, Step Functions, ECS
