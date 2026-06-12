# AWS Cost Analysis

## OTT Streaming Analytics Platform

---

## Total Project Cost: ~$0.11

Built using $100 AWS promotional credits.
**99.9% of credits remaining after complete project.**

---

## Cost Breakdown by Service

| Service | Usage | Unit Cost | Total |
|---------|-------|-----------|-------|
| Amazon S3 | ~35 MB stored | $0.023/GB/month | $0.001 |
| AWS Glue ETL | 5 jobs × 0.045 DPU-hr avg | $0.44/DPU-hr | $0.090 |
| AWS Glue Crawlers | 3 crawlers × ~3 min | $0.44/DPU-hr | $0.015 |
| Amazon Athena | 10 queries × ~5MB scanned | $5/TB | $0.002 |
| CloudWatch | Basic metrics | Free tier | $0.000 |
| SNS | Email alerts | Free tier | $0.000 |
| IAM | Users and roles | Always free | $0.000 |
| **TOTAL** | | | **~$0.11** |

---

## Glue Job Cost Detail

| Job | Duration | DPUs | DPU-Hours | Cost |
|-----|----------|------|-----------|------|
| ott-raw-to-bronze | 1m 38s | 2 | 0.054 | $0.024 |
| ott-bronze-to-silver (failed) | 1m 15s | 2 | 0.041 | $0.018 |
| ott-bronze-to-silver (success) | 1m 37s | 2 | 0.054 | $0.024 |
| ott-silver-to-gold | 1m 17s | 2 | 0.043 | $0.019 |
| ott-data-quality | ~1m 30s | 2 | 0.050 | $0.022 |
| **Total Glue** | | | | **~$0.107** |

---

## Cost Optimization Decisions

### 1. Serverless Over Provisioned
```
Redshift Provisioned Cluster:
- dc2.large = $0.25/hour
- Always running = $180/month
- Our usage: 0 hours needed

Athena Instead:
- $5 per TB scanned
- Our queries: ~50 MB total
- Cost: $0.0003
- Savings: ~$180/month
```

### 2. Minimum Glue Workers
```
Used 2 workers (minimum) instead of 10
For 121,000 records: 2 workers is sufficient
Cost ratio: 2/10 = 80% savings on compute
```

### 3. S3 Standard vs Intelligent Tiering
```
Our data: 35 MB (tiny)
S3 Standard: $0.023/GB = $0.001/month
No need for tiering at this scale
```

### 4. Free Tier Usage
```
Services used within free tier:
- CloudWatch: 10 metrics free/month
- SNS: 1,000 email notifications free/month
- IAM: Always free
- Glue Data Catalog: 1M objects free/month
```

---

## Production Cost Estimate

If this project processed real Netflix-scale data:

| Component | Current | Production (Netflix Scale) |
|-----------|---------|---------------------------|
| Data Volume | 35 MB | 100 TB/day |
| S3 Storage | $0.001 | ~$2,300/month |
| Glue ETL | $0.09 | ~$15,000/month |
| Athena Queries | $0.002 | ~$5,000/month |
| **Total** | **$0.11** | **~$22,000/month** |

> Netflix actually uses a mix of Spark on EMR, Iceberg tables,
> and Druid for real-time analytics at this scale.

---

## Budget Alert Configuration

```
Alert 1: Zero Spend Alert
- Threshold: $1.00
- Purpose: Detect any unexpected charges immediately
- Status: HEALTHY (no charges triggered)

Alert 2: Monthly Cap Alert
- Threshold: $60.00
- Purpose: Warn before hitting 60% of $100 budget
- Status: HEALTHY ($0.11 spent)
```
