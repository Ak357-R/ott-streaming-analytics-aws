# Architecture Documentation

## OTT Streaming Analytics Platform on AWS

---

## System Architecture

### Data Flow

```
[Source Data]
     |
     | Python script generates realistic OTT data
     |
[S3 Raw Layer]  s3://bucket/raw/
     |           - users.csv         (10,000 rows)
     |           - subscriptions.csv (10,000 rows)
     |           - content_catalog.csv (1,000 rows)
     |           - watch_events.json  (100,000 records)
     |
     | AWS Glue Job 01: raw_to_bronze (PySpark)
     | - Cast data types
     | - Remove duplicates
     | - Add metadata columns
     |
[S3 Bronze Layer]  s3://bucket/bronze/
     |              - users/          (Parquet)
     |              - subscriptions/  (Parquet)
     |              - content_catalog/(Parquet)
     |              - watch_events/   (Parquet)
     |
     | AWS Glue Job 02: bronze_to_silver (PySpark)
     | - Join tables across domains
     | - Apply business logic
     | - Create derived columns
     |
[S3 Silver Layer]  s3://bucket/silver/
     |              - user_watch_profile/   (Parquet)
     |              - content_performance/  (Parquet)
     |
     | AWS Glue Job 03: silver_to_gold (PySpark)
     | - Pre-aggregate KPIs
     | - Build business metrics
     |
[S3 Gold Layer]  s3://bucket/gold/
     |            - dau_metrics/       (Parquet)
     |            - revenue_summary/   (Parquet)
     |            - top_content/       (Parquet)
     |            - device_analytics/  (Parquet)
     |
     | AWS Glue Data Catalog (Crawlers)
     | Registers schema for all layers
     |
[Amazon Athena]
     |  - SQL query engine
     |  - Queries Parquet directly from S3
     |  - No data loading required
     |
[Power BI Dashboard]
     - Connected via Athena ODBC Connector
     - DAU trend chart
     - Revenue by plan
     - Device market share
     - Top content table
```

---

## Architecture Decision Records (ADR)

### ADR-001: Data Lakehouse over Traditional Warehouse

**Decision:** Use S3 + Athena (Lakehouse) instead of Amazon Redshift

**Context:**
Amazon Redshift Serverless was not available on the AWS
promotional account type used for this project.

**Options Considered:**
1. Amazon Redshift Serverless
2. Amazon Redshift Provisioned
3. S3 + Athena (Data Lakehouse)

**Decision:**
Chose S3 + Athena — the modern Data Lakehouse pattern.

**Rationale:**
- Serverless: zero cluster management overhead
- Pay per query: cost efficient for variable workloads
- Modern pattern: adopted by Uber, Airbnb, Databricks
- Infinite scalability: no capacity planning required
- Same SQL interface: no learning curve difference

**Consequences:**
- Slightly slower for extremely frequent repeated queries
- No stored procedures or user-defined functions
- No direct JDBC connection for some BI tools
- Used ODBC connector for Power BI integration

---

### ADR-002: Medallion Architecture for Data Lake

**Decision:** Use Bronze/Silver/Gold layering

**Rationale:**
- Separation of concerns: each layer has one responsibility
- Data lineage: easy to trace issues to specific layer
- Reprocessing: can replay from any layer without raw re-ingestion
- Multiple consumers: Silver for ML, Gold for dashboards
- Industry standard: used by Netflix, Hotstar, Databricks

---

### ADR-003: Parquet over CSV for Processed Layers

**Decision:** Convert CSV/JSON to Parquet in Bronze layer

**Rationale:**
- Columnar format: 10x faster for analytical queries
- Built-in compression: 60-80% smaller file size
- Schema embedded: no schema-on-read issues
- Predicate pushdown: skip irrelevant row groups
- Native support: works with Glue, Athena, Redshift

---

### ADR-004: Power BI over Amazon QuickSight

**Decision:** Use Power BI for dashboarding

**Context:**
Amazon QuickSight Enterprise trial was not available on
the AWS promotional account type.

**Rationale:**
- Power BI is industry standard BI tool
- More widely used in enterprise environments
- Athena ODBC connector provides native integration
- Cross-platform skill more valuable for portfolio
- Demonstrates cross-cloud/tool integration capability

---

## AWS Services Used

| Service | Purpose | Cost |
|---------|---------|------|
| Amazon S3 | Data Lake storage (4 layers) | ~$0.001 |
| AWS Glue | ETL jobs (PySpark) | ~$0.090 |
| AWS Glue Crawlers | Schema detection | ~$0.015 |
| AWS Glue Data Catalog | Metadata store | Free |
| Amazon Athena | SQL query engine | ~$0.002 |
| AWS CloudWatch | Monitoring & alerts | Free tier |
| Amazon SNS | Email notifications | Free tier |
| AWS IAM | Security & access | Free |

**Total project cost: ~$0.11**

---

## Security Architecture

```
IAM Structure:
├── Root Account (locked, MFA enabled)
│
└── IAM User: ott-project-admin
    ├── AdministratorAccess policy
    ├── MFA enabled
    └── Used for all console operations

IAM Roles:
└── ott-glue-role
    ├── AmazonS3FullAccess
    ├── AWSGlueServiceRole
    └── Assumed by Glue ETL jobs

S3 Security:
├── Block all public access: ENABLED
├── Versioning: ENABLED
├── Encryption: SSE-S3 (free)
└── No bucket policies allowing public access
```

---

## Scalability Considerations

| Current | Production Scale |
|---------|-----------------|
| 121,000 records | 100M+ records/day |
| 2 Glue DPUs | 20-100 Glue DPUs |
| Batch daily | Kinesis streaming |
| Single region | Multi-region |
| Manual trigger | EventBridge schedule |
| Basic monitoring | Full observability |
