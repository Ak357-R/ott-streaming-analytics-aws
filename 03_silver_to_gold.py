"""
OTT Streaming Analytics Platform
Phase 8: Silver to Gold ETL Job

What it does:
- Reads Parquet files from S3 silver layer
- Creates business KPI aggregations:
  1. dau_metrics     - Daily Active Users
  2. revenue_summary - Revenue by plan and month
  3. top_content     - Most watched content with hours
  4. device_analytics- Device usage breakdown
- Writes Parquet files to S3 gold layer

Gold layer is consumed directly by:
- Amazon Athena SQL queries
- Power BI dashboards via ODBC

AWS Service: AWS Glue (PySpark)
Author: Akash
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

# ── Init ────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

BUCKET = "ott-platform-akash-700398842715"
ingestion_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

print("Starting Gold Layer Aggregations...")

# ── Read Silver Tables ───────────────────────────────
watch_profile = spark.read.parquet(f"s3://{BUCKET}/silver/user_watch_profile/")
content_perf  = spark.read.parquet(f"s3://{BUCKET}/silver/content_performance/")
subs_df       = spark.read.parquet(f"s3://{BUCKET}/bronze/subscriptions/")

print(f"  watch_profile rows: {watch_profile.count()}")

# ════════════════════════════════════════════════════
# Gold Table 1: DAU (Daily Active Users)
# ════════════════════════════════════════════════════
print("Building DAU metrics...")

dau = (
    watch_profile
    .groupBy("watch_date")
    .agg(
        F.countDistinct("user_id").alias("daily_active_users"),
        F.count("event_id").alias("total_watch_events"),
        F.round(F.avg("watched_mins"), 1).alias("avg_watch_mins"),
        F.round(F.avg("completion_pct"), 1).alias("avg_completion_pct"),
        F.sum("watched_mins").alias("total_watch_mins")
    )
    .withColumn("ingestion_time", F.lit(ingestion_time))
    .orderBy("watch_date")
)

dau.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dau_metrics/")
print(f"  DONE: dau_metrics - {dau.count()} days")

# ════════════════════════════════════════════════════
# Gold Table 2: Revenue Summary by Plan and Month
# ════════════════════════════════════════════════════
print("Building revenue summary...")

revenue = (
    subs_df
    .withColumn("month", F.date_format(F.col("start_date"), "yyyy-MM"))
    .groupBy("month","plan")
    .agg(
        F.count("subscription_id").alias("new_subscribers"),
        F.sum("amount_paid").alias("monthly_revenue"),
        F.round(F.avg("amount_paid"), 2).alias("avg_revenue_per_user")
    )
    .withColumn("ingestion_time", F.lit(ingestion_time))
    .orderBy("month","plan")
)

revenue.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/revenue_summary/")
print(f"  DONE: revenue_summary - {revenue.count()} rows")

# ════════════════════════════════════════════════════
# Gold Table 3: Top Content
# ════════════════════════════════════════════════════
print("Building top content...")

top_content = (
    content_perf
    .select(
        "content_id","title","type","genre",
        "language","rating","total_views",
        "avg_completion_pct","total_watch_mins","unique_viewers"
    )
    .withColumn("watch_hours", F.round(F.col("total_watch_mins") / 60, 1))
    .withColumn("ingestion_time", F.lit(ingestion_time))
    .orderBy(F.desc("total_views"))
)

top_content.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/top_content/")
print(f"  DONE: top_content - {top_content.count()} records")

# ════════════════════════════════════════════════════
# Gold Table 4: Device Analytics
# ════════════════════════════════════════════════════
print("Building device analytics...")

device_analytics = (
    watch_profile
    .groupBy("watch_device")
    .agg(
        F.countDistinct("user_id").alias("unique_users"),
        F.count("event_id").alias("total_sessions"),
        F.round(F.avg("watched_mins"), 1).alias("avg_watch_mins"),
        F.round(F.avg("completion_pct"), 1).alias("avg_completion_pct")
    )
    .withColumn("ingestion_time", F.lit(ingestion_time))
    .orderBy(F.desc("total_sessions"))
)

device_analytics.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/gold/device_analytics/"
)
print(f"  DONE: device_analytics - {device_analytics.count()} rows")

print("Gold Layer COMPLETE!")
job.commit()
