"""
OTT Streaming Analytics Platform
Phase 7: Bronze to Silver ETL Job

What it does:
- Reads Parquet files from S3 bronze layer
- Joins users + subscriptions + watch_events + content_catalog
- Applies business logic (age_group, is_completed, is_premium_user)
- Renames conflicting columns before join (device -> user_preferred_device / watch_device)
- Builds content_performance aggregation table
- Writes Parquet files to S3 silver layer

Key Learning: Always alias columns before joining tables
that share column names to avoid AnalysisException.

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

print("Starting Silver Layer Transformation...")

# ── Read Bronze Tables ───────────────────────────────
print("Reading bronze tables...")
users_df   = spark.read.parquet(f"s3://{BUCKET}/bronze/users/")
subs_df    = spark.read.parquet(f"s3://{BUCKET}/bronze/subscriptions/")
watch_df   = spark.read.parquet(f"s3://{BUCKET}/bronze/watch_events/")
content_df = spark.read.parquet(f"s3://{BUCKET}/bronze/content_catalog/")

print(f"  users:   {users_df.count()}")
print(f"  subs:    {subs_df.count()}")
print(f"  watch:   {watch_df.count()}")
print(f"  content: {content_df.count()}")

# ════════════════════════════════════════════════════
# FIX: Rename conflicting 'device' column BEFORE joining
# users.device     -> user_preferred_device
# watch_df.device  -> watch_device
# ════════════════════════════════════════════════════
users_clean = users_df.select(
    "user_id", "name", "age", "gender", "region",
    F.col("device").alias("user_preferred_device"),
    "signup_date", "is_active", "preferred_lang",
    "ingestion_timestamp", "layer"
)

watch_clean = watch_df.select(
    "event_id", "user_id", "content_id",
    "watch_start_time", "watched_mins", "completion_pct",
    F.col("device").alias("watch_device"),
    "quality", "buffering_events", "watch_date", "watch_hour"
)

# ════════════════════════════════════════════════════
# Silver Table 1: user_watch_profile
# Join users + subscriptions + watch_events + content
# ════════════════════════════════════════════════════
print("Building user_watch_profile...")

user_sub = users_clean.join(
    subs_df.select("user_id","plan","amount_paid","start_date","end_date"),
    on="user_id", how="left"
)

user_watch = user_sub.join(
    watch_clean,
    on="user_id", how="inner"
)

user_watch_full = user_watch.join(
    content_df.select("content_id","title","type","genre","language","rating"),
    on="content_id", how="left"
)

silver_df = (
    user_watch_full
    .withColumn("is_completed",
        F.when(F.col("completion_pct") >= 90, True).otherwise(False))
    .withColumn("is_premium_user",
        F.when(F.col("plan") == "premium", True).otherwise(False))
    .withColumn("age_group",
        F.when(F.col("age") < 25, "18-24")
         .when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 45, "35-44")
         .otherwise("45+"))
    .withColumn("silver_ingestion_time", F.lit(ingestion_time))
    .drop("ingestion_timestamp", "layer")
)

silver_df.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/silver/user_watch_profile/"
)
print(f"  DONE: user_watch_profile - {silver_df.count()} records")

# ════════════════════════════════════════════════════
# Silver Table 2: content_performance
# Per content aggregation
# ════════════════════════════════════════════════════
print("Building content_performance...")

content_perf = (
    watch_df
    .join(
        content_df.select("content_id","title","type","genre","language","rating"),
        on="content_id", how="left"
    )
    .groupBy("content_id","title","type","genre","language","rating")
    .agg(
        F.count("event_id").alias("total_views"),
        F.round(F.avg("completion_pct"), 1).alias("avg_completion_pct"),
        F.sum("watched_mins").alias("total_watch_mins"),
        F.countDistinct("user_id").alias("unique_viewers"),
        F.round(F.avg("buffering_events"), 2).alias("avg_buffering")
    )
    .withColumn("silver_ingestion_time", F.lit(ingestion_time))
)

content_perf.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/silver/content_performance/"
)
print(f"  DONE: content_performance - {content_perf.count()} records")

print("Silver Layer Transformation COMPLETE!")
job.commit()
