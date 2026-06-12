"""
OTT Streaming Analytics Platform
Phase 5: Raw to Bronze ETL Job

What it does:
- Reads CSV/JSON files from S3 raw layer
- Casts data types correctly
- Removes duplicates
- Adds metadata columns (ingestion_timestamp, source_file, layer)
- Writes Parquet files to S3 bronze layer

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
from pyspark.sql.types import IntegerType, BooleanType
from datetime import datetime

# ── Init ────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

BUCKET = "ott-platform-akash-700398842715"
ingestion_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

print("Starting OTT Bronze Layer ETL Job...")

# ════════════════════════════════════════════════════
# 1. USERS
# ════════════════════════════════════════════════════
print("Processing users...")

users_df = spark.read.option("header","true").csv(
    f"s3://{BUCKET}/raw/users/users.csv"
)

users_bronze = (
    users_df
    .dropDuplicates(["user_id"])
    .withColumn("age",        F.col("age").cast(IntegerType()))
    .withColumn("is_active",  F.col("is_active").cast(BooleanType()))
    .withColumn("ingestion_timestamp", F.lit(ingestion_time))
    .withColumn("source_file", F.lit("raw/users/users.csv"))
    .withColumn("layer",       F.lit("bronze"))
)

users_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/users/"
)
print(f"  DONE: users - {users_bronze.count()} records written to bronze")


# ════════════════════════════════════════════════════
# 2. SUBSCRIPTIONS
# ════════════════════════════════════════════════════
print("Processing subscriptions...")

subs_df = spark.read.option("header","true").csv(
    f"s3://{BUCKET}/raw/subscriptions/subscriptions.csv"
)

subs_bronze = (
    subs_df
    .dropDuplicates(["subscription_id"])
    .withColumn("amount_paid", F.col("amount_paid").cast(IntegerType()))
    .withColumn("auto_renew",  F.col("auto_renew").cast(BooleanType()))
    .withColumn("ingestion_timestamp", F.lit(ingestion_time))
    .withColumn("source_file", F.lit("raw/subscriptions/subscriptions.csv"))
    .withColumn("layer",       F.lit("bronze"))
)

subs_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/subscriptions/"
)
print(f"  DONE: subscriptions - {subs_bronze.count()} records written to bronze")


# ════════════════════════════════════════════════════
# 3. CONTENT CATALOG
# ════════════════════════════════════════════════════
print("Processing content catalog...")

content_df = spark.read.option("header","true").csv(
    f"s3://{BUCKET}/raw/content_catalog/content_catalog.csv"
)

content_bronze = (
    content_df
    .dropDuplicates(["content_id"])
    .withColumn("release_year",  F.col("release_year").cast(IntegerType()))
    .withColumn("duration_mins", F.col("duration_mins").cast(IntegerType()))
    .withColumn("rating",        F.col("rating").cast("double"))
    .withColumn("is_original",   F.col("is_original").cast(BooleanType()))
    .withColumn("ingestion_timestamp", F.lit(ingestion_time))
    .withColumn("source_file", F.lit("raw/content_catalog/content_catalog.csv"))
    .withColumn("layer",       F.lit("bronze"))
)

content_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/content_catalog/"
)
print(f"  DONE: content_catalog - {content_bronze.count()} records written to bronze")


# ════════════════════════════════════════════════════
# 4. WATCH EVENTS (JSON)
# ════════════════════════════════════════════════════
print("Processing watch events...")

watch_df = spark.read.option("multiline","true").json(
    f"s3://{BUCKET}/raw/watch_events/watch_events.json"
)

watch_bronze = (
    watch_df
    .dropDuplicates(["event_id"])
    .withColumn("duration_mins",    F.col("duration_mins").cast(IntegerType()))
    .withColumn("watched_mins",     F.col("watched_mins").cast(IntegerType()))
    .withColumn("completion_pct",   F.col("completion_pct").cast("double"))
    .withColumn("buffering_events", F.col("buffering_events").cast(IntegerType()))
    .withColumn("watch_date",  F.to_date(F.col("watch_start_time")))
    .withColumn("watch_hour",  F.hour(F.col("watch_start_time")))
    .withColumn("ingestion_timestamp", F.lit(ingestion_time))
    .withColumn("source_file", F.lit("raw/watch_events/watch_events.json"))
    .withColumn("layer",       F.lit("bronze"))
)

watch_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/watch_events/"
)
print(f"  DONE: watch_events - {watch_bronze.count()} records written to bronze")

print("OTT Bronze Layer ETL Job COMPLETE!")
job.commit()
