"""
OTT Streaming Analytics Platform
Phase 11: Data Quality Checks

What it does:
- Runs 11 automated quality checks across 3 tables
- Quarantines failed records to separate S3 path
- Saves quality report as Parquet for Athena querying
- Prints summary with PASS/FAIL status

Checks Implemented:
  Users (4 checks):
    - No NULL user_id
    - Age between 18 and 100
    - Email contains @
    - No duplicate user_ids

  Watch Events (4 checks):
    - watched_mins > 0
    - completion_pct between 0 and 100
    - No NULL user_id
    - No NULL content_id

  Subscriptions (3 checks):
    - amount_paid >= 0
    - Plan is valid value (free/basic/standard/premium)
    - end_date is after start_date

Failed Records Location:
  s3://bucket/data_quality/failed_records/table/date/

Quality Report Location:
  s3://bucket/data_quality/reports/date/

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

BUCKET   = "ott-platform-akash-700398842715"
run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
run_date = datetime.utcnow().strftime("%Y-%m-%d")

print("Starting Data Quality Checks...")

# ── Helper ───────────────────────────────────────────
quality_results = []

def run_check(check_name, table, passed, total, failed_df=None):
    failed_count = total - passed
    status       = "PASS" if failed_count == 0 else "FAIL"
    pct          = round((passed / total) * 100, 1) if total > 0 else 0

    quality_results.append({
        "check_name"   : check_name,
        "table"        : table,
        "total_records": total,
        "passed"       : passed,
        "failed"       : failed_count,
        "pass_rate_pct": pct,
        "status"       : status,
        "run_time"     : run_time
    })

    print(f"  [{status}] {check_name}: {passed}/{total} ({pct}%)")

    if failed_df is not None and failed_count > 0:
        failed_df.write.mode("append").parquet(
            f"s3://{BUCKET}/data_quality/failed_records/{table}/{run_date}/"
        )

    return status

# ════════════════════════════════════════════════════
# 1. USERS Quality Checks
# ════════════════════════════════════════════════════
print("\nChecking users table...")
users_df    = spark.read.parquet(f"s3://{BUCKET}/bronze/users/")
total_users = users_df.count()

# Check 1: No NULL user_ids
null_users = users_df.filter(F.col("user_id").isNull())
run_check("users_no_null_user_id", "users",
          total_users - null_users.count(), total_users, null_users)

# Check 2: Age between 18 and 100
invalid_age = users_df.filter(
    (F.col("age") < 18) | (F.col("age") > 100) | F.col("age").isNull()
)
run_check("users_valid_age", "users",
          total_users - invalid_age.count(), total_users, invalid_age)

# Check 3: Email contains @
invalid_email = users_df.filter(
    ~F.col("email").contains("@") | F.col("email").isNull()
)
run_check("users_valid_email", "users",
          total_users - invalid_email.count(), total_users, invalid_email)

# Check 4: No duplicate user_ids
unique_users = users_df.select("user_id").distinct().count()
dup_status = "PASS" if unique_users == total_users else "FAIL"
quality_results.append({
    "check_name"   : "users_no_duplicates",
    "table"        : "users",
    "total_records": total_users,
    "passed"       : unique_users,
    "failed"       : total_users - unique_users,
    "pass_rate_pct": round((unique_users/total_users)*100, 1),
    "status"       : dup_status,
    "run_time"     : run_time
})
print(f"  [{dup_status}] users_no_duplicates: {unique_users}/{total_users}")

# ════════════════════════════════════════════════════
# 2. WATCH EVENTS Quality Checks
# ════════════════════════════════════════════════════
print("\nChecking watch_events table...")
watch_df    = spark.read.parquet(f"s3://{BUCKET}/bronze/watch_events/")
total_watch = watch_df.count()

# Check 1: watched_mins > 0
invalid_watch = watch_df.filter(
    (F.col("watched_mins") <= 0) | F.col("watched_mins").isNull()
)
run_check("watch_positive_duration", "watch_events",
          total_watch - invalid_watch.count(), total_watch, invalid_watch)

# Check 2: completion_pct between 0 and 100
invalid_pct = watch_df.filter(
    (F.col("completion_pct") < 0) | (F.col("completion_pct") > 100)
)
run_check("watch_valid_completion_pct", "watch_events",
          total_watch - invalid_pct.count(), total_watch, invalid_pct)

# Check 3: No NULL user_id
null_user_watch = watch_df.filter(F.col("user_id").isNull())
run_check("watch_no_null_user_id", "watch_events",
          total_watch - null_user_watch.count(), total_watch, null_user_watch)

# Check 4: No NULL content_id
null_content = watch_df.filter(F.col("content_id").isNull())
run_check("watch_no_null_content_id", "watch_events",
          total_watch - null_content.count(), total_watch, null_content)

# ════════════════════════════════════════════════════
# 3. SUBSCRIPTIONS Quality Checks
# ════════════════════════════════════════════════════
print("\nChecking subscriptions table...")
subs_df    = spark.read.parquet(f"s3://{BUCKET}/bronze/subscriptions/")
total_subs = subs_df.count()

# Check 1: amount_paid >= 0
invalid_amount = subs_df.filter(F.col("amount_paid") < 0)
run_check("subs_valid_amount", "subscriptions",
          total_subs - invalid_amount.count(), total_subs, invalid_amount)

# Check 2: Valid plan values
valid_plans = ["free","basic","standard","premium"]
invalid_plan = subs_df.filter(~F.col("plan").isin(valid_plans))
run_check("subs_valid_plan", "subscriptions",
          total_subs - invalid_plan.count(), total_subs, invalid_plan)

# Check 3: end_date after start_date
invalid_dates = subs_df.filter(F.col("end_date") <= F.col("start_date"))
run_check("subs_end_after_start", "subscriptions",
          total_subs - invalid_dates.count(), total_subs, invalid_dates)

# ════════════════════════════════════════════════════
# 4. Save Quality Report to S3
# ════════════════════════════════════════════════════
print("\nSaving quality report...")
report_df = spark.createDataFrame(quality_results)
report_df.write.mode("append").parquet(
    f"s3://{BUCKET}/data_quality/reports/{run_date}/"
)

# ── Print Summary ────────────────────────────────────
print("\n" + "="*50)
print("DATA QUALITY SUMMARY")
print("="*50)

total_checks  = len(quality_results)
passed_checks = sum(1 for r in quality_results if r["status"] == "PASS")
failed_checks = total_checks - passed_checks

for r in quality_results:
    icon = "PASS" if r["status"] == "PASS" else "FAIL"
    print(f"[{icon}] {r['check_name']}: {r['pass_rate_pct']}%")

print(f"\nResult: {passed_checks}/{total_checks} checks passed")

if failed_checks > 0:
    print(f"WARNING: {failed_checks} checks FAILED")
    print(f"Check: s3://{BUCKET}/data_quality/failed_records/")
else:
    print("ALL CHECKS PASSED - Data is clean!")

print("="*50)
job.commit()
