"""
OTT Streaming Analytics Platform
Data Generation Script

Generates realistic OTT platform datasets:
- 10,000 users
- 10,000 subscriptions
- 1,000 content catalog records
- 100,000 watch events

Uploads directly to S3 raw layer.

Author: Akash
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
import subprocess

random.seed(42)

# ── Config ──────────────────────────────────────────
NUM_USERS        = 10000
NUM_CONTENT      = 1000
NUM_WATCH_EVENTS = 100000
BUCKET_NAME      = "ott-platform-akash-700398842715"

# ── Reference Data ───────────────────────────────────
REGIONS   = ["Maharashtra","Karnataka","Tamil Nadu","Delhi",
             "Telangana","Gujarat","West Bengal","Rajasthan",
             "Uttar Pradesh","Kerala"]
DEVICES   = ["mobile","tablet","smart_tv","laptop","desktop"]
PLANS     = ["free","basic","standard","premium"]
GENRES    = ["Action","Comedy","Drama","Thriller","Romance",
             "Horror","Sci-Fi","Documentary","Animation","Crime"]
LANGUAGES = ["Hindi","English","Tamil","Telugu","Malayalam",
             "Kannada","Bengali","Marathi"]
CONTENT_TYPES = ["movie","series"]

def rand_date(start_year=2021, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(
        days=random.randint(0,(end-start).days)
    )).strftime("%Y-%m-%d")

def rand_datetime(start_year=2023, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(
        days=random.randint(0,(end-start).days),
        hours=random.randint(0,23),
        minutes=random.randint(0,59),
        seconds=random.randint(0,59)
    )).strftime("%Y-%m-%d %H:%M:%S")


# ── 1. Users ─────────────────────────────────────────
print("Generating users...")
users = []
for i in range(1, NUM_USERS + 1):
    users.append({
        "user_id"       : f"USR{i:06d}",
        "name"          : f"User_{i}",
        "email"         : f"user{i}@ottmail.com",
        "age"           : random.randint(18, 65),
        "gender"        : random.choice(["M","F","Other"]),
        "region"        : random.choice(REGIONS),
        "device"        : random.choice(DEVICES),
        "signup_date"   : rand_date(2021, 2023),
        "is_active"     : random.choice([True, False]),
        "preferred_lang": random.choice(LANGUAGES)
    })

with open("users.csv","w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=users[0].keys())
    writer.writeheader()
    writer.writerows(users)
print(f"  DONE: users.csv - {len(users):,} rows")


# ── 2. Subscriptions ─────────────────────────────────
print("Generating subscriptions...")
subs = []
for u in users:
    plan  = random.choice(PLANS)
    start = rand_date(2021, 2023)
    subs.append({
        "subscription_id": str(uuid.uuid4()),
        "user_id"        : u["user_id"],
        "plan"           : plan,
        "start_date"     : start,
        "end_date"       : (
            datetime.strptime(start, "%Y-%m-%d") +
            timedelta(days=random.choice([30,90,180,365]))
        ).strftime("%Y-%m-%d"),
        "amount_paid"    : {"free":0,"basic":99,"standard":199,"premium":349}[plan],
        "payment_method" : random.choice(["upi","credit_card","debit_card","netbanking"]),
        "auto_renew"     : random.choice([True, False])
    })

with open("subscriptions.csv","w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=subs[0].keys())
    writer.writeheader()
    writer.writerows(subs)
print(f"  DONE: subscriptions.csv - {len(subs):,} rows")


# ── 3. Content Catalog ────────────────────────────────
print("Generating content catalog...")
content = []
for i in range(1, NUM_CONTENT + 1):
    ctype = random.choice(CONTENT_TYPES)
    content.append({
        "content_id"    : f"CNT{i:04d}",
        "title"         : f"{'Movie' if ctype=='movie' else 'Show'}_Title_{i}",
        "type"          : ctype,
        "genre"         : random.choice(GENRES),
        "language"      : random.choice(LANGUAGES),
        "release_year"  : random.randint(2015, 2024),
        "duration_mins" : random.randint(20, 180),
        "rating"        : round(random.uniform(1.0, 10.0), 1),
        "num_seasons"   : random.randint(1,5) if ctype=="series" else None,
        "is_original"   : random.choice([True, False])
    })

with open("content_catalog.csv","w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=content[0].keys())
    writer.writeheader()
    writer.writerows(content)
print(f"  DONE: content_catalog.csv - {len(content):,} rows")


# ── 4. Watch Events ───────────────────────────────────
print("Generating watch events (this takes a moment)...")
user_ids    = [u["user_id"]    for u in users]
content_ids = [c["content_id"] for c in content]
events = []
for i in range(1, NUM_WATCH_EVENTS + 1):
    duration = random.randint(5, 180)
    watched  = random.randint(1, duration)
    events.append({
        "event_id"        : str(uuid.uuid4()),
        "user_id"         : random.choice(user_ids),
        "content_id"      : random.choice(content_ids),
        "watch_start_time": rand_datetime(2023, 2024),
        "duration_mins"   : duration,
        "watched_mins"    : watched,
        "completion_pct"  : round((watched / duration) * 100, 1),
        "device"          : random.choice(DEVICES),
        "region"          : random.choice(REGIONS),
        "buffering_events": random.randint(0, 10),
        "quality"         : random.choice(["480p","720p","1080p","4K"])
    })

with open("watch_events.json","w") as f:
    json.dump(events, f, indent=2)
print(f"  DONE: watch_events.json - {len(events):,} records")


# ── 5. Upload to S3 ───────────────────────────────────
print("\nUploading to S3...")
files = [
    ("users.csv",           f"s3://{BUCKET_NAME}/raw/users/users.csv"),
    ("subscriptions.csv",   f"s3://{BUCKET_NAME}/raw/subscriptions/subscriptions.csv"),
    ("content_catalog.csv", f"s3://{BUCKET_NAME}/raw/content_catalog/content_catalog.csv"),
    ("watch_events.json",   f"s3://{BUCKET_NAME}/raw/watch_events/watch_events.json"),
]
for local, s3_path in files:
    result = subprocess.run(
        ["aws","s3","cp", local, s3_path],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  UPLOADED: {local} -> {s3_path}")
    else:
        print(f"  FAILED: {local} - {result.stderr}")

print("\nALL DONE - Data generation complete!")
