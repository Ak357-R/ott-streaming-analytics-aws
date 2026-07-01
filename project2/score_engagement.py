"""
OTT Platform - Engagement Scorer
Lambda Function: ott-score-engagement

Used in: Step Functions state machine (State 2)

What it does:
  - Calculates engagement score (0-100)
    60% weight: completion percentage
    40% weight: watch duration (capped at 2 hours)
  - Assigns engagement level: HIGH / MEDIUM / LOW
  - Sets should_notify flag for downstream routing

Scoring Formula:
  completion_score = completion_pct * 0.6
  duration_score   = min(watched_mins/120 * 100, 100) * 0.4
  engagement_score = completion_score + duration_score

Author: Akash
"""

import json
from datetime import datetime


def lambda_handler(event, context):
    print(f"Scoring engagement: {json.dumps(event)}")

    completion_pct = float(event.get('completion_pct', 0))
    watched_mins   = int(event.get('watched_mins', 0))

    # ── Calculate engagement score ───────────────────────
    completion_score = completion_pct * 0.6
    duration_score   = min(watched_mins / 120 * 100, 100) * 0.4
    engagement_score = round(completion_score + duration_score, 1)

    # ── Determine engagement level ───────────────────────
    if completion_pct >= 80:
        engagement_level = "HIGH"
    elif completion_pct >= 50:
        engagement_level = "MEDIUM"
    else:
        engagement_level = "LOW"

    print(f"Score: {engagement_score} | Level: {engagement_level}")

    return {
        **event,
        "engagement_score": engagement_score,
        "engagement_level": engagement_level,
        "should_notify"   : completion_pct >= 80,
        "scored_at"       : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }
