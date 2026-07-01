"""
OTT Platform - Event Validator
Lambda Function: ott-validate-event

Used in: Step Functions state machine (State 1)

What it does:
  - Validates all required fields present
  - Checks completion_pct range (0-100)
  - Checks watched_mins is positive
  - Raises exception on validation failure
    (Step Functions catches and routes to ValidationFailed state)

Author: Akash
"""

import json
from datetime import datetime


def lambda_handler(event, context):
    print(f"Validating event: {json.dumps(event)}")

    # ── Check required fields ────────────────────────────
    required_fields = ['user_id', 'content_id', 'completion_pct', 'watched_mins']
    missing = [f for f in required_fields if f not in event]

    if missing:
        raise Exception(f"Missing required fields: {missing}")

    # ── Validate ranges ──────────────────────────────────
    completion_pct = float(event.get('completion_pct', 0))
    if not 0 <= completion_pct <= 100:
        raise Exception(f"Invalid completion_pct: {completion_pct}. Must be 0-100.")

    watched_mins = int(event.get('watched_mins', 0))
    if watched_mins < 0:
        raise Exception(f"Invalid watched_mins: {watched_mins}. Must be >= 0.")

    # ── Validate user_id format ──────────────────────────
    user_id = str(event.get('user_id', ''))
    if not user_id or user_id == 'UNKNOWN':
        raise Exception(f"Invalid user_id: {user_id}")

    print(f"Validation PASSED for user: {user_id}")

    # Return enriched event
    return {
        **event,
        "validation_status": "PASSED",
        "validated_at"     : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }
