"""
OTT Platform - Watch Event Processor
Lambda Function: ott-watch-event-processor

Triggered by:
  - API Gateway GET /recommend
  - SQS queue: ott-watch-events-queue
  - Direct invocation

What it does:
  1. Parses watch event from any source
  2. Calculates engagement level (HIGH/MEDIUM/LOW)
  3. Generates notifications (binge watcher, long session)
  4. Saves record to DynamoDB

Environment:
  - Runtime: Python 3.12
  - Memory: 128 MB
  - Timeout: 30 seconds
  - Region: eu-north-1

Author: Akash
"""

import json
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
table    = dynamodb.Table('ott-user-engagement')


def lambda_handler(event, context):
    print(f"FULL EVENT: {json.dumps(event)}")

    # ── Parse event from any source ─────────────────────
    params = {}

    if event.get('user_id'):
        # Direct invocation or mapping template
        params = event
        print("Source: Direct invocation / mapping template")

    elif event.get('queryStringParameters'):
        # API Gateway proxy
        params = event['queryStringParameters']
        print("Source: API Gateway query params")

    elif event.get('Records'):
        # SQS trigger
        try:
            params = json.loads(event['Records'][0]['body'])
        except:
            params = {}
        print("Source: SQS")

    elif event.get('body'):
        # API Gateway POST
        try:
            params = json.loads(event['body'])
        except:
            params = {}
        print("Source: API Gateway body")

    print(f"Parsed params: {json.dumps(params)}")

    # ── Extract fields ───────────────────────────────────
    user_id        = str(params.get('user_id', 'UNKNOWN'))
    content_id     = str(params.get('content_id', 'UNKNOWN'))
    completion_pct = float(params.get('completion_pct', 0))
    watched_mins   = int(float(params.get('watched_mins', 0)))
    device         = str(params.get('device', 'unknown'))
    region         = str(params.get('region', 'unknown'))

    # ── Business Logic ───────────────────────────────────
    engagement_level = (
        "HIGH"   if completion_pct >= 80 else
        "MEDIUM" if completion_pct >= 50 else
        "LOW"
    )
    is_binge_watcher = completion_pct >= 80
    is_engaged       = completion_pct >= 50

    notifications = []
    if is_binge_watcher:
        notifications.append({
            'type'   : 'BINGE_WATCHER_BADGE',
            'message': f'User {user_id} earned Binge Watcher badge!'
        })
    if completion_pct == 100:
        notifications.append({
            'type'   : 'COMPLETION_REWARD',
            'message': f'User {user_id} completed {content_id}!'
        })
    if watched_mins > 120:
        notifications.append({
            'type'   : 'LONG_SESSION',
            'message': f'User {user_id} watched {watched_mins} mins'
        })

    processed_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    result = {
        'user_id'           : user_id,
        'content_id'        : content_id,
        'completion_pct'    : completion_pct,
        'watched_mins'      : watched_mins,
        'device'            : device,
        'region'            : region,
        'engagement_level'  : engagement_level,
        'is_binge_watcher'  : is_binge_watcher,
        'is_engaged'        : is_engaged,
        'processed_at'      : processed_at,
        'notifications'     : notifications,
        'notification_count': len(notifications),
        'lambda_request_id' : context.aws_request_id
    }

    # ── Save to DynamoDB ─────────────────────────────────
    try:
        table.put_item(Item={
            'user_id'           : user_id,
            'processed_at'      : processed_at,
            'content_id'        : content_id,
            'completion_pct'    : str(completion_pct),
            'watched_mins'      : watched_mins,
            'engagement_level'  : engagement_level,
            'is_binge_watcher'  : is_binge_watcher,
            'device'            : device,
            'region'            : region,
            'notification_count': len(notifications),
            'lambda_request_id' : context.aws_request_id
        })
        print(f"Saved to DynamoDB: {user_id}")
        result['dynamodb_saved'] = True

    except Exception as e:
        print(f"DynamoDB error: {str(e)}")
        result['dynamodb_saved'] = False
        result['dynamodb_error'] = str(e)

    print(f"Result: {json.dumps(result)}")

    return {
        'statusCode': 200,
        'headers'   : {'Content-Type': 'application/json'},
        'body'      : json.dumps(result)
    }
