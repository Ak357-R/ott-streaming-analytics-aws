# OTT Real-Time Processing Engine on AWS

![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Docker](https://img.shields.io/badge/Docker-Container-blue)
![Status](https://img.shields.io/badge/Status-Complete-green)

> **Project 2** of the OTT Platform series.
> Project 1 (Data Lake + Analytics): [ott-streaming-analytics-aws](https://github.com/Ak357-R/ott-streaming-analytics-aws)

A **production-style real-time processing engine** built on AWS demonstrating serverless architecture, containerization, event-driven design, and microservices orchestration.

---

## Architecture

```
Client / Mobile App
        |
   HTTPS Request
        ↓
  API Gateway ──────────────────────────────────────┐
        |                                            |
   Lambda Proxy                               /health|
        ↓                                            |
  Lambda Function                                    |
  (watch_event_processor)                            |
        |              |                             |
        |         DynamoDB                           |
        |    (ott-user-engagement)                   |
        ↓                                            |
  SQS Queue                                          |
        |                                            |
  Lambda Trigger                                     |
        ↓                                            |
  Step Functions Pipeline                            |
  ├── ValidateEvent (Lambda)                         |
  ├── ScoreEngagement (Lambda)                       |
  ├── Choice: HIGH/MEDIUM/LOW                        |
  └── Route to action                                |
                                                     |
  EC2 / ECS Fargate ───────────────────────────────-┘
  (Flask Recommendation API)
```

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Compute | Amazon EC2 (t3.micro) |
| Serverless | AWS Lambda (Python 3.12) |
| Orchestration | AWS Step Functions |
| Messaging | Amazon SQS |
| API | Amazon API Gateway |
| Database | Amazon DynamoDB |
| Container Build | Docker |
| Container Registry | Amazon ECR |
| Container Deploy | Amazon ECS Fargate |
| Monitoring | Amazon CloudWatch |
| Security | AWS IAM (least privilege) |

---

## Project Structure

```
ott-realtime-engine-aws/
│
├── README.md
│
├── ec2/
│   ├── recommendation_api.py    # Flask API (3 endpoints)
│   └── ott-api.service          # systemd service file
│
├── lambda/
│   ├── watch_event_processor.py # Main processor + DynamoDB
│   ├── validate_event.py        # Step Functions: State 1
│   └── score_engagement.py      # Step Functions: State 2
│
├── step_functions/
│   └── pipeline_definition.json # State machine (5 states)
│
├── docker/
│   ├── Dockerfile               # Container definition
│   └── requirements.txt         # Python dependencies
│
└── docs/
    └── architecture.md          # Full architecture docs
```

---

## Services Built

### 1. EC2 Flask API
```
Endpoint: http://51.20.56.215:5000
Routes:
  GET /health
  GET /recommend?region=Karnataka&age=25
  GET /stats

Stack: Flask + Gunicorn (3 workers) + systemd
```

### 2. Lambda Functions (3)
```
ott-watch-event-processor  → Main processor + DynamoDB save
ott-validate-event         → Input validation for Step Functions
ott-score-engagement       → Engagement scoring for Step Functions
```

### 3. Step Functions Pipeline
```
5-state workflow:
ValidateEvent → ScoreEngagement → Choice → Route → Success

Branching:
  HIGH   → SEND_BINGE_NOTIFICATION
  MEDIUM → UPDATE_RECOMMENDATIONS
  LOW    → LOG_AND_EXIT
```

### 4. SQS Queue
```
Queue: ott-watch-events-queue
Trigger: Automatically invokes Lambda on new messages
Pattern: Producer/Consumer decoupling
```

### 5. API Gateway
```
API: ott-recommendation-api
URL: https://ylsa3xb4c0.execute-api.eu-north-1.amazonaws.com/prod
Routes: GET /recommend, GET /health
```

### 6. DynamoDB Table
```
Table: ott-user-engagement
PK:    user_id (String)
SK:    processed_at (String)
Mode:  On-demand (auto-scales)
```

### 7. ECS Fargate
```
Cluster: ott-cluster
Service: ott-recommendation-service
Image:   ECR → ott-recommendation-api:latest
CPU:     0.25 vCPU | Memory: 0.5 GB
```

---

## Live API Test

```bash
# Health check
curl http://51.20.56.215:5000/health

# Recommendations (EC2)
curl "http://51.20.56.215:5000/recommend?region=Karnataka&age=25"

# Engagement processing (API Gateway + Lambda + DynamoDB)
curl "https://ylsa3xb4c0.execute-api.eu-north-1.amazonaws.com/prod/recommend?user_id=USR001234&content_id=CNT0571&completion_pct=85&watched_mins=127&device=smart_tv&region=Karnataka"
```

---

## Sample Response

```json
{
  "user_id": "USR001234",
  "content_id": "CNT0571",
  "completion_pct": 85.0,
  "watched_mins": 127,
  "device": "smart_tv",
  "region": "Karnataka",
  "engagement_level": "HIGH",
  "is_binge_watcher": true,
  "is_engaged": true,
  "notifications": [
    {
      "type": "BINGE_WATCHER_BADGE",
      "message": "User USR001234 earned Binge Watcher badge!"
    },
    {
      "type": "LONG_SESSION",
      "message": "User USR001234 watched 127 mins"
    }
  ],
  "notification_count": 2,
  "dynamodb_saved": true
}
```

---

## Key Architecture Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Event processing | Lambda | Serverless, auto-scales, pay per invocation |
| Storage | DynamoDB | Millisecond writes, simple key-value pattern |
| Buffering | SQS | Decouples producer/consumer, handles spikes |
| Orchestration | Step Functions | Visual pipeline, built-in retry/error handling |
| Containers | ECS Fargate | No server management, consistent environment |
| Security | IAM Roles | Zero hardcoded credentials, least privilege |

---

## Cost Analysis

```
Total Project 2 AWS spend: ~$0.06

EC2:           $0.00 (free tier)
Lambda:        $0.00 (1M free requests/month)
Step Functions:$0.00 (4000 free transitions/month)
SQS:           $0.00 (1M free requests/month)
API Gateway:   $0.00 (1M free requests/month)
DynamoDB:      $0.00 (25GB free forever)
ECR:           ~$0.05 (image storage)
ECS Fargate:   ~$0.01 (30 min testing only)
```

---

## Setup Guide

### Prerequisites
- AWS Account
- AWS CLI configured
- Docker Desktop
- Python 3.9+

### Deploy EC2 API
```bash
# Launch EC2 t3.micro (Amazon Linux 2023)
# Connect via Session Manager
sudo yum install python3-pip -y
pip3 install flask gunicorn
# Copy recommendation_api.py to instance
sudo cp ott-api.service /etc/systemd/system/
sudo systemctl enable ott-api
sudo systemctl start ott-api
```

### Deploy Lambda Functions
```bash
# Create Lambda functions in AWS Console
# Runtime: Python 3.12
# Copy code from lambda/ folder
# Attach IAM policies: SQSFullAccess + DynamoDBFullAccess
```

### Deploy Step Functions
```bash
# Create state machine in AWS Console
# Copy step_functions/pipeline_definition.json
# Replace Lambda ARNs with your actual ARNs
```

### Deploy ECS Fargate
```bash
# Build and push Docker image
docker build -t ott-recommendation-api .
aws ecr get-login-password --region eu-north-1 | \
  docker login --username AWS --password-stdin \
  700398842715.dkr.ecr.eu-north-1.amazonaws.com
docker tag ott-recommendation-api:latest \
  700398842715.dkr.ecr.eu-north-1.amazonaws.com/ott-recommendation-api:latest
docker push \
  700398842715.dkr.ecr.eu-north-1.amazonaws.com/ott-recommendation-api:latest
# Create ECS cluster + task definition + service in console
```

---

## Related Project

**Project 1 — OTT Streaming Analytics Platform**
Data Lake + ETL Pipeline + Analytics Dashboard
[github.com/Ak357-R/ott-streaming-analytics-aws](https://github.com/Ak357-R/ott-streaming-analytics-aws)

---

## Author
**Akash** | AWS Cloud & Data Engineering Portfolio
Built on AWS | June 2026
