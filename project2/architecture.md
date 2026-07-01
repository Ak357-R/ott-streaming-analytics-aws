# Architecture Documentation — Project 2
# OTT Real-Time Processing Engine

---

## System Architecture

```
Client / Mobile App
        |
        | HTTPS
        ↓
API Gateway (ott-recommendation-api)
        |
        | Lambda Proxy Integration
        ↓
Lambda: ott-watch-event-processor
        |              |
        |              ↓
        |         DynamoDB
        |    (ott-user-engagement)
        |
        ↓
SQS Queue (ott-watch-events-queue)
        |
        | Event Source Mapping
        ↓
Lambda: ott-watch-event-processor
        |
        ↓
Step Functions: ott-watch-event-pipeline
        |
        ├── State 1: ValidateEvent (Lambda)
        ├── State 2: ScoreEngagement (Lambda)
        ├── State 3: Choice (HIGH/MEDIUM/LOW)
        ├── State 4: Route to action
        └── State 5: PipelineSuccess


EC2 Instance (t3.micro - eu-north-1b Stockholm)
        |
        ├── Flask API (recommendation_api.py)
        ├── Gunicorn (3 workers)
        └── systemd (auto-restart, start on boot)
        |
        ↓ Containerized version:

Docker Image
        |
        ↓ Pushed to:
ECR (Elastic Container Registry)
        |
        ↓ Deployed on:
ECS Fargate (ott-cluster)
        └── ott-recommendation-service
            └── ott-recommendation-task
```

---

## Services Used

| Service | Purpose | Configuration |
|---------|---------|---------------|
| EC2 t3.micro | Flask API hosting | eu-north-1, systemd managed |
| AWS Lambda | Serverless event processing | Python 3.12, 128MB |
| Step Functions | Pipeline orchestration | Standard workflow |
| SQS | Message queuing | Standard queue |
| API Gateway | REST API endpoint | REST API, Regional |
| DynamoDB | Real-time storage | On-demand, user_id PK |
| Docker | Containerization | python:3.9-slim base |
| ECR | Container registry | Private repository |
| ECS Fargate | Serverless containers | 0.25 vCPU, 0.5GB |
| CloudWatch | Logging & monitoring | Auto-configured |
| IAM | Security & permissions | Least privilege |

---

## IAM Roles

```
ott-ec2-role (attached to EC2):
├── AmazonEC2ContainerRegistryFullAccess
└── AmazonSSMManagedInstanceCore

Lambda execution role (auto-created):
├── AWSLambdaBasicExecutionRole (CloudWatch logs)
├── AmazonSQSFullAccess (read SQS messages)
└── AmazonDynamoDBFullAccess (write engagement data)
```

---

## API Endpoints

### EC2 Flask API (Port 5000)
```
GET /health
GET /recommend?region=Karnataka&age=25&limit=5
GET /stats
```

### API Gateway (HTTPS)
```
Base URL: https://ylsa3xb4c0.execute-api.eu-north-1.amazonaws.com/prod

GET /recommend?user_id=USR001234&content_id=CNT0571&completion_pct=85&watched_mins=127&device=smart_tv&region=Karnataka
GET /health
```

---

## DynamoDB Table Design

```
Table: ott-user-engagement

Partition Key: user_id (String)
Sort Key:      processed_at (String)

Attributes:
  content_id        String
  completion_pct    String
  watched_mins      Number
  engagement_level  String (HIGH/MEDIUM/LOW)
  is_binge_watcher  Boolean
  device            String
  region            String
  notification_count Number
  lambda_request_id String

Access Patterns:
  - Get all events for a user (partition key query)
  - Get events in time range (sort key range query)
  - Scan all HIGH engagement users (filter expression)
```

---

## Step Functions Pipeline

```
Input:
{
  "user_id": "USR001234",
  "content_id": "CNT0571",
  "completion_pct": 85,
  "watched_mins": 127,
  "device": "smart_tv",
  "region": "Karnataka"
}

Flow:
ValidateEvent
  ↓ (pass) or → ValidationFailed
ScoreEngagement
  ↓
CheckEngagementLevel (Choice)
  ├── HIGH   → HighEngagementPath   → SEND_BINGE_NOTIFICATION
  ├── MEDIUM → MediumEngagementPath → UPDATE_RECOMMENDATIONS
  └── LOW    → LowEngagementPath    → LOG_AND_EXIT
                    ↓
              PipelineSuccess
```

---

## Architecture Decisions

### ADR-001: Lambda over EC2 for Event Processing
Chose Lambda because watch events are sporadic — paying
per invocation is cheaper than always-on EC2. Lambda
scales to thousands of concurrent events automatically.

### ADR-002: DynamoDB over RDS for Engagement Storage
Watch events need millisecond writes at high volume.
DynamoDB on-demand handles spikes without capacity planning.
Simple key-value access pattern — no SQL joins needed.

### ADR-003: SQS for Decoupling
During IPL, millions of simultaneous events would
overwhelm direct Lambda invocation. SQS buffers the spike,
Lambda processes at its own pace — zero events lost.

### ADR-004: ECS Fargate over EC2 for Containers
Fargate eliminates server management — no patching, no
capacity planning. Pay only when container runs. Same
Docker image works locally and in production.

---

## Cost Analysis

| Service | Usage | Cost |
|---------|-------|------|
| EC2 t3.micro | Free tier (750hrs/month) | $0.00 |
| Lambda | < 1M requests/month | $0.00 |
| Step Functions | < 4000 transitions/month | $0.00 |
| SQS | < 1M requests/month | $0.00 |
| API Gateway | < 1M requests/month | $0.00 |
| DynamoDB | < 25GB storage | $0.00 |
| ECR | ~500MB image storage | ~$0.05 |
| ECS Fargate | ~30 minutes testing | ~$0.01 |
| **Total** | | **~$0.06** |
