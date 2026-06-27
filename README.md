# Clickstream Analytics Platform
### E-Commerce Behavioral Data Pipeline

A production-grade data platform processing e-commerce clickstream events through a full medallion architecture with AI-powered analytics.

**Stack:** Kafka · BigQuery · GCS · dbt · Airflow · LangSmith · Vertex AI · Terraform

---

## Architecture

```
[Python Event Simulator]
        │
        ▼
[Kafka / Confluent Cloud]   ← real-time streaming broker
        │
        ▼
[Python Consumer]
  ├── BigQuery Bronze (raw_events) ← partitioned by day, clustered by event_type
  └── GCS Parquet backup           ← partitioned by date for reprocessing
        │
        ▼
[dbt Transformation]        ← Week 2
  ├── Silver: stg_sessions, stg_events, stg_users
  └── Gold:   fct_purchases, fct_sessions, dim_products, dim_users
        │
        ▼
[Airflow DAG]               ← Week 3
  ingest → dbt run → dbt test → LLM eval
        │
        ▼
[LLM Agent + LangSmith]     ← Week 4
  Natural language queries over Gold layer
  Eval scoring tracked in LangSmith
```

---

## Week 1 Setup

### Prerequisites
- Python 3.11+
- Terraform >= 1.5
- GCP project with billing enabled
- Confluent Cloud account (free tier)

### Step 1 — Clone and install
```bash
git clone https://github.com/yourname/clickstream-analytics-platform
cd clickstream-analytics-platform
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Step 2 — Configure environment
```bash
cp .env.example .env
# Fill in your Confluent and GCP credentials
```

### Step 3 — Provision GCP infrastructure
```bash
cd terraform
terraform init
terraform plan -var="project_id=your-gcp-project-id"
terraform apply -var="project_id=your-gcp-project-id"
```

### Step 4 — Create Kafka topic
In Confluent Cloud UI:
1. Create a cluster (free tier)
2. Create topic: `clickstream_events` with 3 partitions
3. Generate API key and add to `.env`

### Step 5 — Run the pipeline
```bash
# Terminal 1: Start consumer (listens for events)
cd ingestion
python consumer.py

# Terminal 2: Run producer (generates events)
python producer.py
```

### Step 6 — Verify in BigQuery
```sql
SELECT event_type, COUNT(*) as event_count
FROM `your-project.bronze.raw_events`
GROUP BY event_type
ORDER BY event_count DESC;
```

---

## Event Schema

| Field | Type | Description |
|---|---|---|
| event_id | STRING | Unique event UUID |
| event_type | STRING | page_view, add_to_cart, purchase, etc. |
| session_id | STRING | User session identifier |
| user_id | STRING | Anonymous user identifier |
| timestamp | TIMESTAMP | Event time (UTC) |
| device_type | STRING | desktop, mobile, tablet |
| product_id | STRING | Product identifier (purchase events) |
| total_amount | FLOAT64 | Order total (purchase events) |
| _ingested_at | TIMESTAMP | Pipeline ingestion timestamp |
| _kafka_offset | INTEGER | Kafka message offset for lineage |

---

## Event Types

| Event | Trigger |
|---|---|
| session_start | User lands on site |
| page_view | User visits any page |
| product_view | User views a product detail page |
| add_to_cart | User adds item to cart |
| remove_from_cart | User removes item from cart |
| checkout_start | User begins checkout |
| purchase | User completes order |
| session_end | User leaves site |

---

## Coming Next

- **Week 2:** dbt Bronze → Silver → Gold models, tests, CI/CD
- **Week 3:** Airflow DAG orchestrating full pipeline
- **Week 4:** LLM agent + LangSmith evaluation framework
