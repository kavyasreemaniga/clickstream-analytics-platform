"""
Configuration — load from environment variables.
Copy .env.example to .env and fill in your values.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka / Confluent ────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "clickstream_events")

# ── GCP ─────────────────────────────────────────────────────────
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "bronze")
GCS_BUCKET = os.getenv("GCS_BUCKET")
