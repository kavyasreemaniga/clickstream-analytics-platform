"""
Clickstream Kafka Consumer → BigQuery Bronze Loader

Reads events from Kafka and batch-inserts into BigQuery raw_events table.
Also writes Parquet files to GCS for backup / reprocessing.
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Optional
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery, storage
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from config import KAFKA_CONFIG, TOPIC_NAME, BQ_PROJECT, BQ_DATASET_BRONZE, GCS_BUCKET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── BigQuery schema ──────────────────────────────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("session_id", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("browser", "STRING"),
    bigquery.SchemaField("ip_address", "STRING"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("page", "STRING"),
    bigquery.SchemaField("page_url", "STRING"),
    bigquery.SchemaField("referrer", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("utm_campaign", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("subtotal", "FLOAT64"),
    bigquery.SchemaField("tax", "FLOAT64"),
    bigquery.SchemaField("shipping", "FLOAT64"),
    bigquery.SchemaField("total_amount", "FLOAT64"),
    bigquery.SchemaField("payment_method", "STRING"),
    bigquery.SchemaField("coupon_used", "STRING"),
    bigquery.SchemaField("cart_total", "FLOAT64"),
    bigquery.SchemaField("cart_item_count", "INTEGER"),
    bigquery.SchemaField("session_duration_seconds", "INTEGER"),
    bigquery.SchemaField("total_page_views", "INTEGER"),
    bigquery.SchemaField("bounced", "BOOLEAN"),
    bigquery.SchemaField("time_on_page_seconds", "INTEGER"),
    bigquery.SchemaField("scroll_depth_pct", "INTEGER"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_kafka_offset", "INTEGER"),
    bigquery.SchemaField("_kafka_partition", "INTEGER"),
]

TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET_BRONZE}.raw_events"


# ── BigQuery helpers ─────────────────────────────────────────────
def ensure_table(client: bigquery.Client):
    """Create Bronze table if it does not exist."""
    dataset_ref = client.dataset(BQ_DATASET_BRONZE)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)
        logger.info(f"Created dataset: {BQ_DATASET_BRONZE}")

    table = bigquery.Table(TABLE_ID, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )
    table.clustering_fields = ["event_type", "user_id"]

    try:
        client.get_table(TABLE_ID)
        logger.info(f"Table exists: {TABLE_ID}")
    except Exception:
        client.create_table(table)
        logger.info(f"Created table: {TABLE_ID}")


def flatten_event(event: dict, offset: int, partition: int) -> dict:
    """Flatten nested event fields into BQ row."""
    row = {field.name: None for field in BQ_SCHEMA}
    for key, value in event.items():
        if key in row:
            row[key] = value
    row["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    row["_kafka_offset"] = offset
    row["_kafka_partition"] = partition
    return row


def insert_to_bq(client: bigquery.Client, rows: list) -> int:
    """Batch insert rows into BigQuery. Returns error count."""
    errors = client.insert_rows_json(TABLE_ID, rows)
    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
        return len(errors)
    return 0


# ── GCS Parquet backup ───────────────────────────────────────────
def write_parquet_to_gcs(rows: list, bucket_name: str, partition_date: str):
    """Write batch as Parquet to GCS for backup and reprocessing."""
    try:
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        local_path = f"/tmp/events_{partition_date}_{datetime.now().strftime('%H%M%S')}.parquet"
        pq.write_table(table, local_path)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        gcs_path = f"bronze/raw_events/date={partition_date}/{os.path.basename(local_path)}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        logger.info(f"Written to GCS: gs://{bucket_name}/{gcs_path}")
        os.remove(local_path)
    except Exception as e:
        logger.warning(f"GCS write failed (non-fatal): {e}")


# ── Main consumer loop ───────────────────────────────────────────
def run(batch_size: int = 500, timeout_seconds: int = 30):
    consumer_config = {
        **KAFKA_CONFIG,
        "group.id": "clickstream-bq-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(consumer_config)
    bq_client = bigquery.Client(project=BQ_PROJECT)
    ensure_table(bq_client)

    consumer.subscribe([TOPIC_NAME])
    logger.info(f"Subscribed to topic: {TOPIC_NAME}")

    batch = []
    total_inserted = 0
    error_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush remaining batch on idle
                if batch:
                    logger.info(f"Flushing remaining {len(batch)} events")
                    errs = insert_to_bq(bq_client, batch)
                    if errs == 0:
                        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        write_parquet_to_gcs(batch, GCS_BUCKET, partition_date)
                        consumer.commit()
                        total_inserted += len(batch)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                row = flatten_event(event, msg.offset(), msg.partition())
                batch.append(row)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping malformed message: {e}")
                error_count += 1
                continue

            # Flush batch
            if len(batch) >= batch_size:
                logger.info(f"Inserting batch of {len(batch)} events to BigQuery")
                errs = insert_to_bq(bq_client, batch)
                if errs == 0:
                    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    write_parquet_to_gcs(batch, GCS_BUCKET, partition_date)
                    consumer.commit()
                    total_inserted += len(batch)
                    logger.info(f"Total inserted: {total_inserted}")
                else:
                    error_count += errs
                batch = []

    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total inserted: {total_inserted} | Errors: {error_count}")


if __name__ == "__main__":
    run(batch_size=500)
