terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Variables ────────────────────────────────────────────────────
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "dev"
}

# ── GCS Bucket — raw Parquet landing zone ────────────────────────
resource "google_storage_bucket" "clickstream_bucket" {
  name          = "${var.project_id}-clickstream-${var.environment}"
  location      = "US"
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }
  }

  labels = {
    environment = var.environment
    team        = "data-engineering"
  }
}

# ── BigQuery Datasets (Medallion) ────────────────────────────────
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = "bronze"
  description = "Raw ingested events — Bronze layer"
  location    = "US"

  labels = {
    layer       = "bronze"
    environment = var.environment
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = "silver"
  description = "Cleaned and standardized events — Silver layer"
  location    = "US"

  labels = {
    layer       = "silver"
    environment = var.environment
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "gold"
  description = "Business-ready facts and dimensions — Gold layer"
  location    = "US"

  labels = {
    layer       = "gold"
    environment = var.environment
  }
}

# ── Service Account — data pipeline runner ───────────────────────
resource "google_service_account" "pipeline_sa" {
  account_id   = "clickstream-pipeline-sa"
  display_name = "Clickstream Pipeline Service Account"
}

# BigQuery Data Editor — allows insert/update
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# BigQuery Job User — allows running queries
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# GCS Object Admin — allows read/write to bucket
resource "google_storage_bucket_iam_member" "gcs_admin" {
  bucket = google_storage_bucket.clickstream_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ── Outputs ──────────────────────────────────────────────────────
output "gcs_bucket_name" {
  value = google_storage_bucket.clickstream_bucket.name
}

output "pipeline_sa_email" {
  value = google_service_account.pipeline_sa.email
}

output "bq_datasets" {
  value = {
    bronze = google_bigquery_dataset.bronze.dataset_id
    silver = google_bigquery_dataset.silver.dataset_id
    gold   = google_bigquery_dataset.gold.dataset_id
  }
}
