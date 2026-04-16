terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "spark_bucket" {
  name     = var.bucket_name
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    project     = "pyspark-storage"
    environment = "dev"
  }
}

resource "google_service_account" "spark_sa" {
  account_id   = "pyspark-gcs-sa"
  display_name = "PySpark GCS Service Account"
}

resource "google_project_iam_member" "spark_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_service_account_key" "spark_sa_key" {
  service_account_id = google_service_account.spark_sa.name
}
