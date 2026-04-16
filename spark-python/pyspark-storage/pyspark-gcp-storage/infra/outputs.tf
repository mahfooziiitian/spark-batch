output "bucket_name" {
  description = "Name of the created GCS bucket"
  value       = google_storage_bucket.spark_bucket.name
}

output "service_account_email" {
  description = "Email of the Spark service account"
  value       = google_service_account.spark_sa.email
}

output "service_account_key" {
  description = "Base64-encoded service account JSON key"
  value       = google_service_account_key.spark_sa_key.private_key
  sensitive   = true
}
