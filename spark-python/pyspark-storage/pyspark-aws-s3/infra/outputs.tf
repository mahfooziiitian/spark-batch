output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.spark_demo.bucket
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.spark_demo.arn
}

output "access_key_id" {
  description = "IAM access key ID for Spark user"
  value       = aws_iam_access_key.spark.id
  sensitive   = true
}

output "secret_access_key" {
  description = "IAM secret access key for Spark user"
  value       = aws_iam_access_key.spark.secret
  sensitive   = true
}
