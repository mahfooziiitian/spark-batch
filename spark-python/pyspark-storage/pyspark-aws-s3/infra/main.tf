terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ── Unique suffix for globally-unique bucket name ────────────────────────────
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# ── S3 Bucket ────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "spark_demo" {
  bucket        = "spark-demo-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Project     = "pyspark-storage"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_versioning" "spark_demo" {
  bucket = aws_s3_bucket.spark_demo.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "spark_demo" {
  bucket = aws_s3_bucket.spark_demo.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "spark_demo" {
  bucket = aws_s3_bucket.spark_demo.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM User for Spark access ───────────────────────────────────────────────
resource "aws_iam_user" "spark" {
  name = "${var.project_name}-spark-user"

  tags = {
    Project     = "pyspark-storage"
    Environment = "dev"
  }
}

resource "aws_iam_user_policy" "spark_s3" {
  name = "${var.project_name}-s3-access"
  user = aws_iam_user.spark.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject",
        ]
        Resource = [
          aws_s3_bucket.spark_demo.arn,
          "${aws_s3_bucket.spark_demo.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_access_key" "spark" {
  user = aws_iam_user.spark.name
}
