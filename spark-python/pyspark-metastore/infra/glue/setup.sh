#!/usr/bin/env bash
set -euo pipefail

echo "=== AWS Glue Data Catalog Setup ==="

# Check AWS CLI
if ! command -v aws &>/dev/null; then
    echo "ERROR: AWS CLI is not installed." >&2
    echo "  Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
    exit 1
fi

# Check credentials
if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS credentials not configured." >&2
    echo "  Run: aws configure" >&2
    exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${GLUE_WAREHOUSE_BUCKET:-my-spark-warehouse}"
GLUE_DATABASE="${GLUE_DATABASE:-spark_metastore}"

echo "Region:    $AWS_REGION"
echo "Bucket:    s3://$S3_BUCKET"
echo "Database:  $GLUE_DATABASE"

# Create S3 bucket
echo "Creating S3 bucket..."
aws s3 mb "s3://$S3_BUCKET" --region "$AWS_REGION" 2>/dev/null || echo "Bucket already exists"

# Create Glue database
echo "Creating Glue database..."
aws glue create-database \
    --region "$AWS_REGION" \
    --database-input "{\"Name\": \"$GLUE_DATABASE\", \"Description\": \"PySpark Metastore demo database\"}" \
    2>/dev/null || echo "Database already exists"

echo ""
echo "=== Glue Catalog Ready ==="
echo ""
echo "Required Spark configs:"
echo "  spark.sql.catalogImplementation = hive"
echo "  spark.hadoop.hive.metastore.client.factory.class = com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
echo "  spark.sql.warehouse.dir = s3://$S3_BUCKET/warehouse"
echo ""
echo "Run example:"
echo "  SPARK_WAREHOUSE=s3://$S3_BUCKET/warehouse python src/metastore/glue/glue_metastore.py"
