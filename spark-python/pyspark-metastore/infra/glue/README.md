# AWS Glue Data Catalog Infrastructure

AWS Glue Data Catalog is a **managed AWS service** — there is no local Docker setup required.

## Setup

Run the setup script to configure AWS resources:

```bash
./setup.sh
```

This will:

- Verify AWS CLI installation and credentials
- Create an S3 bucket for the Spark warehouse
- Create a Glue database for metadata

## Required IAM Permissions

The following IAM permissions are needed:

- `glue:CreateDatabase`
- `glue:GetDatabase`
- `glue:GetDatabases`
- `glue:CreateTable`
- `glue:GetTable`
- `glue:GetTables`
- `glue:UpdateTable`
- `glue:DeleteTable`
- `glue:GetPartitions`
- `glue:CreatePartition`
- `glue:BatchCreatePartition`
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`
- `s3:CreateBucket`

## Local Testing with LocalStack

For local development without AWS credentials, you can use [LocalStack](https://localstack.cloud/):

```bash
# Start LocalStack (provides local Glue + S3 emulation)
docker run -d --name localstack -p 4566:4566 localstack/localstack

# Point AWS CLI at LocalStack
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Then run setup.sh as usual
./setup.sh
```

## References

- [AWS Glue Data Catalog Documentation](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Using Glue Data Catalog with Spark](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-data-catalog-hive.html)
- [LocalStack Glue Support](https://docs.localstack.cloud/user-guide/aws/glue/)
