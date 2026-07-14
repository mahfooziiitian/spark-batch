# PySpark Storage

PySpark examples for reading and writing data to cloud and local object storage systems.

Each storage backend lives in its own sub-project with self-contained examples,
authentication patterns, and dependencies.

## Sub-Projects

| Sub-Project | Storage | Protocol | Description |
|-------------|---------|----------|-------------|
| [pyspark-local-s3](pyspark-local-s3/) | S3 (LocalStack) | `s3a://` | S3 access via LocalStack for local development |
| [pyspark-aws-s3](pyspark-aws-s3/) | AWS S3 | `s3a://` | S3 access on real AWS (IAM, STS, EMR) |
| [pyspark-azure-storage](pyspark-azure-storage/) | Azure Blob / ADLS Gen2 | `abfss://` | Azure storage with Service Principal & SAS Token |
| [pyspark-gcp-storage](pyspark-gcp-storage/) | Google Cloud Storage | `gs://` | GCS access with service account & ADC |
| [pyspark-hdfs](pyspark-hdfs/) | HDFS | `hdfs://` | Hadoop Distributed File System |
| [pyspark-minio](pyspark-minio/) | MinIO | `s3a://` | S3-compatible self-hosted object storage |

## Common Patterns

All sub-projects follow the same conventions:

- **Spark config approach** — credentials via `spark.hadoop.fs.*` properties in SparkSession builder
- **Hadoop config approach** — credentials via `sc._jsc.hadoopConfiguration().set(...)` at runtime
- **Environment variables** — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or equivalent
- **JAR dependencies** — managed through `spark.jars.packages` config

## Prerequisites

- Python 3.11+
- PySpark 3.5.x
- Java 11
