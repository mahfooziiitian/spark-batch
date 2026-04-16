# MinIO and Spark

PySpark examples for reading and writing data to **MinIO**, an S3-compatible
self-hosted object storage, using the `s3a://` protocol.

## Prerequisites

- Java 11
- PySpark 3.5.x
- MinIO server running (default: `http://localhost:9000`)

## Setup MinIO

### Docker

```bash
docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
```

### Create a bucket

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/my-bucket
mc cp data.csv local/my-bucket/
```

## Library

MinIO uses the same S3A connector as AWS S3:

1. `hadoop-aws` JAR
2. `aws-java-sdk-bundle` JAR

## Authentication

MinIO uses access key / secret key — same as S3A Simple credentials:

```python
spark = (SparkSession.builder
         .appName("minio-demo")
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.3.4")
         .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate())
```

`path.style.access` must be `true` for MinIO (virtual-hosted style is not supported
by default).

## Path Format

```
s3a://<BUCKET>/<PATH>
```
