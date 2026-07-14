---
applyTo: "**/*.py"
---

# MinIO Storage Instructions

## Protocol

Use `s3a://` — MinIO is S3-compatible and uses the same Hadoop AWS connector.

## Path Format

```
s3a://<BUCKET>/<PATH>
```

## JARs

```python
hadoop_aws = "3.3.4"
.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
```

## Critical Config

Path-style access **must** be enabled for MinIO:

```python
.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
```

## Environment Variables

```bash
MINIO_ENDPOINT       # default: http://localhost:9000
MINIO_ACCESS_KEY     # default: minioadmin
MINIO_SECRET_KEY     # default: minioadmin
```

## Docker Setup

```bash
docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
```

## Bucket Operations (mc CLI)

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/my-bucket
mc cp data.csv local/my-bucket/
```
