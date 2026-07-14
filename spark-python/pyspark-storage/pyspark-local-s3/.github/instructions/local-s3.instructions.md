---
applyTo: "**/*.py"
---

# LocalStack S3 Storage Instructions

## Protocol

Use `s3a://` — LocalStack emulates AWS S3 locally.

## Path Format

```
s3a://<BUCKET>/<PATH>
```

## JARs

```python
hadoop_aws = "3.2.2"
.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
```

## Critical Config

Path-style access **must** be enabled and endpoint must point to LocalStack:

```python
.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
.config("spark.hadoop.fs.s3a.access.key", "test")
.config("spark.hadoop.fs.s3a.secret.key", "test")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

## Credential Providers

Use `SimpleAWSCredentialsProvider` for explicit key/secret or
`ProfileCredentialsProvider` for `~/.aws/credentials`:

```python
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
```

## Environment Variables

```bash
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

## LocalStack Setup

```bash
localstack start -d
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 s3 cp data.csv s3://my-bucket/
```
