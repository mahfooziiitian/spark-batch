---
applyTo: "**/*.py"
---

# AWS S3 Storage Instructions

## Protocol

Use `s3a://` for all AWS S3 access. On EMR, `s3://` (EMRFS) is also available.

## JARs

```python
hadoop_aws = "3.3.4"
.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
```

The `hadoop-aws` version must match the Hadoop version bundled with Spark.

## Authentication Providers

| Class | Use Case |
|-------|----------|
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` | Static access key / secret key |
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider` | Session token (STS) |
| `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` | Cross-account assumed role |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider` | EC2 / EMR instance profile |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | `AWS_ACCESS_KEY_ID` env var |
| `com.amazonaws.auth.profile.ProfileCredentialsProvider` | `~/.aws/credentials` named profile |

## Environment Variables

```bash
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN       # for temporary credentials
AWS_ROLE_ARN            # for assumed role
```

## EMR-Specific Configs

```python
.config("spark.speculation", "false")
.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
```
