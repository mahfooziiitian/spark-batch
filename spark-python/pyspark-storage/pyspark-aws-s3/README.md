# AWS S3 and Spark

PySpark examples for reading and writing data to **Amazon S3** using the `s3a://` protocol.

## Prerequisites

- Java 11
- PySpark 3.5.x
- AWS credentials (IAM user, role, or instance profile)

## Library

S3A depends upon two JARs, alongside hadoop-common and its dependencies.

1. `hadoop-aws` JAR
2. `aws-java-sdk-bundle` JAR

The versions of `hadoop-common` and `hadoop-aws` must be identical.

## Authentication Methods

### Simple Credentials (via Spark config)

```python
spark = (SparkSession.builder
         .appName("aws-s3-demo")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
         .config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>")
         .config("spark.hadoop.fs.s3a.secret.key", "<SECRET_KEY>")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate())
```

### Environment Variables

```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

### IAM Instance Profile (EC2 / EMR)

```python
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider")
```

### Assumed Role (STS)

```python
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
.config("spark.hadoop.fs.s3a.assumed.role.arn",
        "arn:aws:iam::123456789012:role/my-role")
```

### Temporary Credentials (Session Token)

```python
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
.config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>")
.config("spark.hadoop.fs.s3a.secret.key", "<SECRET_KEY>")
.config("spark.hadoop.fs.s3a.session.token", "<SESSION_TOKEN>")
```

## Credential Providers

| Class | Description |
|-------|-------------|
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` | Simple name/secret credentials |
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider` | Session credentials |
| `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` | Assumed role credentials |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider` | EC2 metadata credentials |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | AWS environment variables |
| `com.amazonaws.auth.profile.ProfileCredentialsProvider` | Named profile credentials |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` | Anonymous access (public buckets) |

## EMR Note

On EMR you can use the `s3://` scheme instead of `s3a://`. The EMRFS connector handles
S3 access natively with consistent view and server-side encryption support.
