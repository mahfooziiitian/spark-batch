---
applyTo: "**/*.py"
---

# PySpark Storage Code Instructions

## SparkSession with Storage JARs

Always load connector JARs via `spark.jars.packages` (Maven coordinates) so
dependencies resolve automatically:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("storage-job")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.3.4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

Use `spark.jars` (local path) only when a JAR is not published to Maven Central
(e.g. the GCS connector).

## Two Configuration Approaches

Every storage sub-project should demonstrate both approaches:

### 1. Spark Config (preferred for new code)

Set `spark.hadoop.fs.*` properties on the SparkSession builder:

```python
.config("spark.hadoop.fs.s3a.access.key", access_key)
.config("spark.hadoop.fs.s3a.secret.key", secret_key)
```

### 2. Hadoop Config (runtime)

Set properties directly on the Hadoop `Configuration` object:

```python
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
```

## Credentials from Environment Variables

Never hard-code credentials. Always read from env vars with empty-string fallbacks:

```python
access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
```

For local development tools (LocalStack, MinIO), use obvious placeholder defaults:

```python
access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
```

## Connector JAR Versions

Keep the connector JAR version in a variable at the top of each script:

```python
hadoop_aws = "3.3.4"
.config("spark.jars.packages",
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
```

## Protocol Reference

| Provider   | Protocol   | Connector JAR                         |
|------------|------------|---------------------------------------|
| AWS S3     | `s3a://`   | `org.apache.hadoop:hadoop-aws`        |
| Azure ADLS | `abfss://` | `org.apache.hadoop:hadoop-azure`      |
| GCS        | `gs://`    | `gcs-connector-hadoop3-*-shaded.jar`  |
| HDFS       | `hdfs://`  | Bundled with Spark                    |
| MinIO      | `s3a://`   | `org.apache.hadoop:hadoop-aws`        |
| LocalStack | `s3a://`   | `org.apache.hadoop:hadoop-aws`        |

## S3A-Compatible Endpoints (MinIO, LocalStack)

When targeting a non-AWS S3 endpoint, always set path-style access:

```python
.config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

## Read / Write Pattern

```python
input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
output_path = os.environ.get("OUTPUT_PATH", "s3a://my-bucket/output")

df = spark.read.option("header", True).csv(input_path)
df.write.mode("overwrite").parquet(output_path)
```

## spark.stop()

Always call `spark.stop()` at the end of standalone storage scripts.
