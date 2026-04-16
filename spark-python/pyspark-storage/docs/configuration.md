# Configuration Reference

Common Spark and Hadoop configuration patterns used across all storage providers.

## SparkSession Template

Every script follows this pattern:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))       # (1)!
         .config("spark.sql.adaptive.enabled", "true")              # (2)!
         .config("spark.sql.adaptive.coalescePartitions.enabled",
                 "true")
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.3.4")              # (3)!
         # ... storage-specific configs ...
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")                              # (4)!
```

1. Falls back to local mode — runs anywhere without modification.
2. Adaptive Query Execution for better shuffle performance.
3. Connector JARs — change per provider.
4. Suppress verbose Spark logs. Use `"ERROR"` in tests.

## Spark Config vs Hadoop Config

=== "Spark Config (build time)"
    Set at session creation. Immutable after `.getOrCreate()`.

    ```python
    spark = (SparkSession.builder
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .getOrCreate())
    ```

    !!! note
        Properties prefixed with `spark.hadoop.` are automatically propagated
        to the underlying Hadoop `Configuration`.

=== "Hadoop Config (runtime)"
    Set after session creation. Allows dynamic or per-path config.

    ```python
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    ```

    !!! note
        No `spark.hadoop.` prefix — you set Hadoop properties directly.

## Environment Variables

All scripts use environment variables with safe fallbacks:

```python
INPUT_PATH  = os.environ.get("INPUT_PATH")           # None → use sample data
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/output")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

### Provider Credentials

| Provider | Key Variable | Secret Variable |
|----------|-------------|-----------------|
| AWS S3 | `AWS_ACCESS_KEY_ID` | `AWS_SECRET_ACCESS_KEY` |
| Azure | `AZURE_STORAGE_ACCOUNT` | `AZURE_STORAGE_KEY` |
| GCP | `GOOGLE_APPLICATION_CREDENTIALS` | *(path to JSON keyfile)* |
| HDFS | *(none — OS user or Kerberos)* | `KRB5_KTNAME` |
| MinIO | `MINIO_ACCESS_KEY` | `MINIO_SECRET_KEY` |
| LocalStack | `AWS_ACCESS_KEY_ID` | `AWS_SECRET_ACCESS_KEY` |

## JAR Dependencies

| Provider | Package Coordinate | Version |
|----------|-------------------|---------|
| AWS S3 / MinIO / LocalStack | `org.apache.hadoop:hadoop-aws` | 3.3.4 |
| Azure Storage | `org.apache.hadoop:hadoop-azure` | 3.3.4 |
| GCS | `gcs-connector` (local JAR) | hadoop3-latest |
| HDFS | *(bundled with Spark)* | — |

```python
# Maven coordinate — resolved automatically
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

# Local JAR — for connectors not on Maven Central
.config("spark.jars", "/path/to/gcs-connector-hadoop3-latest.jar")
```

## FileSystem Implementations

| Protocol | Class |
|----------|-------|
| `s3a://` | `org.apache.hadoop.fs.s3a.S3AFileSystem` |
| `abfss://` | `org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem` |
| `gs://` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` |
| `hdfs://` | `org.apache.hadoop.hdfs.DistributedFileSystem` |

## Performance Configs

### Local Mode

```python
.config("spark.sql.shuffle.partitions", "4")
.config("spark.ui.enabled", "false")
```

### Cluster Mode (YARN / K8s / EMR)

```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.speculation", "false")  # disable for S3 / EMRFS
```

## Output Formats

!!! success "Prefer Parquet"
    Parquet is columnar, compressed, and supports predicate pushdown.

    ```python
    df.write.mode("overwrite").parquet(output_path)
    ```

!!! note "CSV for non-technical audiences"
    Use CSV only when the consumer cannot read Parquet.

    ```python
    df.write.mode("overwrite").option("header", True).csv(output_path)
    ```
