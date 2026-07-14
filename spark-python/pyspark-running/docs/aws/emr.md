# Amazon EMR (EC2)

Amazon EMR (Elastic MapReduce) is AWS's managed Spark service on EC2.
EMR provisions the cluster, installs Spark, configures YARN, and integrates
natively with S3 via EMRFS — no Hadoop setup required.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Amazon EMR Cluster                                      │
│                                                          │
│  ┌────────────┐    YARN    ┌──────────┐  ┌──────────┐   │
│  │  Primary   │ ─────────► │  Core 1  │  │  Core N  │   │
│  │  (master)  │            │ (worker) │  │ (worker) │   │
│  └────────────┘            └──────────┘  └──────────┘   │
│         │  EMRFS (s3://)                                 │
└─────────┼────────────────────────────────────────────────┘
          ▼
     ┌─────────┐
     │ Amazon  │
     │   S3    │
     └─────────┘
```

## Prerequisites

- AWS CLI configured (`aws configure` or instance profile)
- S3 bucket for scripts, input data, output, and logs
- IAM role: `EMR_DefaultRole` (service) + `EMR_EC2_DefaultRole` (EC2 profile)

## Create a cluster

```bash
aws emr create-cluster \
  --name "PySpark Tutorial" \
  --release-label emr-7.1.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://my-bucket/emr-logs/ \
  --region us-east-1
```

Save the `ClusterID` returned (e.g. `j-XXXXXXXXXXXX`).

## Submit a job

=== "EMR Step (recommended)"
    Upload your script first, then add a step:

    ```bash
    aws s3 cp cluster/aws/emr/emr_example.py s3://my-bucket/scripts/

    aws emr add-steps --cluster-id j-XXXX \
      --steps Type=Spark,Name="PySpark ETL",ActionOnFailure=CONTINUE,\
    Args=[--deploy-mode,cluster,\
          --conf,spark.sql.adaptive.enabled=true,\
          s3://my-bucket/scripts/emr_example.py]
    ```

=== "spark-submit via SSH"
    ```bash
    # SSH to the primary node
    aws emr ssh --cluster-id j-XXXX --key-pair-file my-key.pem

    # Submit from inside the cluster
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --conf spark.sql.adaptive.enabled=true \
      s3://my-bucket/scripts/emr_example.py
    ```

=== "Local test"
    ```bash
    USE_LOCAL_DATA=true python cluster/aws/emr/emr_example.py
    ```

## S3 access patterns

!!! tip "Use `s3://` on EMR, `s3a://` off-cluster"
    On EMR, `s3://` uses EMRFS (the AWS-optimised S3 file system).
    Outside EMR (e.g. local dev), use `s3a://` with `hadoop-aws`:

    ```python
    # On EMR — EMRFS, no config needed
    df = spark.read.parquet("s3://my-bucket/data/")

    # Off-cluster — hadoop-aws + credentials
    spark.conf.set("spark.hadoop.fs.s3a.impl",
                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    df = spark.read.parquet("s3a://my-bucket/data/")
    ```

## EMR-optimised SparkSession

```python
spark = (SparkSession.builder
         .appName("emr-job")
         # AQE: auto-coalesces shuffle partitions
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         # Faster S3 output commits
         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
         # Speculative execution causes double-writes on S3
         .config("spark.speculation", "false")
         .getOrCreate())
```

## Monitor

```bash
# Watch step status
aws emr list-steps --cluster-id j-XXXX --query 'Steps[*].[Name,Status.State]'

# Fetch YARN application logs after completion
yarn logs -applicationId application_<timestamp>_<id>
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_PATH` | in-memory sample | `s3://bucket/prefix/` for input Parquet |
| `OUTPUT_PATH` | `/tmp/emr_output` | `s3://bucket/prefix/` for output |
| `USE_LOCAL_DATA` | — | Any value → use in-memory data |

## Example — `emr_example.py`

```python title="cluster/aws/emr/emr_example.py"
--8<-- "cluster/aws/emr/emr_example.py"
```
