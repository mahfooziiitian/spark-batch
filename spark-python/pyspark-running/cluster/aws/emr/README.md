# PySpark on Amazon EMR (EC2)

Amazon EMR manages the full Spark cluster lifecycle on EC2. It runs YARN under
the hood, so you can use the same `spark-submit` commands as a self-managed
Hadoop cluster — plus tight S3, Glue Catalog, and IAM integration.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Amazon EMR Cluster (EC2)                                    │
│                                                              │
│  ┌────────────┐    YARN    ┌──────────┐  ┌──────────┐       │
│  │  Primary   │ ─────────► │  Core 1  │  │  Core N  │       │
│  │  (master)  │            │  (worker)│  │  (worker)│       │
│  └────────────┘            └──────────┘  └──────────┘       │
│         │                                                    │
└─────────┼────────────────────────────────────────────────────┘
          │  EMRFS (s3://)
          ▼
    ┌───────────┐
    │  Amazon   │
    │    S3     │
    └───────────┘
```

## Prerequisites

- AWS CLI configured (`aws configure`)
- An S3 bucket for input, output, and logs
- An EMR-compatible IAM role (`EMR_EC2_DefaultRole`)

## Create a cluster (AWS CLI)

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

Save the returned `ClusterID` (e.g. `j-XXXXXXXXXXXX`).

## Upload your script to S3

```bash
aws s3 cp emr_example.py s3://my-bucket/scripts/emr_example.py
```

## Submit a job as an EMR Step

```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXX \
  --steps Type=Spark,Name="PySpark ETL",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
      --conf,spark.sql.adaptive.enabled=true,\
      s3://my-bucket/scripts/emr_example.py]
```

## Submit via spark-submit (from the primary node)

```bash
# SSH to the primary node
aws emr ssh --cluster-id j-XXXXXXXXXXXX --key-pair-file my-key.pem

# Then submit
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  s3://my-bucket/scripts/emr_example.py
```

## Monitor

```bash
# List steps
aws emr list-steps --cluster-id j-XXXXXXXXXXXX

# Stream step logs
aws emr ssh --cluster-id j-XXXXXXXXXXXX --key-pair-file my-key.pem
# then: yarn logs -applicationId application_<id>
```

## Key EMR-specific configuration

| Config | Value | Purpose |
|--------|-------|---------|
| `spark.hadoop.fs.s3.impl` | `com.amazon.ws.emr.hadoop.fs.EmrFileSystem` | Enable EMRFS (default on EMR) |
| `spark.sql.adaptive.enabled` | `true` | AQE for dynamic partition tuning |
| `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` | `2` | Faster S3 commits |
| `spark.speculation` | `false` | Disable speculative execution (unstable on S3) |

## Run the example locally first

```bash
# Test without a cluster
AWS_DEFAULT_REGION=us-east-1 python emr_example.py

# Or fully local (no AWS needed)
USE_LOCAL_DATA=true python emr_example.py
```
