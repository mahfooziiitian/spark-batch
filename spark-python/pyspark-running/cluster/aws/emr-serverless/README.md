# PySpark on EMR Serverless

EMR Serverless runs Spark jobs without provisioning or managing EC2 instances.
You create an **application** once, then submit any number of **job runs** to it.
AWS auto-scales workers per job and bills only for vCPU-seconds actually used.

## Architecture

```
┌──────────────────────────────────────────────────┐
│  EMR Serverless Application                      │
│                                                  │
│  Job Run 1  ──► [ Auto-scaled Workers ] ──► S3  │
│  Job Run 2  ──► [ Auto-scaled Workers ] ──► S3  │
│  Job Run N  ──► [ Auto-scaled Workers ] ──► S3  │
└──────────────────────────────────────────────────┘
```

## Prerequisites

- AWS CLI ≥ 2.9 (`aws --version`)
- EMR Serverless runtime role with S3 and Glue permissions
- An S3 bucket for scripts, input, output, and logs

## One-time setup

### Create the application

```bash
aws emr-serverless create-application \
  --name pyspark-app \
  --type SPARK \
  --release-label emr-7.1.0 \
  --region us-east-1
```

Note the returned `applicationId` (e.g. `00abcdef1234`).

### IAM trust policy (runtime role)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "emr-serverless.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

## Upload your script

```bash
aws s3 cp emr_serverless_example.py s3://my-bucket/scripts/
```

## Submit a job run

```bash
aws emr-serverless start-job-run \
  --application-id 00abcdef1234 \
  --execution-role-arn arn:aws:iam::123456789012:role/EMRServerlessRole \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://my-bucket/scripts/emr_serverless_example.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://my-bucket/emr-serverless-logs/"
      }
    }
  }'
```

## Monitor

```bash
# Get job run status
aws emr-serverless get-job-run \
  --application-id 00abcdef1234 \
  --job-run-id <job-run-id>

# Download logs to inspect failures
aws s3 cp s3://my-bucket/emr-serverless-logs/ ./logs/ --recursive
```

## Vs EMR on EC2

| | EMR on EC2 | EMR Serverless |
|-|------------|----------------|
| Cluster management | Manual | None |
| Startup time | 5-10 min | ~30 sec |
| Cost model | Per-hour EC2 | Per vCPU-second |
| Best for | Long-running / interactive | Intermittent batch jobs |
| SSH access | Yes | No |

## Run the example locally

```bash
USE_LOCAL_DATA=true python emr_serverless_example.py
```
