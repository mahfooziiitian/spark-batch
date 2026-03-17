# EMR Serverless

EMR Serverless runs Spark jobs without provisioning or managing EC2 instances.
Create an **application** once; submit any number of **job runs** to it.
Workers auto-scale per job and billing is per vCPU-second actually used.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  EMR Serverless Application                          │
│                                                      │
│  Job Run 1  ──► [ Auto-scaled Workers ] ──► S3      │
│  Job Run 2  ──► [ Auto-scaled Workers ] ──► S3      │
│  Job Run N  ──► [ Auto-scaled Workers ] ──► S3      │
└──────────────────────────────────────────────────────┘
```

No SSH. No YARN ResourceManager to manage. No idle cluster costs.

## Prerequisites

- AWS CLI ≥ 2.9
- IAM execution role with `s3:GetObject`, `s3:PutObject`, and Glue Catalog permissions
- S3 bucket for scripts, input, output, and logs

## One-time setup

### Create the application

```bash
aws emr-serverless create-application \
  --name pyspark-app \
  --type SPARK \
  --release-label emr-7.1.0 \
  --region us-east-1
```

Note the `applicationId` (e.g. `00abcdef1234`).

### IAM execution role trust policy

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

## Submit a job run

=== "AWS CLI"
    ```bash
    aws s3 cp cluster/aws/emr-serverless/emr_serverless_example.py \
              s3://my-bucket/scripts/

    aws emr-serverless start-job-run \
      --application-id 00abcdef1234 \
      --execution-role-arn arn:aws:iam::123456789012:role/EMRServerlessRole \
      --job-driver '{
        "sparkSubmit": {
          "entryPoint": "s3://my-bucket/scripts/emr_serverless_example.py",
          "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.sql.adaptive.enabled=true"
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

=== "Local test"
    ```bash
    USE_LOCAL_DATA=true python cluster/aws/emr-serverless/emr_serverless_example.py
    ```

## Monitor

```bash
# Poll job status
aws emr-serverless get-job-run \
  --application-id 00abcdef1234 \
  --job-run-id <job-run-id> \
  --query 'jobRun.state'

# Download driver logs
aws s3 cp s3://my-bucket/emr-serverless-logs/ ./logs/ --recursive
```

## EMR on EC2 vs EMR Serverless

| | EMR on EC2 | EMR Serverless |
|-|------------|----------------|
| Cluster management | Manual (provision / terminate) | None |
| Startup time | 5–10 min | ~30 sec |
| Cost model | Per-hour EC2 | Per vCPU-second |
| SSH / interactive | Yes | No |
| Best for | Long-running / interactive | Intermittent batch jobs |
| Maximum job size | Instance family limits | 400 vCPU default |

## Worker types & sizing

```bash
# Override resource allocation per job
"sparkSubmitParameters": "--conf spark.driver.cores=2
                          --conf spark.driver.memory=4g
                          --conf spark.executor.cores=4
                          --conf spark.executor.memory=8g
                          --conf spark.executor.instances=10"
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_PATH` | in-memory sample | `s3://bucket/prefix/` for input |
| `OUTPUT_PATH` | `/tmp/emr_serverless_output` | `s3://bucket/prefix/` for output |
| `USE_LOCAL_DATA` | — | Any value → skip S3 |

## Example — `emr_serverless_example.py`

```python title="cluster/aws/emr-serverless/emr_serverless_example.py"
--8<-- "cluster/aws/emr-serverless/emr_serverless_example.py"
```
