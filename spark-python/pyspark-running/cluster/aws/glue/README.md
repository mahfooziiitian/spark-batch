# AWS Glue

AWS Glue is a fully managed, serverless ETL service built on top of Apache Spark.
It adds the **Glue Data Catalog** (a managed Hive-compatible metastore), a
**DynamicFrame** API, and built-in connectors for S3, RDS, Redshift, and more.

## Key differences from vanilla PySpark

| Concept | PySpark | AWS Glue |
|---------|---------|----------|
| Session entry point | `SparkSession` | `GlueContext` (wraps `SparkContext`) |
| Primary data structure | `DataFrame` | `DynamicFrame` (schema-flexible) |
| Schema | Fixed at read time | Inferred per record |
| Job parameters | env vars / CLI args | `getResolvedOptions` |
| Metastore | External Hive / local | Glue Data Catalog |
| Job bookmarks | Manual | Built-in (track processed partitions) |

## Glue job structure

```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# ... ETL logic ...

job.commit()   # required — marks job success for bookmarks
```

## Create a Glue job (AWS CLI)

```bash
aws glue create-job \
  --name pyspark-glue-job \
  --role AWSGlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://my-bucket/scripts/glue_example.py","PythonVersion":"3"}' \
  --default-arguments '{
    "--job-language": "python",
    "--enable-metrics": "",
    "--enable-continuous-cloudwatch-log": "true",
    "--input_path": "s3://my-bucket/input/",
    "--output_path": "s3://my-bucket/output/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type G.1X
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name pyspark-glue-job \
  --arguments '{"--input_path":"s3://my-bucket/input/","--output_path":"s3://my-bucket/output/"}'
```

## Monitor

```bash
# Get run status
aws glue get-job-run --job-name pyspark-glue-job --run-id <run-id>

# List all runs
aws glue get-job-runs --job-name pyspark-glue-job
```

## Worker types

| Type | vCPU | Memory | Best for |
|------|------|--------|---------|
| `G.025X` | 0.25 | 2 GB | Small / dev |
| `G.1X` | 4 | 16 GB | Standard ETL |
| `G.2X` | 8 | 32 GB | Memory-intensive |
| `G.4X` | 16 | 64 GB | Large datasets |
| `G.8X` | 32 | 128 GB | ML / very large |

## Run locally (Glue local libraries)

```bash
pip install aws-glue-sessions
python glue_example.py --JOB_NAME test --input_path /tmp/input --output_path /tmp/output
```
