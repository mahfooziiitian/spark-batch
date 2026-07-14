# AWS Glue

AWS Glue is a fully managed, serverless ETL service built on Apache Spark.
It adds the **Glue Data Catalog** (a Hive-compatible metastore), the
`DynamicFrame` API, and built-in connectors for S3, RDS, Redshift, and more.

## Glue vs Vanilla PySpark

| Concept | PySpark | AWS Glue |
|---------|---------|----------|
| Entry point | `SparkSession` | `GlueContext` (wraps `SparkContext`) |
| Primary type | `DataFrame` | `DynamicFrame` (schema-flexible) |
| Job parameters | env vars / CLI args | `getResolvedOptions` |
| Metastore | External Hive / local | Glue Data Catalog |
| Job bookmarks | Manual | Built-in |

## Job Structure

```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args     = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# ... ETL logic ...

job.commit()  # marks success for job bookmarks
```

## Local Test

```bash
pip install aws-glue-sessions
python aws/glue_example.py \
  --JOB_NAME test \
  --input_path /tmp/input \
  --output_path /tmp/output
```

!!! tip "Fallback mode"
    The example script detects whether Glue libraries are available and falls back
    to plain PySpark automatically — no code changes needed for local runs.

## Create & Start a Glue Job

```bash
aws glue create-job \
  --name pyspark-setup-glue-job \
  --role AWSGlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://my-bucket/scripts/glue_example.py","PythonVersion":"3"}' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type G.1X

aws glue start-job-run --job-name pyspark-setup-glue-job \
  --arguments '{"--input_path":"s3://my-bucket/hr/","--output_path":"s3://my-bucket/output/"}'
```

## Worker Types

| Type | vCPU | Memory | Best for |
|------|------|--------|---------|
| `G.025X` | 0.25 | 2 GB | Small / dev |
| `G.1X` | 4 | 16 GB | Standard ETL |
| `G.2X` | 8 | 32 GB | Memory-intensive |
| `G.4X` | 16 | 64 GB | Large datasets |

## Full Example

```python title="aws/glue_example.py"
--8<-- "aws/glue_example.py"
```
