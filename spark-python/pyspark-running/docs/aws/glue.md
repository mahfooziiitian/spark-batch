# AWS Glue

AWS Glue is a fully managed, serverless ETL service built on Apache Spark.
It adds the **Glue Data Catalog** (a managed Hive-compatible metastore),
the **DynamicFrame** API, built-in connectors, and job bookmarks on top of PySpark.

## Key differences from vanilla PySpark

| Concept | PySpark | AWS Glue |
|---------|---------|----------|
| Session entry point | `SparkSession` | `GlueContext` (wraps `SparkContext`) |
| Primary data structure | `DataFrame` | `DynamicFrame` |
| Schema enforcement | At read time | Per-record (flexible / self-describing) |
| Job parameters | env vars / CLI | `getResolvedOptions` |
| Metastore | External Hive | Glue Data Catalog |
| Incremental loads | Manual | Job bookmarks (built-in) |
| Serverless | No | Yes — workers auto-provision |

## Glue job anatomy

```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# 1. Parse job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

# 2. Bootstrap Glue context
sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session   # standard SparkSession also available
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)    # initialise job bookmarks

# 3. ETL logic
dyf = glue_ctx.create_dynamic_frame.from_catalog(
    database=args["input_database"],
    table_name=args["input_table"],
)
df = dyf.toDF()                     # DynamicFrame → DataFrame

# ... transforms ...

# 4. Write back
out_dyf = DynamicFrame.fromDF(df, glue_ctx, "output")
glue_ctx.write_dynamic_frame.from_options(
    frame=out_dyf,
    connection_type="s3",
    connection_options={"path": args["output_path"]},
    format="parquet",
)

# 5. Commit — advances job bookmarks
job.commit()
```

## Create a Glue job

```bash
aws glue create-job \
  --name pyspark-glue-demo \
  --role AWSGlueServiceRole \
  --command '{
    "Name":            "glueetl",
    "ScriptLocation":  "s3://my-bucket/scripts/glue_example.py",
    "PythonVersion":   "3"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type G.1X \
  --default-arguments '{
    "--job-language":                       "python",
    "--enable-metrics":                     "",
    "--enable-continuous-cloudwatch-log":   "true",
    "--input_database":                     "my_catalog_db",
    "--input_table":                        "raw_orders",
    "--output_path":                        "s3://my-bucket/output/"
  }'
```

## Start a run

=== "AWS CLI"
    ```bash
    aws s3 cp cluster/aws/glue/glue_example.py s3://my-bucket/scripts/

    aws glue start-job-run \
      --job-name pyspark-glue-demo \
      --arguments '{
        "--input_database": "my_catalog_db",
        "--input_table":    "raw_orders",
        "--output_path":    "s3://my-bucket/output/orders/"
      }'
    ```

=== "Local test (no Glue runtime)"
    ```bash
    USE_LOCAL_DATA=true python cluster/aws/glue/glue_example.py \
        --JOB_NAME local-test \
        --input_database ignored \
        --input_table    ignored \
        --output_path    /tmp/glue_output
    ```

## Monitor

```bash
# Poll the latest run
aws glue get-job-runs --job-name pyspark-glue-demo \
  --query 'JobRuns[0].[JobRunState,StartedOn,CompletedOn]'

# CloudWatch logs (if --enable-continuous-cloudwatch-log is set)
aws logs tail /aws-glue/jobs/output --follow
```

## Worker types

| Worker | vCPU | Memory | Best for |
|--------|------|--------|---------|
| `G.025X` | 0.25 | 2 GB | Dev / small jobs |
| `G.1X` | 4 | 16 GB | Standard ETL |
| `G.2X` | 8 | 32 GB | Memory-intensive |
| `G.4X` | 16 | 64 GB | Large datasets |
| `G.8X` | 32 | 128 GB | ML / very large |

## DynamicFrame vs DataFrame

!!! note
    Use **DynamicFrame** when reading from the Glue Catalog or mixed-schema sources.
    Convert to **DataFrame** (`dyf.toDF()`) for all standard PySpark transformations.
    Convert back (`DynamicFrame.fromDF(df, ctx, "name")`) only when writing via
    `write_dynamic_frame`.

```python
# DynamicFrame operations
dyf.printSchema()
dyf.count()
dyf.toDF()                # → DataFrame
dyf.resolveChoice(...)    # fix ambiguous types
dyf.dropNullFields()      # remove all-null columns

# Write DynamicFrame to S3
glue_ctx.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": "s3://bucket/output/", "partitionKeys": ["region"]},
    format="parquet",
)
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_LOCAL_DATA` | — | Any value → skip Glue Catalog, use in-memory data |

## Example — `glue_example.py`

```python title="cluster/aws/glue/glue_example.py"
--8<-- "cluster/aws/glue/glue_example.py"
```
