"""
PySpark on AWS Glue
====================
AWS Glue uses GlueContext (wraps SparkContext) and adds DynamicFrame,
which is schema-flexible: each record can have different fields.

This script runs as a Glue ETL job. For local testing, install:
    pip install aws-glue-sessions

Create the Glue job:
    aws glue create-job \\
        --name pyspark-glue-demo \\
        --role AWSGlueServiceRole \\
        --command '{"Name":"glueetl","ScriptLocation":"s3://my-bucket/scripts/glue_example.py","PythonVersion":"3"}' \\
        --glue-version "4.0" \\
        --number-of-workers 5 \\
        --worker-type G.1X \\
        --default-arguments '{
            "--input_database": "my_catalog_db",
            "--input_table": "raw_orders",
            "--output_path": "s3://my-bucket/output/orders_clean/"
        }'

Start a run:
    aws glue start-job-run --job-name pyspark-glue-demo

Local test (in-memory, no Glue runtime needed):
    USE_LOCAL_DATA=true python glue_example.py \\
        --JOB_NAME local-test \\
        --input_database ignored \\
        --input_table ignored \\
        --output_path /tmp/glue_output
"""

import sys
import os

USE_LOCAL_DATA = os.environ.get("USE_LOCAL_DATA")

# ---------------------------------------------------------------------------
# Runtime detection: Glue runtime vs local Python
# On Glue, awsglue libraries are pre-installed.
# For local testing we fall back to a plain SparkSession.
# ---------------------------------------------------------------------------
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "input_database", "input_table", "output_path"]
    )
    sc       = SparkContext()
    glue_ctx = GlueContext(sc)
    spark    = glue_ctx.spark_session
    job      = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)
    IS_GLUE  = True
    print("Running inside AWS Glue runtime")

except ModuleNotFoundError:
    # Local fallback — plain PySpark
    from pyspark.sql import SparkSession

    args = {
        "JOB_NAME":       "local-test",
        "input_database": "n/a",
        "input_table":    "n/a",
        "output_path":    os.environ.get("OUTPUT_PATH", "/tmp/glue_output"),
    }
    spark    = (SparkSession.builder
                .appName("glue-local-test")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate())
    glue_ctx = None
    job      = None
    IS_GLUE  = False
    print("Running in LOCAL fallback mode (awsglue not found)")

spark.sparkContext.setLogLevel("WARN")
print(f"Job name   : {args['JOB_NAME']}")
print(f"Spark ver  : {spark.version}")

# ---------------------------------------------------------------------------
# Ingest
# On Glue: read from the Glue Data Catalog (Hive-compatible metastore).
# Locally:  fall back to an in-memory DataFrame.
# ---------------------------------------------------------------------------
if IS_GLUE and not USE_LOCAL_DATA:
    # Read via GlueContext — returns a DynamicFrame
    dyf = glue_ctx.create_dynamic_frame.from_catalog(
        database=args["input_database"],
        table_name=args["input_table"],
        transformation_ctx="dyf_raw",
    )
    # Convert DynamicFrame → DataFrame for standard PySpark operations
    raw = dyf.toDF()
    print(f"Rows from Glue Catalog: {raw.count()}")
else:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    schema = StructType([
        StructField("order_id",    IntegerType(), False),
        StructField("customer_id", StringType(),  False),
        StructField("product",     StringType(),  False),
        StructField("quantity",    IntegerType(),  True),
        StructField("unit_price",  DoubleType(),   True),
        StructField("country",     StringType(),   True),
    ])
    sample_rows = [
        (1001, "C001", "Widget-A", 5,  9.99,  "US"),
        (1002, "C002", "Widget-B", 2, 19.99,  "UK"),
        (1003, "C001", "Widget-C", 8,  4.99,  "US"),
        (1004, "C003", "Widget-A", 3,  9.99, "DE"),
        (1005, "C002", "Widget-C", 1,  4.99,  "UK"),
        (1006, "C004", "Widget-B", 6, 19.99,  "AU"),
        (1007, "C003", "Widget-B", 4, 19.99,  "DE"),
        (1008, "C001", "Widget-A", 10, 9.99,  "US"),
        (1009, None,   "Widget-C", 2,  4.99,  "US"),   # bad row — null customer
        (1010, "C005", "Widget-A", -1, 9.99,  "AU"),   # bad row — negative qty
    ]
    raw = spark.createDataFrame(sample_rows, schema)
    print(f"Rows from in-memory sample: {raw.count()}")

# ---------------------------------------------------------------------------
# Clean
# Glue DynamicFrames have a .resolveChoice() / .dropNullFields() API, but
# once converted to DataFrames the standard PySpark API applies.
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F

cleaned = (raw
           .filter(F.col("customer_id").isNotNull())
           .filter(F.col("quantity") > 0)
           .filter(F.col("unit_price") > 0)
           .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)))

print(f"Rows after cleaning: {cleaned.count()}")

# ---------------------------------------------------------------------------
# Aggregate — revenue by product and country
# ---------------------------------------------------------------------------
agg = (cleaned
       .groupBy("country", "product")
       .agg(
           F.count("order_id").alias("num_orders"),
           F.sum("quantity").alias("total_units"),
           F.round(F.sum("revenue"), 2).alias("total_revenue"),
       )
       .orderBy("country", F.desc("total_revenue")))

print("\n=== Revenue by Country & Product ===")
agg.show()

# ---------------------------------------------------------------------------
# Write
# On Glue: convert DataFrame → DynamicFrame and use write_dynamic_frame
# for Glue Catalog + S3 integration.
# Locally: plain DataFrame.write.parquet().
# ---------------------------------------------------------------------------
OUTPUT_PATH = args["output_path"]

if IS_GLUE and glue_ctx:
    agg_dyf = DynamicFrame.fromDF(agg, glue_ctx, "agg_dyf")
    glue_ctx.write_dynamic_frame.from_options(
        frame=agg_dyf,
        connection_type="s3",
        connection_options={"path": OUTPUT_PATH, "partitionKeys": ["country"]},
        format="parquet",
        transformation_ctx="write_output",
    )
else:
    agg.write.mode("overwrite").partitionBy("country").parquet(OUTPUT_PATH)

print(f"Output written to: {OUTPUT_PATH}")

# ---------------------------------------------------------------------------
# Commit the job (required for Glue job bookmarks to advance)
# ---------------------------------------------------------------------------
if job:
    job.commit()

spark.stop()
print("Glue job complete.")
