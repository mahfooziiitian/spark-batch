"""
PySpark — AWS Glue Setup Example
===================================
Runs as an AWS Glue ETL job. Falls back to plain PySpark when Glue
libraries are not available (local development without aws-glue-sessions).

Local test:
    pip install aws-glue-sessions
    python aws/glue_example.py \\
        --JOB_NAME test \\
        --input_path /tmp/input \\
        --output_path /tmp/output

    # Or without Glue libs (plain PySpark fallback):
    python aws/glue_example.py

Create a Glue job (AWS CLI):
    aws glue create-job \\
        --name pyspark-setup-glue-job \\
        --role AWSGlueServiceRole \\
        --command '{"Name":"glueetl","ScriptLocation":"s3://my-bucket/scripts/glue_example.py","PythonVersion":"3"}' \\
        --default-arguments '{"--input_path":"s3://my-bucket/hr/","--output_path":"s3://my-bucket/output/"}' \\
        --glue-version "4.0" \\
        --number-of-workers 5 \\
        --worker-type G.1X

Start a run:
    aws glue start-job-run \\
        --job-name pyspark-setup-glue-job \\
        --arguments '{"--input_path":"s3://my-bucket/hr/","--output_path":"s3://my-bucket/output/"}'
"""

import sys

# Glue-specific imports — available in the Glue runtime or via aws-glue-sessions.
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    print("INFO: AWS Glue libraries not found — running in plain PySpark mode.")
    print("      Install with: pip install aws-glue-sessions")
    print()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Initialise Glue or plain PySpark ─────────────────────────────────────────
if GLUE_AVAILABLE:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "input_path", "output_path"],
    )
    sc       = SparkContext()
    glue_ctx = GlueContext(sc)
    spark    = glue_ctx.spark_session
    job      = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)
    INPUT_PATH  = args["input_path"]
    OUTPUT_PATH = args["output_path"]
else:
    spark = (SparkSession.builder
             .appName("glue-setup-example-local")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    INPUT_PATH  = None
    OUTPUT_PATH = "/tmp/glue_setup_output"

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark AWS Glue — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Glue available  : {GLUE_AVAILABLE}")
print(f"  Output path     : {OUTPUT_PATH}")
print()

# ── Ingest ────────────────────────────────────────────────────────────────────
if GLUE_AVAILABLE and INPUT_PATH:
    # Read from S3 via Glue Data Catalog or direct path
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [INPUT_PATH], "recurse": True},
        format="parquet",
    )
    raw = dynamic_frame.toDF()
else:
    # Simulated HR / payroll data
    rows = [
        ("EMP001", "Engineering", "Senior", 115000, "2020-03-01"),
        ("EMP002", "Marketing",   "Junior",  58000, "2022-07-15"),
        ("EMP003", "Engineering", "Lead",   145000, "2018-01-10"),
        ("EMP004", "HR",          "Senior",  78000, "2019-06-01"),
        ("EMP005", "Engineering", "Junior",  72000, "2023-01-20"),
        ("EMP006", "Marketing",   "Senior",  92000, "2020-11-01"),
        ("EMP007", "Finance",     "Lead",   138000, "2017-05-15"),
        ("EMP008", "HR",          "Junior",  52000, "2023-08-01"),
        ("EMP009", "Finance",     "Senior", 105000, "2021-03-12"),
        ("EMP010", "Engineering", "Senior", 120000, "2021-09-01"),
    ]
    raw = spark.createDataFrame(
        rows, ["emp_id", "department", "level", "salary", "hire_date"]
    )

raw = raw.withColumn("hire_date", F.to_date("hire_date"))
print(f"Input rows: {raw.count()}")

# ── Department salary statistics ──────────────────────────────────────────────
dept_stats = (raw
              .groupBy("department")
              .agg(
                  F.round(F.avg("salary"), 0).alias("avg_salary"),
                  F.min("salary").alias("min_salary"),
                  F.max("salary").alias("max_salary"),
                  F.count("emp_id").alias("headcount"),
              )
              .orderBy(F.desc("avg_salary")))

print("=== Department Salary Stats ===")
dept_stats.show()

# ── Salary bands by level ─────────────────────────────────────────────────────
level_bands = (raw
               .groupBy("level")
               .agg(
                   F.round(F.avg("salary"), 0).alias("avg_salary"),
                   F.min("salary").alias("min_salary"),
                   F.max("salary").alias("max_salary"),
               )
               .orderBy(F.desc("avg_salary")))

print("=== Salary Bands by Level ===")
level_bands.show()

# ── Salary percentile rank within department ──────────────────────────────────
w = Window.partitionBy("department").orderBy("salary")
ranked = raw.withColumn("dept_salary_pct", F.round(F.percent_rank().over(w), 2))

print("=== Employee Salary Rank within Department ===")
ranked.select("emp_id", "department", "level", "salary", "dept_salary_pct") \
      .orderBy("department", F.desc("salary")) \
      .show()

# ── Write output ──────────────────────────────────────────────────────────────
if GLUE_AVAILABLE:
    output_dyf = DynamicFrame.fromDF(dept_stats, glue_ctx, "dept_stats_output")
    glue_ctx.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="s3",
        connection_options={"path": OUTPUT_PATH},
        format="parquet",
    )
    job.commit()   # marks job success for Glue job bookmarks
else:
    dept_stats.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("Glue setup verification complete.")
