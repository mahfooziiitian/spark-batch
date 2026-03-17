"""
PySpark on Amazon EMR (EC2)
============================
Designed to run as an EMR Step or via spark-submit from the primary node.
Uses EMRFS (s3://) for S3 access — no extra credentials or jars needed on EMR.

For local testing without a cluster, set USE_LOCAL_DATA=true.

Upload to S3 and submit as an EMR step:
    aws s3 cp emr_example.py s3://my-bucket/scripts/

    aws emr add-steps --cluster-id j-XXXX \\
      --steps Type=Spark,Name="ETL",ActionOnFailure=CONTINUE,\\
    Args=[s3://my-bucket/scripts/emr_example.py]

spark-submit from the primary node:
    spark-submit \\
      --master yarn --deploy-mode cluster \\
      --conf spark.sql.adaptive.enabled=true \\
      s3://my-bucket/scripts/emr_example.py

Environment variables:
    INPUT_PATH      — s3://bucket/prefix/  (default: in-memory sample)
    OUTPUT_PATH     — s3://bucket/output/  (default: /tmp/emr_output)
    USE_LOCAL_DATA  — set to any value to skip S3 and use in-memory data
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# SparkSession — on EMR master is injected by YARN; keep getOrCreate() only
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("emr-sales-etl")
         # EMR: these are already configured by the cluster; safe to re-declare
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         # Faster S3 commits — avoid the rename dance
         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
         # Never speculate on S3 — can cause double-writes
         .config("spark.speculation", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"Master        : {spark.sparkContext.master}")

# ---------------------------------------------------------------------------
# Ingest
# On EMR, s3:// uses EMRFS — IAM role on the EC2 instances handles auth.
# Locally with hadoop-aws, use s3a:// and set AWS credentials via env vars.
# ---------------------------------------------------------------------------
INPUT_PATH     = os.environ.get("INPUT_PATH")
USE_LOCAL_DATA = os.environ.get("USE_LOCAL_DATA")

if INPUT_PATH and not USE_LOCAL_DATA:
    print(f"Reading from S3: {INPUT_PATH}")
    # Adjust format to match your actual data (parquet / csv / json)
    raw = spark.read.parquet(INPUT_PATH)
else:
    print("Using in-memory sample data")
    rows = [
        ("2024-01", "us-east-1", "Electronics",  1200.0),
        ("2024-01", "us-west-2", "Electronics",   950.0),
        ("2024-01", "eu-west-1", "Clothing",       600.0),
        ("2024-02", "us-east-1", "Electronics",  1550.0),
        ("2024-02", "us-west-2", "Clothing",       400.0),
        ("2024-02", "ap-southeast-1", "Electronics", 780.0),
        ("2024-03", "us-east-1", "Clothing",       350.0),
        ("2024-03", "eu-west-1", "Electronics",   1100.0),
        ("2024-03", "ap-southeast-1", "Clothing",  210.0),
        ("2024-03", "us-west-2", "Electronics",    880.0),
    ]
    raw = spark.createDataFrame(
        rows, ["month", "region", "category", "revenue"]
    )

print(f"Input rows: {raw.count()}")

# ---------------------------------------------------------------------------
# Aggregate KPIs by month + region + category
# ---------------------------------------------------------------------------
kpi = (raw
       .groupBy("month", "region", "category")
       .agg(
           F.round(F.sum("revenue"), 2).alias("total_revenue"),
           F.count("*").alias("num_records"),
       )
       .orderBy("month", "region"))

print("\n=== KPIs ===")
kpi.show()

# ---------------------------------------------------------------------------
# Month-over-month revenue change per region (window function)
# ---------------------------------------------------------------------------
window = Window.partitionBy("region", "category").orderBy("month")

mom = (kpi
       .withColumn("prev_revenue", F.lag("total_revenue").over(window))
       .withColumn(
           "mom_change_pct",
           F.round(
               (F.col("total_revenue") - F.col("prev_revenue"))
               / F.col("prev_revenue") * 100,
               1
           )
       ))

print("=== Month-over-Month Change ===")
mom.show()

# ---------------------------------------------------------------------------
# Write to S3 (or local /tmp)
# Partition by region so downstream consumers can read one region at a time.
# On EMR, s3:// writes go directly to S3 via EMRFS (atomic and consistent).
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/emr_output")

(mom
 .write
 .mode("overwrite")
 .partitionBy("region")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("Job complete.")
