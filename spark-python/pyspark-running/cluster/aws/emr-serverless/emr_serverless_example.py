"""
PySpark on EMR Serverless
==========================
Designed to be submitted to an EMR Serverless application via the AWS CLI or SDK.
No cluster to manage — workers are auto-provisioned per job run.

One-time setup:
    aws emr-serverless create-application \\
        --name pyspark-app --type SPARK --release-label emr-7.1.0

Upload this script:
    aws s3 cp emr_serverless_example.py s3://my-bucket/scripts/

Submit a job run:
    aws emr-serverless start-job-run \\
        --application-id <app-id> \\
        --execution-role-arn arn:aws:iam::<account>:role/EMRServerlessRole \\
        --job-driver '{
            "sparkSubmit": {
                "entryPoint": "s3://my-bucket/scripts/emr_serverless_example.py",
                "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
            }
        }'

Local test (no AWS needed):
    USE_LOCAL_DATA=true python emr_serverless_example.py

Environment variables:
    INPUT_PATH      — s3://bucket/prefix/ for input
    OUTPUT_PATH     — s3://bucket/prefix/ for output
    USE_LOCAL_DATA  — any value → skip S3, use in-memory sample
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ---------------------------------------------------------------------------
# SparkSession
# EMR Serverless injects master + YARN config automatically.
# The fallback master lets you test locally.
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("emr-serverless-etl")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         # Serverless: prefer dynamic allocation (on by default in EMR Serverless)
         .config("spark.dynamicAllocation.enabled", "true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"Master        : {spark.sparkContext.master}")

# ---------------------------------------------------------------------------
# Ingest
# On EMR Serverless, s3:// is accessed via the execution role's IAM permissions.
# ---------------------------------------------------------------------------
INPUT_PATH     = os.environ.get("INPUT_PATH")
USE_LOCAL_DATA = os.environ.get("USE_LOCAL_DATA")

schema = StructType([
    StructField("event_date",  StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("product_id",  StringType(),  False),
    StructField("event_type",  StringType(),  False),
    StructField("quantity",    IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
])

if INPUT_PATH and not USE_LOCAL_DATA:
    print(f"Reading from S3: {INPUT_PATH}")
    events = spark.read.schema(schema).json(INPUT_PATH)
else:
    print("Using in-memory sample data")
    sample_rows = [
        ("2024-03-01", "u001", "P001", "purchase", 2, 19.98),
        ("2024-03-01", "u002", "P002", "purchase", 1, 49.99),
        ("2024-03-02", "u001", "P003", "purchase", 3,  8.97),
        ("2024-03-02", "u003", "P001", "purchase", 1,  9.99),
        ("2024-03-03", "u004", "P002", "purchase", 2, 99.98),
        ("2024-03-03", "u002", "P003", "purchase", 1,  2.99),
        ("2024-03-04", "u001", "P001", "purchase", 5, 49.95),
        ("2024-03-04", "u005", "P002", "purchase", 1, 49.99),
        ("2024-03-05", "u003", "P003", "purchase", 4, 11.96),
        ("2024-03-05", "u004", "P001", "purchase", 2, 19.98),
    ]
    events = spark.createDataFrame(sample_rows, schema)

events = events.withColumn("event_date", F.to_date("event_date"))
print(f"Input rows: {events.count()}")

# ---------------------------------------------------------------------------
# Daily revenue
# ---------------------------------------------------------------------------
daily = (events
         .groupBy("event_date")
         .agg(
             F.round(F.sum("amount"), 2).alias("daily_revenue"),
             F.countDistinct("user_id").alias("buyers"),
             F.sum("quantity").alias("units_sold"),
         )
         .orderBy("event_date"))

print("\n=== Daily Revenue ===")
daily.show()

# ---------------------------------------------------------------------------
# Top products by revenue
# ---------------------------------------------------------------------------
top_products = (events
                .groupBy("product_id")
                .agg(
                    F.round(F.sum("amount"), 2).alias("total_revenue"),
                    F.sum("quantity").alias("total_units"),
                )
                .orderBy(F.desc("total_revenue")))

print("=== Top Products ===")
top_products.show()

# ---------------------------------------------------------------------------
# Repeat-buyer rate: users with > 1 purchase date
# ---------------------------------------------------------------------------
buyer_dates = (events
               .groupBy("user_id")
               .agg(F.countDistinct("event_date").alias("active_days")))

repeat_rate = (buyer_dates
               .agg(
                   F.count("user_id").alias("total_buyers"),
                   F.sum(F.when(F.col("active_days") > 1, 1).otherwise(0))
                    .alias("repeat_buyers"),
               )
               .withColumn(
                   "repeat_rate_pct",
                   F.round(F.col("repeat_buyers") / F.col("total_buyers") * 100, 1)
               ))

print("=== Repeat Buyer Rate ===")
repeat_rate.show()

# ---------------------------------------------------------------------------
# Write output to S3 (or /tmp locally)
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/emr_serverless_output")

daily.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/daily_revenue")
top_products.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/top_products")
repeat_rate.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/repeat_buyers")

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("Job complete.")
