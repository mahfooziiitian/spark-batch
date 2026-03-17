"""
PySpark — AWS EMR Setup Example
=================================
Runs on Amazon EMR (EC2) or locally for development testing.

Upload script to S3:
    aws s3 cp aws/emr_example.py s3://my-bucket/scripts/

Submit as an EMR Step:
    aws emr add-steps \\
        --cluster-id j-XXXXXXXXXXXX \\
        --steps Type=Spark,Name="RetailETL",ActionOnFailure=CONTINUE,\\
    Args=[--deploy-mode,cluster,\\
          --conf,spark.sql.adaptive.enabled=true,\\
          s3://my-bucket/scripts/emr_example.py]

Or ssh to the primary node and submit directly:
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --conf spark.sql.adaptive.enabled=true \\
        s3://my-bucket/scripts/emr_example.py

Local test (no AWS needed):
    USE_LOCAL_DATA=true python aws/emr_example.py

Environment variables:
    INPUT_PATH     — S3 input path    (default: in-memory sample)
    OUTPUT_PATH    — S3 output path   (default: /tmp/emr_setup_output)
    USE_LOCAL_DATA — any value forces in-memory sample data
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH  = os.environ.get("INPUT_PATH")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/emr_setup_output")
USE_LOCAL   = bool(os.environ.get("USE_LOCAL_DATA"))

# ── SparkSession ───────────────────────────────────────────────────────────────
# On EMR the cluster-level SparkContext is already configured by bootstrap.
# .master() is omitted so EMR sets it to yarn automatically.
# The local fallback enables identical code to run on a developer laptop.
builder = (SparkSession.builder
           .appName("emr-setup-example")
           .config("spark.sql.adaptive.enabled",                   "true")
           .config("spark.sql.adaptive.coalescePartitions.enabled","true")
           # Faster S3 commit protocol — no-op outside EMR
           .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
           # Disable speculative execution — can corrupt S3 output
           .config("spark.speculation", "false"))

if not INPUT_PATH or USE_LOCAL:
    builder = (builder
               .master("local[*]")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false"))

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark AWS EMR — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Master          : {spark.sparkContext.master}")
print()

# ── Ingest ────────────────────────────────────────────────────────────────────
# On EMR, INPUT_PATH is an S3 path read via EMRFS:
#   s3://my-bucket/data/transactions/year=2024/
if INPUT_PATH and not USE_LOCAL:
    raw = spark.read.parquet(INPUT_PATH)
else:
    rows = [
        ("TXN001", "2024-01-05", "C100", "Electronics", 1,  899.99),
        ("TXN002", "2024-01-06", "C101", "Clothing",     3,   49.99),
        ("TXN003", "2024-01-07", "C100", "Clothing",     1,   89.99),
        ("TXN004", "2024-02-01", "C102", "Electronics",  2,  299.99),
        ("TXN005", "2024-02-03", "C103", "Books",        5,   12.99),
        ("TXN006", "2024-02-10", "C101", "Electronics",  1,  599.99),
        ("TXN007", "2024-03-01", "C100", "Books",        3,   19.99),
        ("TXN008", "2024-03-05", "C104", "Clothing",     2,   74.99),
        ("TXN009", "2024-03-12", "C102", "Books",        7,   12.99),
        ("TXN010", "2024-03-20", "C103", "Electronics",  1, 1299.99),
    ]
    raw = spark.createDataFrame(
        rows,
        ["txn_id", "txn_date", "customer_id", "category", "qty", "unit_price"],
    )

raw = (raw
       .withColumn("txn_date", F.to_date("txn_date"))
       .withColumn("revenue",  F.round(F.col("qty") * F.col("unit_price"), 2)))

print(f"Input rows: {raw.count()}")

# ── Monthly revenue by category ───────────────────────────────────────────────
monthly = (raw
           .withColumn("year_month", F.date_format("txn_date", "yyyy-MM"))
           .groupBy("year_month", "category")
           .agg(
               F.round(F.sum("revenue"), 2).alias("total_revenue"),
               F.sum("qty").alias("total_qty"),
               F.count("txn_id").alias("transactions"),
           )
           .orderBy("year_month", "category"))

print("=== Monthly Revenue by Category ===")
monthly.show()

# ── Customer lifetime value ───────────────────────────────────────────────────
clv = (raw
       .groupBy("customer_id")
       .agg(
           F.round(F.sum("revenue"), 2).alias("lifetime_value"),
           F.count("txn_id").alias("total_orders"),
           F.round(F.avg("revenue"), 2).alias("avg_order_value"),
           F.min("txn_date").alias("first_purchase"),
           F.max("txn_date").alias("last_purchase"),
       )
       .orderBy(F.desc("lifetime_value")))

print("=== Customer Lifetime Value ===")
clv.show()

# ── Category revenue rank ─────────────────────────────────────────────────────
w = Window.orderBy(F.desc("total_revenue"))
cat_ranking = (raw
               .groupBy("category")
               .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
               .withColumn("rank", F.rank().over(w))
               .orderBy("rank"))

print("=== Category Ranking ===")
cat_ranking.show()

# ── Write ─────────────────────────────────────────────────────────────────────
# On EMR OUTPUT_PATH is an S3 path: s3://my-bucket/output/retail/
(monthly
 .write
 .mode("overwrite")
 .partitionBy("year_month")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("EMR setup verification complete.")
