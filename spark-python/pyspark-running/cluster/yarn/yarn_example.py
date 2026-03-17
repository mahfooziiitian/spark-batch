"""
PySpark — YARN Cluster Example
================================
This script is designed to be submitted to a YARN cluster but also runs
locally for development/testing (master is read from spark-submit or env var).

Submit to YARN:
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --num-executors 4 \\
        --executor-memory 4g \\
        yarn_example.py

Local test (no cluster):
    spark-submit --master local[*] yarn_example.py
    # or:
    python yarn_example.py   (sets master=local[*] automatically)

Environment variables:
    INPUT_PATH   — HDFS or S3 path to input data  (default: in-memory sample)
    OUTPUT_PATH  — HDFS or S3 path for output     (default: /tmp/yarn_output)
    YARN_QUEUE   — YARN queue name                 (default: default)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# SparkSession
# When submitted via spark-submit --master yarn the master is already set.
# The fallback lets you run the script directly with `python yarn_example.py`.
# ---------------------------------------------------------------------------
queue = os.environ.get("YARN_QUEUE", "default")

spark = (SparkSession.builder
         .appName("yarn-etl-example")
         # master() is a no-op when --master is passed via spark-submit
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.yarn.queue", queue)
         .config("spark.sql.shuffle.partitions", "200")      # good default for YARN
         .config("spark.sql.adaptive.enabled", "true")       # AQE — auto-tunes shuffles
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Master:      {spark.sparkContext.master}")
print(f"App name:    {spark.sparkContext.appName}")
print(f"YARN queue:  {queue}")

# ---------------------------------------------------------------------------
# Ingest
# In production, INPUT_PATH would be an HDFS path like:
#   hdfs://namenode:8020/data/sales/year=2024/
# ---------------------------------------------------------------------------
INPUT_PATH = os.environ.get("INPUT_PATH")

if INPUT_PATH:
    raw = spark.read.parquet(INPUT_PATH)
else:
    rows = [
        ("2024-01", "North",  "Electronics", 1500.0),
        ("2024-01", "South",  "Clothing",     800.0),
        ("2024-02", "North",  "Electronics", 1750.0),
        ("2024-02", "East",   "Electronics",  950.0),
        ("2024-02", "West",   "Clothing",     600.0),
        ("2024-03", "North",  "Clothing",     400.0),
        ("2024-03", "South",  "Electronics", 2100.0),
        ("2024-03", "East",   "Clothing",     350.0),
    ]
    raw = spark.createDataFrame(rows, ["month", "region", "category", "revenue"])

print(f"Input rows: {raw.count()}")

# ---------------------------------------------------------------------------
# Transform & aggregate
# ---------------------------------------------------------------------------
monthly_kpi = (raw
               .groupBy("month", "region", "category")
               .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
               .orderBy("month", "region"))

print("\n=== Monthly KPIs ===")
monthly_kpi.show()

# Running total per region using a window function
from pyspark.sql.window import Window

window = Window.partitionBy("region").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)

with_running_total = monthly_kpi.withColumn(
    "running_revenue",
    F.round(F.sum("total_revenue").over(window), 2)
)

print("=== Running Revenue Total by Region ===")
with_running_total.show()

# ---------------------------------------------------------------------------
# Write output
# On YARN the output would typically go to HDFS:
#   hdfs://namenode:8020/output/sales_kpi/
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/yarn_output")

(with_running_total
 .write
 .mode("overwrite")
 .partitionBy("region")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
