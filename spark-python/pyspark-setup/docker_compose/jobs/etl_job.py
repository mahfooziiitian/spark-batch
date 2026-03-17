"""
ETL Job — PySpark Standalone Cluster
======================================
Sample ETL job for the docker-compose Spark cluster.

Submit via the spark-submit service:
    docker compose --profile submit run --rm spark-submit

Or override the job file:
    JOB_FILE=jobs/etl_job.py docker compose --profile submit run --rm spark-submit

Submit manually from inside a container:
    spark-submit \\
        --master spark://spark-master:7077 \\
        --conf spark.eventLog.enabled=true \\
        --conf spark.eventLog.dir=/opt/spark/events \\
        /opt/spark/work-dir/etl_job.py

Environment variables:
    INPUT_PATH   — path to input data     (default: in-memory sample)
    OUTPUT_PATH  — path for Parquet output (default: /tmp/etl_output)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH  = os.environ.get("INPUT_PATH")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/etl_output")

# ── SparkSession ────────────────────────────────────────────────────────────────
# master is injected via spark-defaults.conf (spark.master = spark://spark-master:7077)
# or passed via --master on the spark-submit command line.
spark = (SparkSession.builder
         .appName("docker-compose-etl-job")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Docker Compose ETL Job")
print("=" * 60)
print(f"  Spark version : {spark.version}")
print(f"  Master        : {spark.sparkContext.master}")
print(f"  App UI        : {spark.sparkContext.uiWebUrl}")
print()

# ── Ingest ──────────────────────────────────────────────────────────────────────
if INPUT_PATH:
    raw = spark.read.parquet(INPUT_PATH)
else:
    # Simulated retail sales data
    rows = [
        ("2024-01", "North", "Electronics", 1500.00, 10),
        ("2024-01", "South", "Clothing",     800.00,  5),
        ("2024-01", "East",  "Books",         250.00, 20),
        ("2024-02", "North", "Electronics", 1750.00, 12),
        ("2024-02", "South", "Electronics",  950.00,  8),
        ("2024-02", "West",  "Clothing",      600.00,  4),
        ("2024-03", "North", "Clothing",      400.00,  3),
        ("2024-03", "East",  "Electronics", 2100.00, 15),
        ("2024-03", "South", "Books",         180.00, 14),
        ("2024-03", "West",  "Electronics", 1300.00,  9),
    ]
    raw = spark.createDataFrame(
        rows, ["month", "region", "category", "revenue", "units"]
    )

print(f"Input rows: {raw.count()}")

# ── Transform: revenue per unit ────────────────────────────────────────────────
enriched = raw.withColumn(
    "revenue_per_unit",
    F.round(F.col("revenue") / F.col("units"), 2)
)

# ── Aggregate: monthly KPIs by region ─────────────────────────────────────────
monthly_kpi = (enriched
               .groupBy("month", "region")
               .agg(
                   F.round(F.sum("revenue"), 2).alias("total_revenue"),
                   F.sum("units").alias("total_units"),
                   F.round(F.avg("revenue_per_unit"), 2).alias("avg_rev_per_unit"),
               )
               .orderBy("month", "region"))

print("\n=== Monthly KPIs by Region ===")
monthly_kpi.show()

# ── Window: running revenue total per region ───────────────────────────────────
w = (Window
     .partitionBy("region")
     .orderBy("month")
     .rowsBetween(Window.unboundedPreceding, 0))

with_running = monthly_kpi.withColumn(
    "running_revenue",
    F.round(F.sum("total_revenue").over(w), 2)
)

print("=== Running Revenue per Region ===")
with_running.show()

# ── Category revenue share ─────────────────────────────────────────────────────
cat_totals = (enriched
              .groupBy("category")
              .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
              .withColumn(
                  "revenue_share_pct",
                  F.round(
                      F.col("total_revenue")
                      / F.sum("total_revenue").over(Window.orderBy(F.lit(1))
                                                    .rowsBetween(Window.unboundedPreceding,
                                                                 Window.unboundedFollowing))
                      * 100, 1
                  )
              )
              .orderBy(F.desc("total_revenue")))

print("=== Category Revenue Share ===")
cat_totals.show()

# ── Write ───────────────────────────────────────────────────────────────────────
(with_running
 .write
 .mode("overwrite")
 .partitionBy("region")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("ETL job complete.")
