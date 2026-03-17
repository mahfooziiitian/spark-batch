"""
Cluster Example — PySpark Standalone Cluster (Docker Compose)
================================================================
Demonstrates connecting to the docker-compose Spark cluster,
reading data, running aggregations and window functions, and
writing Parquet output.

Run from inside the JupyterLab terminal:
    python cluster_example.py

Or from outside (requires docker network access):
    SPARK_MASTER=spark://localhost:7077 python cluster_example.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
OUTPUT_PATH  = os.environ.get("OUTPUT_PATH",  "/tmp/cluster_output")

# ── SparkSession ───────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("cluster-example")
         .master(SPARK_MASTER)
         .config("spark.sql.shuffle.partitions", "20")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark Standalone Cluster — Docker Compose Example")
print("=" * 60)
print(f"  Spark version : {spark.version}")
print(f"  Master        : {spark.sparkContext.master}")
print(f"  Default parallelism: {spark.sparkContext.defaultParallelism}")
print()

# ── Sample data — web analytics ────────────────────────────────────────────────
events = [
    ("2024-03-01", "user_01", "page_view",  "home",       0.00),
    ("2024-03-01", "user_02", "page_view",  "products",   0.00),
    ("2024-03-01", "user_01", "add_cart",   "widget_a",  19.99),
    ("2024-03-01", "user_03", "page_view",  "home",       0.00),
    ("2024-03-02", "user_01", "purchase",   "widget_a",  19.99),
    ("2024-03-02", "user_02", "add_cart",   "widget_b",  49.99),
    ("2024-03-02", "user_04", "page_view",  "products",   0.00),
    ("2024-03-03", "user_02", "purchase",   "widget_b",  49.99),
    ("2024-03-03", "user_03", "add_cart",   "widget_c",  29.99),
    ("2024-03-03", "user_05", "purchase",   "widget_a",  19.99),
    ("2024-03-04", "user_03", "purchase",   "widget_c",  29.99),
    ("2024-03-04", "user_04", "add_cart",   "widget_b",  49.99),
    ("2024-03-04", "user_05", "page_view",  "home",       0.00),
    ("2024-03-05", "user_04", "purchase",   "widget_b",  49.99),
    ("2024-03-05", "user_01", "page_view",  "products",   0.00),
]

df = spark.createDataFrame(
    events, ["event_date", "user_id", "event_type", "item", "amount"]
)
df = df.withColumn("event_date", F.to_date("event_date"))

# ── Conversion funnel ──────────────────────────────────────────────────────────
funnel = (df
          .groupBy("event_type")
          .agg(
              F.countDistinct("user_id").alias("unique_users"),
              F.round(F.sum("amount"), 2).alias("total_revenue"),
          )
          .orderBy(F.desc("unique_users")))

print("=== Conversion Funnel ===")
funnel.show()

# ── Daily revenue with running total ──────────────────────────────────────────
daily = (df
         .filter(F.col("event_type") == "purchase")
         .groupBy("event_date")
         .agg(F.round(F.sum("amount"), 2).alias("daily_revenue"))
         .orderBy("event_date"))

w_running = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)
daily = daily.withColumn(
    "cumulative_revenue",
    F.round(F.sum("daily_revenue").over(w_running), 2)
)

print("=== Daily Revenue + Running Total ===")
daily.show()

# ── User engagement scores ─────────────────────────────────────────────────────
scored = df.withColumn(
    "score",
    F.when(F.col("event_type") == "purchase", 3)
     .when(F.col("event_type") == "add_cart", 2)
     .otherwise(1)
)

user_scores = (scored
               .groupBy("user_id")
               .agg(F.sum("score").alias("engagement_score"))
               .withColumn("rank", F.rank().over(
                   Window.orderBy(F.desc("engagement_score"))
               ))
               .orderBy("rank"))

print("=== User Engagement Ranking ===")
user_scores.show()

# ── Write Parquet ──────────────────────────────────────────────────────────────
daily.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Output written to: {OUTPUT_PATH}")

spark.stop()
print("Cluster example complete.")
