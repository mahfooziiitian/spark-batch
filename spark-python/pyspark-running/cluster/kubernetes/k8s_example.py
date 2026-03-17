"""
PySpark — Kubernetes Cluster Example
======================================
Designed to run inside a Docker container on a Kubernetes cluster.
Also runnable locally for development.

Build & push:
    docker build -t my-registry/pyspark-job:latest .
    docker push my-registry/pyspark-job:latest

Submit to K8s (cluster mode):
    spark-submit \\
      --master k8s://https://<api-server>:6443 \\
      --deploy-mode cluster \\
      --conf spark.kubernetes.container.image=my-registry/pyspark-job:latest \\
      --conf spark.kubernetes.namespace=default \\
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
      --conf spark.executor.instances=3 \\
      local:///opt/spark/work-dir/k8s_example.py

Local test:
    spark-submit --master local[*] k8s_example.py
    # or:
    python k8s_example.py

Environment variables:
    INPUT_PATH   — path/URI to input data  (default: in-memory sample)
    OUTPUT_PATH  — path/URI for output     (default: /tmp/k8s_output)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# SparkSession
# On K8s the master is passed via --master k8s://... at submit time.
# The fallback lets you run the script locally without a cluster.
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("k8s-etl-example")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "12")
         .config("spark.sql.adaptive.enabled", "true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Master:   {spark.sparkContext.master}")
print(f"App name: {spark.sparkContext.appName}")

# ---------------------------------------------------------------------------
# Ingest
# On K8s, data typically lives in object storage (S3, GCS, ADLS).
# Example S3 path: s3a://my-bucket/data/events/
# ---------------------------------------------------------------------------
INPUT_PATH = os.environ.get("INPUT_PATH")

if INPUT_PATH:
    events = spark.read.json(INPUT_PATH)
else:
    event_rows = [
        ("2024-03-01", "user_001", "page_view",  "home",      1),
        ("2024-03-01", "user_002", "purchase",   "checkout",  1),
        ("2024-03-01", "user_001", "click",      "product_a", 1),
        ("2024-03-02", "user_003", "page_view",  "home",      1),
        ("2024-03-02", "user_002", "page_view",  "product_b", 1),
        ("2024-03-02", "user_003", "purchase",   "checkout",  1),
        ("2024-03-03", "user_001", "purchase",   "checkout",  1),
        ("2024-03-03", "user_004", "page_view",  "home",      1),
        ("2024-03-03", "user_004", "click",      "product_a", 1),
        ("2024-03-03", "user_003", "click",      "product_b", 1),
    ]
    events = spark.createDataFrame(
        event_rows, ["event_date", "user_id", "event_type", "page", "cnt"]
    )

events = events.withColumn("event_date", F.to_date("event_date"))

print(f"Event count: {events.count()}")

# ---------------------------------------------------------------------------
# Daily active users (DAU)
# ---------------------------------------------------------------------------
dau = (events
       .groupBy("event_date")
       .agg(F.countDistinct("user_id").alias("dau"))
       .orderBy("event_date"))

print("\n=== Daily Active Users ===")
dau.show()

# ---------------------------------------------------------------------------
# Event funnel: page_view → click → purchase
# ---------------------------------------------------------------------------
funnel = (events
          .groupBy("event_type")
          .agg(F.count("cnt").alias("event_count"))
          .orderBy(F.desc("event_count")))

print("=== Event Funnel ===")
funnel.show()

# ---------------------------------------------------------------------------
# User-level engagement score: purchases weighted 3×, clicks 2×, views 1×
# ---------------------------------------------------------------------------
scored = events.withColumn(
    "score",
    F.when(F.col("event_type") == "purchase", 3)
     .when(F.col("event_type") == "click",    2)
     .otherwise(1)
)

user_scores = (scored
               .groupBy("user_id")
               .agg(F.sum("score").alias("engagement_score"))
               .orderBy(F.desc("engagement_score")))

print("=== User Engagement Scores ===")
user_scores.show()

# ---------------------------------------------------------------------------
# Rank users by engagement using a window function
# ---------------------------------------------------------------------------
window_spec = Window.orderBy(F.desc("engagement_score"))
ranked = user_scores.withColumn("rank", F.rank().over(window_spec))
print("=== Ranked Users ===")
ranked.show()

# ---------------------------------------------------------------------------
# Write output
# On K8s, write to object storage:
#   s3a://my-bucket/output/engagement/
#   gs://my-bucket/output/engagement/
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/k8s_output")

ranked.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Output written to: {OUTPUT_PATH}")

spark.stop()
print("Job complete.")
