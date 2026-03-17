"""
PySpark — Kubernetes Setup Example
====================================
Designed to run inside the Docker container on a Kubernetes cluster.
Also runs locally for development without any changes.

Build and push the Docker image:
    docker build -t my-registry/pyspark-job:3.5 docker/
    docker push my-registry/pyspark-job:3.5

Submit to Kubernetes (cluster mode):
    spark-submit \\
        --master k8s://https://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') \\
        --deploy-mode cluster \\
        --conf spark.kubernetes.container.image=my-registry/pyspark-job:3.5 \\
        --conf spark.kubernetes.namespace=default \\
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
        --conf spark.executor.instances=3 \\
        local:///opt/spark/work-dir/k8s_example.py

Local test:
    python cluster/k8s_example.py
    # or:
    spark-submit --master local[*] cluster/k8s_example.py

Environment variables:
    SPARK_MASTER   — override master URL    (default: local[*])
    INPUT_PATH     — path / URI to input    (default: in-memory sample)
    OUTPUT_PATH    — path / URI for output  (default: /tmp/k8s_setup_output)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH  = os.environ.get("INPUT_PATH")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/k8s_setup_output")

# ── SparkSession ───────────────────────────────────────────────────────────────
# On K8s --master k8s://... is injected by spark-submit at launch time.
# The SPARK_MASTER env var allows overriding without code changes.
spark = (SparkSession.builder
         .appName("k8s-setup-example")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions",                 "12")
         .config("spark.sql.adaptive.enabled",                   "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled","true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark Kubernetes — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Master          : {spark.sparkContext.master}")
print(f"  PYSPARK_PYTHON  : {os.environ.get('PYSPARK_PYTHON', '<not set>')}")
print()

# ── Ingest ────────────────────────────────────────────────────────────────────
# In production, INPUT_PATH points to object storage:
#   s3a://my-bucket/data/events/
#   gs://my-bucket/data/events/
#   abfss://container@account.dfs.core.windows.net/events/
if INPUT_PATH:
    raw = spark.read.json(INPUT_PATH)
else:
    # Simulated e-commerce order events
    event_rows = [
        ("2024-03-01", "user_101", "view",     "prod_A", 1,   0.00),
        ("2024-03-01", "user_102", "view",     "prod_B", 1,   0.00),
        ("2024-03-01", "user_101", "add_cart", "prod_A", 2, 199.98),
        ("2024-03-02", "user_103", "view",     "prod_C", 1,   0.00),
        ("2024-03-02", "user_101", "purchase", "prod_A", 2, 199.98),
        ("2024-03-02", "user_102", "add_cart", "prod_B", 1,  49.99),
        ("2024-03-03", "user_104", "view",     "prod_A", 1,   0.00),
        ("2024-03-03", "user_102", "purchase", "prod_B", 1,  49.99),
        ("2024-03-03", "user_103", "add_cart", "prod_C", 3, 119.97),
        ("2024-03-03", "user_105", "purchase", "prod_C", 3, 119.97),
    ]
    raw = spark.createDataFrame(
        event_rows,
        ["event_date", "user_id", "event_type", "product_id", "qty", "amount"],
    )

raw = raw.withColumn("event_date", F.to_date("event_date"))
print(f"Event count: {raw.count()}")

# ── Conversion funnel ─────────────────────────────────────────────────────────
funnel = (raw
          .groupBy("event_type")
          .agg(
              F.countDistinct("user_id").alias("unique_users"),
              F.count("*").alias("total_events"),
              F.round(F.sum("amount"), 2).alias("total_revenue"),
          )
          .orderBy(F.desc("total_events")))

print("=== Conversion Funnel ===")
funnel.show()

# ── Daily revenue with running total ─────────────────────────────────────────
daily_rev = (raw
             .filter(F.col("event_type") == "purchase")
             .groupBy("event_date")
             .agg(F.round(F.sum("amount"), 2).alias("daily_revenue"))
             .orderBy("event_date"))

w = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)
daily_rev = daily_rev.withColumn(
    "cumulative_revenue",
    F.round(F.sum("daily_revenue").over(w), 2)
)

print("=== Daily Revenue with Running Total ===")
daily_rev.show()

# ── Product popularity ────────────────────────────────────────────────────────
product_stats = (raw
                 .groupBy("product_id")
                 .agg(
                     F.countDistinct("user_id").alias("unique_buyers"),
                     F.round(F.sum("amount"), 2).alias("total_revenue"),
                 )
                 .withColumn("rank", F.rank().over(
                     Window.orderBy(F.desc("total_revenue"))
                 ))
                 .orderBy("rank"))

print("=== Product Popularity ===")
product_stats.show()

# ── Write ─────────────────────────────────────────────────────────────────────
funnel.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Output written to: {OUTPUT_PATH}")

spark.stop()
print("Kubernetes setup verification complete.")
