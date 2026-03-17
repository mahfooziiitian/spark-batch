"""
PySpark — Docker Container Setup Example
=========================================
Designed to run inside the pyspark-dev Docker image defined in this folder.
Writes output to /tmp inside the container.

Build and run:
    docker build -t pyspark-dev:3.5 docker/

    # Run this script inside the container
    docker run --rm \\
      -v "$(pwd)":/workspace \\
      pyspark-dev:3.5 python3 /workspace/docker/docker_example.py

    # Or open an interactive shell first
    docker run --rm -it -v "$(pwd)":/workspace -p 4040:4040 pyspark-dev:3.5 bash
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── 1. SparkSession ────────────────────────────────────────────────────────────
# SPARK_LOCAL_IP is set to 127.0.0.1 in the Dockerfile to avoid DNS resolution
# issues inside the container network.
spark = (SparkSession.builder
         .appName("docker-setup-example")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark Docker Container — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Master          : {spark.sparkContext.master}")
print(f"  SPARK_LOCAL_IP  : {os.environ.get('SPARK_LOCAL_IP', '<not set>')}")
print(f"  PYSPARK_PYTHON  : {os.environ.get('PYSPARK_PYTHON', '<not set>')}")
print()

# ── 2. Simulated web server access logs ───────────────────────────────────────
log_data = [
    ("2024-03-01 08:01:00", "/home",     "GET",  200, 120),
    ("2024-03-01 08:02:15", "/products", "GET",  200, 340),
    ("2024-03-01 08:03:00", "/cart",     "POST", 201,  80),
    ("2024-03-01 08:04:10", "/checkout", "POST", 200, 210),
    ("2024-03-01 08:05:00", "/home",     "GET",  200, 115),
    ("2024-03-01 08:06:30", "/login",    "POST", 401,  50),
    ("2024-03-01 08:07:00", "/products", "GET",  200, 300),
    ("2024-03-01 08:08:45", "/missing",  "GET",  404,  30),
    ("2024-03-01 08:09:00", "/checkout", "POST", 500,   5),
    ("2024-03-01 08:10:15", "/home",     "GET",  200, 130),
    ("2024-03-01 08:11:00", "/cart",     "GET",  200,  90),
    ("2024-03-01 08:12:30", "/login",    "POST", 200,  75),
]

logs = spark.createDataFrame(
    log_data, ["ts", "path", "method", "status", "resp_ms"]
)
logs = (logs
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn(
            "status_class",
            F.when(F.col("status") < 300, "2xx")
             .when(F.col("status") < 400, "3xx")
             .when(F.col("status") < 500, "4xx")
             .otherwise("5xx")
        ))

print("=== Access Logs ===")
logs.show()

# ── 3. Endpoint statistics ────────────────────────────────────────────────────
endpoint_stats = (logs
                  .groupBy("path")
                  .agg(
                      F.count("*").alias("requests"),
                      F.round(F.avg("resp_ms"), 1).alias("avg_resp_ms"),
                      F.max("resp_ms").alias("max_resp_ms"),
                      F.sum(
                          F.when(F.col("status") >= 400, 1).otherwise(0)
                      ).alias("errors"),
                  )
                  .withColumn(
                      "error_rate_pct",
                      F.round(F.col("errors") / F.col("requests") * 100, 1)
                  )
                  .orderBy(F.desc("requests")))

print("=== Endpoint Stats ===")
endpoint_stats.show()

# ── 4. Response time percentile ranks ─────────────────────────────────────────
w = Window.orderBy("resp_ms")
with_rank = logs.withColumn("resp_pct_rank", F.percent_rank().over(w))
slow_requests = with_rank.filter(F.col("resp_pct_rank") >= 0.75).orderBy(
    F.desc("resp_ms")
)

print("=== Slowest Requests (top 25%) ===")
slow_requests.select("ts", "path", "method", "status", "resp_ms", "resp_pct_rank").show()

# ── 5. Write to container /tmp ────────────────────────────────────────────────
output_path = "/tmp/pyspark_docker_output"
endpoint_stats.write.mode("overwrite").parquet(output_path)
print(f"Output written to: {output_path}")

spark.stop()
print("Docker container setup verification complete.")
