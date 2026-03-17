"""
PySpark — YARN Cluster Setup Example
======================================
Verifies PySpark configuration on a YARN cluster.
Falls back to local[*] when SPARK_MASTER is not set (for local testing).

Source the environment variables first:
    source cluster/setup-yarn-env.sh

Local test (no cluster needed):
    python cluster/yarn_example.py

Submit to YARN:
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --num-executors 4 \\
        --executor-cores 2 \\
        --executor-memory 4g \\
        --driver-memory 2g \\
        cluster/yarn_example.py

Environment variables:
    SPARK_MASTER   — override master URL    (default: local[*])
    YARN_QUEUE     — YARN queue name        (default: default)
    INPUT_PATH     — HDFS path to input     (default: in-memory sample)
    OUTPUT_PATH    — HDFS output path       (default: /tmp/yarn_setup_output)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

YARN_QUEUE  = os.environ.get("YARN_QUEUE",  "default")
INPUT_PATH  = os.environ.get("INPUT_PATH")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/yarn_setup_output")

# ── SparkSession ───────────────────────────────────────────────────────────────
# .master() is a no-op when --master yarn is passed via spark-submit.
# The env-var fallback lets the same script run locally without changes.
spark = (SparkSession.builder
         .appName("yarn-setup-example")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.yarn.queue",                             YARN_QUEUE)
         .config("spark.sql.shuffle.partitions",                 "200")
         .config("spark.sql.adaptive.enabled",                   "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled","true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark YARN Cluster — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Master          : {spark.sparkContext.master}")
print(f"  YARN queue      : {YARN_QUEUE}")
print(f"  PYSPARK_PYTHON  : {os.environ.get('PYSPARK_PYTHON', '<not set>')}")
print(f"  HADOOP_CONF_DIR : {os.environ.get('HADOOP_CONF_DIR', '<not set>')}")
print()

# ── Ingest ────────────────────────────────────────────────────────────────────
# In production, INPUT_PATH is an HDFS path:
#   hdfs://namenode:8020/data/sensors/year=2024/
if INPUT_PATH:
    raw = spark.read.parquet(INPUT_PATH)
else:
    # Simulated IoT sensor readings
    sensor_rows = [
        ("sensor_A", "2024-03-01 00:00", "temperature", 22.5),
        ("sensor_B", "2024-03-01 00:00", "temperature", 19.8),
        ("sensor_A", "2024-03-01 01:00", "temperature", 21.9),
        ("sensor_B", "2024-03-01 01:00", "temperature", 20.1),
        ("sensor_A", "2024-03-01 02:00", "temperature", 23.4),
        ("sensor_B", "2024-03-01 02:00", "temperature", 18.7),
        ("sensor_A", "2024-03-01 03:00", "temperature", 24.0),
        ("sensor_B", "2024-03-01 03:00", "temperature", 17.5),
        ("sensor_A", "2024-03-01 04:00", "temperature", 22.1),
        ("sensor_B", "2024-03-01 04:00", "temperature", 19.3),
        ("sensor_A", "2024-03-01 05:00", "humidity",    65.0),
        ("sensor_B", "2024-03-01 05:00", "humidity",    70.2),
        ("sensor_A", "2024-03-01 06:00", "humidity",    63.5),
        ("sensor_B", "2024-03-01 06:00", "humidity",    68.9),
    ]
    raw = spark.createDataFrame(
        sensor_rows, ["sensor_id", "ts", "metric", "value"]
    )

raw = raw.withColumn("ts", F.to_timestamp("ts"))
print(f"Input rows: {raw.count()}")

# ── Hourly aggregation ────────────────────────────────────────────────────────
hourly = (raw
          .groupBy(
              "sensor_id",
              "metric",
              F.date_trunc("hour", "ts").alias("hour"),
          )
          .agg(
              F.round(F.avg("value"), 2).alias("avg_val"),
              F.round(F.min("value"), 2).alias("min_val"),
              F.round(F.max("value"), 2).alias("max_val"),
          )
          .orderBy("sensor_id", "metric", "hour"))

print("=== Hourly Sensor Aggregation ===")
hourly.show()

# ── Rolling 3-hour average (window function) ──────────────────────────────────
w = (Window
     .partitionBy("sensor_id", "metric")
     .orderBy(F.col("hour").cast("long"))
     .rowsBetween(-2, 0))

with_rolling = hourly.withColumn(
    "rolling_3h_avg",
    F.round(F.avg("avg_val").over(w), 2)
)

print("=== Rolling 3-Hour Average per Sensor ===")
with_rolling.show()

# ── Write ─────────────────────────────────────────────────────────────────────
# On YARN, OUTPUT_PATH is typically an HDFS path:
#   hdfs://namenode:8020/output/sensor_kpis/
(with_rolling
 .write
 .mode("overwrite")
 .partitionBy("sensor_id")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")
spark.stop()
print("YARN cluster setup verification complete.")
