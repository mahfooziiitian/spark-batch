"""
Word Count — PySpark Standalone Cluster (Docker Compose)
=========================================================
Connect to the cluster started by docker-compose.yml and run a word-frequency
analysis. Uses the SPARK_MASTER env var injected by the notebook service.

Run from inside the JupyterLab terminal:
    python word_count.py

Or submit directly:
    docker compose --profile submit run --rm \
      -e JOB_FILE=word_count.py spark-submit
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

# ── SparkSession ───────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("word-count")
         .master(SPARK_MASTER)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Connected to master: {spark.sparkContext.master}")

# ── Sample text corpus ─────────────────────────────────────────────────────────
lines = [
    "Apache Spark is a unified analytics engine for large scale data processing",
    "Spark provides high level APIs in Java Scala Python and R",
    "Spark also supports a rich set of higher level tools including Spark SQL",
    "Spark SQL is used for structured data processing with SQL queries",
    "PySpark is the Python API for Apache Spark",
    "Docker Compose makes it easy to run a Spark cluster locally",
    "The Spark master coordinates the Spark workers in the cluster",
    "Each Spark worker runs executor processes for the Spark applications",
]

# ── Word frequency ─────────────────────────────────────────────────────────────
text = spark.createDataFrame([(l,) for l in lines], ["line"])

word_counts = (text
               .select(F.explode(F.split(F.lower(F.col("line")), r"\s+")).alias("word"))
               .filter(F.length("word") > 3)          # skip short stop-words
               .groupBy("word")
               .agg(F.count("*").alias("count"))
               .orderBy(F.desc("count"))
               .limit(20))

print("\n=== Top 20 Words ===")
word_counts.show()

spark.stop()
print("Done.")
