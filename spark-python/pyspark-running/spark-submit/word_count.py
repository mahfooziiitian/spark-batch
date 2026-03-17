"""
PySpark Script — Word Count
===========================
Classic MapReduce word-count implemented with the DataFrame API.

Submit:
    spark-submit --master local[*] word_count.py

Pass a custom file via an environment variable:
    INPUT_FILE=/path/to/file.txt spark-submit --master local[*] word_count.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# SparkSession — master is intentionally NOT hard-coded so spark-submit can
# inject --master from the command line.
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("word-count")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Input — use file from env var, CLI arg, or a small built-in sample
# ---------------------------------------------------------------------------
input_path = os.environ.get("INPUT_FILE") or (sys.argv[1] if len(sys.argv) > 1 else None)

if input_path:
    lines = spark.read.text(input_path)
else:
    # Built-in sample so the job always runs without extra files
    sample_text = [
        ("the quick brown fox jumps over the lazy dog",),
        ("to be or not to be that is the question",),
        ("all that glitters is not gold",),
        ("the fox and the dog are friends",),
    ]
    lines = spark.createDataFrame(sample_text, ["value"])

# ---------------------------------------------------------------------------
# Word count pipeline
# ---------------------------------------------------------------------------
word_counts = (lines
               .select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
               .filter(F.col("word") != "")
               .withColumn("word", F.lower(F.col("word")))
               .groupBy("word")
               .count()
               .orderBy(F.desc("count")))

print("\n=== Top 20 words ===")
word_counts.show(20)

# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------
output_path = os.environ.get("OUTPUT_PATH", "/tmp/word_count_output")
word_counts.write.mode("overwrite").csv(output_path, header=True)
print(f"Results written to: {output_path}")

spark.stop()
