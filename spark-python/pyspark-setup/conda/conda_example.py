"""
PySpark — Conda Environment Setup Example
==========================================
Verifies PySpark is correctly configured inside a Conda environment.
Uses findspark to locate Spark when SPARK_HOME is not set explicitly.

Activate the environment first:
    conda activate pyspark-env

Run:
    python conda/conda_example.py
"""

import os
import sys

# findspark is optional — it helps locate Spark in non-standard installations.
try:
    import findspark
    findspark.init()
    print("findspark: initialised OK")
except ImportError:
    print("findspark not installed (optional) — skipping")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── 1. SparkSession ────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("conda-setup-example")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ── 2. Print conda / environment info ─────────────────────────────────────────
conda_env = os.environ.get("CONDA_DEFAULT_ENV", "<not in a conda env>")

print()
print("=" * 60)
print("PySpark Conda Environment — Setup Verification")
print("=" * 60)
print(f"  Conda env       : {conda_env}")
print(f"  Python binary   : {sys.executable}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Spark version   : {spark.version}")
print(f"  Master          : {spark.sparkContext.master}")
print()
print("  Installed packages:")
for pkg in ("pyspark", "pyarrow", "pandas", "numpy", "findspark"):
    try:
        mod = __import__(pkg)
        ver = getattr(mod, "__version__", "?")
        print(f"    {pkg:<12}: {ver}")
    except ImportError:
        print(f"    {pkg:<12}: NOT INSTALLED")
print()

# ── 3. Student score analysis ─────────────────────────────────────────────────
data = [
    ("Alice", "Math",    92), ("Alice", "Science", 88), ("Alice", "English", 95),
    ("Bob",   "Math",    75), ("Bob",   "Science", 82), ("Bob",   "English", 68),
    ("Carol", "Math",    98), ("Carol", "Science", 91), ("Carol", "English", 85),
    ("Dave",  "Math",    60), ("Dave",  "Science", 72), ("Dave",  "English", 78),
    ("Eve",   "Math",    88), ("Eve",   "Science", 94), ("Eve",   "English", 90),
]

df = spark.createDataFrame(data, ["student", "subject", "score"])

summary = (df
           .groupBy("student")
           .agg(
               F.round(F.avg("score"), 1).alias("avg_score"),
               F.min("score").alias("min_score"),
               F.max("score").alias("max_score"),
           )
           .orderBy(F.desc("avg_score")))

print("=== Student Score Summary ===")
summary.show()

# ── 4. Pivot — scores as a wide table ─────────────────────────────────────────
pivot = (df
         .groupBy("student")
         .pivot("subject", ["English", "Math", "Science"])
         .agg(F.first("score"))
         .orderBy("student"))

print("=== Score Pivot Table ===")
pivot.show()

spark.stop()
print("Conda environment setup verification complete.")
