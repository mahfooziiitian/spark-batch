"""Read text from multiple files, directories, and glob patterns."""

import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_multifile")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = tempfile.mkdtemp()
    files = {
        "logs_2024.txt": "2024-01-01 INFO startup\n2024-06-15 WARN low disk",
        "logs_2025.txt": "2025-02-10 ERROR timeout\n2025-03-01 INFO recovered",
        "notes.txt": "Remember to upgrade Spark",
    }
    for name, content in files.items():
        with open(os.path.join(base, name), "w") as f:
            f.write(content)

    # 1 — read a single file
    print("=== Single file ===")
    spark.read.text(os.path.join(base, "logs_2024.txt")).show(truncate=False)

    # 2 — read multiple explicit paths as a list
    print("=== Multiple explicit paths ===")
    spark.read.text(
        [os.path.join(base, "logs_2024.txt"), os.path.join(base, "logs_2025.txt")]
    ).show(truncate=False)

    # 3 — read an entire directory (all files)
    print("=== Entire directory ===")
    df_all = spark.read.text(base)
    df_all.show(truncate=False)
    print(f"Total rows: {df_all.count()}")

    # 4 — glob pattern to match only log files
    print("=== Glob pattern (logs_*.txt) ===")
    spark.read.text(os.path.join(base, "logs_*.txt")).show(truncate=False)

    # 5 — add input file name as a column
    print("=== With input_file_name ===")
    df_with_file = spark.read.text(base).withColumn("source_file", F.input_file_name())
    df_with_file.show(truncate=False)

    spark.stop()
