"""Recursively read text files from nested directory structures."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_recursive")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- build a nested directory tree ---
    base = tempfile.mkdtemp()
    structure = {
        "region=north/2025-01.txt": "north jan event\nnorth jan sale",
        "region=north/2025-02.txt": "north feb event",
        "region=south/2025-01.txt": "south jan event\nsouth jan return",
        "region=south/2025-02.txt": "south feb event",
    }
    for rel_path, content in structure.items():
        full = os.path.join(base, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(content)

    # without recursiveFileLookup Spark only reads top-level files
    # (here there are none, so it would fail or return empty)

    # with recursiveFileLookup=true it walks subdirectories
    print("=== Recursive file lookup ===")
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .text(base)
        .withColumn("source", F.input_file_name())
    )
    df.show(truncate=False)
    print(f"Total rows: {df.count()}")

    spark.stop()
