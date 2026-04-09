"""Read compressed text files — Spark auto-detects gzip and bzip2 by extension."""
import gzip
import bz2
import os
import tempfile

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_compressed")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp_dir = tempfile.mkdtemp()
    lines = [
        "2025-01-01 server started",
        "2025-01-02 request processed",
        "2025-01-03 server stopped",
    ]
    text = "\n".join(lines)

    # --- gzip ---
    gz_path = os.path.join(tmp_dir, "logs.txt.gz")
    with gzip.open(gz_path, "wt") as f:
        f.write(text)

    print("=== Gzip compressed ===")
    spark.read.text(gz_path).show(truncate=False)

    # --- bzip2 ---
    bz2_path = os.path.join(tmp_dir, "logs.txt.bz2")
    with bz2.open(bz2_path, "wt") as f:
        f.write(text)

    print("=== Bzip2 compressed ===")
    spark.read.text(bz2_path).show(truncate=False)

    # --- read a directory with mixed compressed and uncompressed ---
    mixed_dir = os.path.join(tmp_dir, "mixed")
    os.makedirs(mixed_dir)
    with open(os.path.join(mixed_dir, "plain.txt"), "w") as f:
        f.write("plain text line")
    with gzip.open(os.path.join(mixed_dir, "compressed.txt.gz"), "wt") as f:
        f.write("gzipped text line")

    print("=== Mixed directory (plain + gzip) ===")
    spark.read.text(mixed_dir).show(truncate=False)

    spark.stop()
