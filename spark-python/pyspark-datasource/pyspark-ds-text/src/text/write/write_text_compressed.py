"""Write text files with compression codecs — gzip, bzip2, deflate."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("write_text_compressed")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [(f"log entry {i}",) for i in range(100)]
    df = spark.createDataFrame(data, ["value"])
    base = tempfile.mkdtemp()

    # --- gzip ---
    gzip_path = os.path.join(base, "gzip_out")
    df.coalesce(1).write.mode("overwrite").option("compression", "gzip").text(gzip_path)
    print("=== Gzip output files ===")
    for f_name in os.listdir(gzip_path):
        full = os.path.join(gzip_path, f_name)
        print(f"  {f_name}  ({os.path.getsize(full)} bytes)")

    # --- bzip2 ---
    bz2_path = os.path.join(base, "bzip2_out")
    df.coalesce(1).write.mode("overwrite").option("compression", "bzip2").text(bz2_path)
    print("\n=== Bzip2 output files ===")
    for f_name in os.listdir(bz2_path):
        full = os.path.join(bz2_path, f_name)
        print(f"  {f_name}  ({os.path.getsize(full)} bytes)")

    # --- no compression (for comparison) ---
    plain_path = os.path.join(base, "plain_out")
    df.coalesce(1).write.mode("overwrite").option("compression", "none").text(plain_path)
    print("\n=== Uncompressed output files ===")
    for f_name in os.listdir(plain_path):
        full = os.path.join(plain_path, f_name)
        print(f"  {f_name}  ({os.path.getsize(full)} bytes)")

    # verify compressed files are still readable
    print("\n=== Read back gzip ===")
    spark.read.text(gzip_path).show(5, truncate=False)

    spark.stop()
