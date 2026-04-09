"""Write DataFrames as text files — the DataFrame must have a single string column."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("write_text_basic")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    out_dir = os.path.join(tempfile.mkdtemp(), "output_text")

    # text writer requires exactly one string column — concatenate fields
    df_text = df.select(F.concat_ws(",", F.col("name"), F.col("age")).alias("value"))

    # --- basic write ---
    path1 = os.path.join(out_dir, "basic")
    df_text.write.mode("overwrite").text(path1)
    print("=== Written text files ===")
    spark.read.text(path1).show(truncate=False)

    # --- write with specific number of partitions ---
    path2 = os.path.join(out_dir, "single_partition")
    df_text.coalesce(1).write.mode("overwrite").text(path2)
    print("=== Single partition output ===")
    spark.read.text(path2).show(truncate=False)

    # --- overwrite vs append ---
    path3 = os.path.join(out_dir, "append_demo")
    df_text.write.mode("overwrite").text(path3)
    df_extra = spark.createDataFrame(
        [("Diana,28",), ("Eve,32",)], ["value"]
    )
    df_extra.write.mode("append").text(path3)
    print("=== After append ===")
    spark.read.text(path3).show(truncate=False)

    spark.stop()
