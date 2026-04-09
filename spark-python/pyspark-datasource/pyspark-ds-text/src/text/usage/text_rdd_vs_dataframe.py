"""Compare RDD textFile vs DataFrame text reader — same source, different APIs."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("text_rdd_vs_dataframe")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp = os.path.join(tempfile.mkdtemp(), "data.txt")
    with open(tmp, "w") as f:
        f.write("apple 3\nbanana 5\ncherry 2\ndate 8\nelderberry 1\n")

    # --- RDD approach: spark.sparkContext.textFile ---
    print("=== RDD textFile ===")
    rdd = spark.sparkContext.textFile(tmp)
    print(f"Type: {type(rdd)}")
    print(f"Lines: {rdd.collect()}")

    # RDD word split and count
    word_counts_rdd = (
        rdd.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    print(f"RDD word counts: {word_counts_rdd}")

    # --- DataFrame approach: spark.read.text ---
    print("\n=== DataFrame text ===")
    df = spark.read.text(tmp)
    print(f"Type: {type(df)}")
    df.show(truncate=False)

    # --- wholeTextFiles RDD (key=path, value=content) ---
    print("=== RDD wholeTextFiles ===")
    rdd_whole = spark.sparkContext.wholeTextFiles(tmp)
    for path, content in rdd_whole.collect():
        print(f"Path: {path}")
        print(f"Content: {repr(content[:80])}")

    # --- convert RDD to DataFrame ---
    print("\n=== RDD → DataFrame ===")
    df_from_rdd = rdd.map(lambda line: (line,)).toDF(["value"])
    df_from_rdd.show(truncate=False)

    # --- convert DataFrame to RDD ---
    print("=== DataFrame → RDD ===")
    rdd_from_df = df.rdd.map(lambda row: row["value"])
    print(rdd_from_df.collect())

    spark.stop()
