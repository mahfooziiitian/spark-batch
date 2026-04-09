"""Classic word count example using the text datasource."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("text_word_count")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp = os.path.join(tempfile.mkdtemp(), "document.txt")
    with open(tmp, "w") as f:
        f.write("spark is fast and spark is scalable\n")
        f.write("pyspark makes spark accessible from python\n")
        f.write("spark sql and spark dataframe are powerful\n")
        f.write("big data processing with spark is easy\n")

    df = spark.read.text(tmp)

    # split lines into words, explode into individual rows, then count
    word_counts = (
        df.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(F.desc("count"))
    )

    print("=== Word counts ===")
    word_counts.show(truncate=False)

    # --- top N words ---
    print("=== Top 5 words ===")
    word_counts.limit(5).show()

    # --- word count using lower-case normalisation ---
    print("=== Case-insensitive word counts ===")
    (
        df.select(
            F.explode(F.split(F.lower(F.col("value")), r"\s+")).alias("word")
        )
        .filter(F.col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    spark.stop()
