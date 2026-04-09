"""Read a text file into a DataFrame — one row per line, single 'value' column."""
import os
import tempfile

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_basic")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- create sample data ---
    tmp = os.path.join(tempfile.mkdtemp(), "sample.txt")
    with open(tmp, "w") as f:
        f.write("Hello World\n")
        f.write("Apache Spark is fast\n")
        f.write("PySpark text datasource\n")

    # --- read ---
    df = spark.read.text(tmp)

    df.printSchema()
    df.show(truncate=False)
    print(f"Row count: {df.count()}")

    spark.stop()
