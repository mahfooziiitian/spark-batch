import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("config-dynamic")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Read a config value
    print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

    # Change a mutable config at runtime
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

    # AQE can also be toggled at runtime
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    print("adaptive.enabled   =", spark.conf.get("spark.sql.adaptive.enabled"))

    # Attempting to change an immutable config raises an AnalysisException
    try:
        spark.conf.set("spark.master", "yarn")
    except Exception as e:
        print(f"Cannot change spark.master at runtime: {e}")

    # Unset a config to revert to default
    spark.conf.unset("spark.sql.shuffle.partitions")
    print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

    spark.stop()
