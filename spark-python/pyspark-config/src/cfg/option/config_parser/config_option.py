import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("config-option")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("All Spark configuration options:")
    print("-" * 50)
    for key, value in sorted(spark.sparkContext.getConf().getAll()):
        print(f"  {key} = {value}")

    spark.stop()
