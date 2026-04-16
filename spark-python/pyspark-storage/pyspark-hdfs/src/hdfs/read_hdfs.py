import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "/usr/lib/jvm/java-8-openjdk-amd64")
    namenode = os.environ.get("HDFS_NAMENODE", "localhost:8020")

    spark = (SparkSession.builder
             .appName("hdfs-read-write")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.hadoop.fs.defaultFS", f"hdfs://{namenode}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "hdfs:///user/data/input.csv")
    output_path = os.environ.get("OUTPUT_PATH", "hdfs:///user/data/output")

    df = (spark.read
          .option("inferSchema", True)
          .option("header", True)
          .csv(input_path))
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()
