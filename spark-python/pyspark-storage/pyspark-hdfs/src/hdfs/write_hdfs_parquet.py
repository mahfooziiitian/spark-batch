import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    namenode = os.environ.get("HDFS_NAMENODE", "localhost:8020")

    spark = (SparkSession.builder
             .appName("hdfs-write-parquet")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.hadoop.fs.defaultFS", f"hdfs://{namenode}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get(
        "INPUT_PATH", "hdfs:///user/data/input/sample.csv")
    output_path = os.environ.get(
        "OUTPUT_PATH", "hdfs:///user/data/output/dept_salary")

    df = (spark.read
          .option("inferSchema", True)
          .option("header", True)
          .csv(input_path))
    df.show(truncate=False)

    dept_salary = (df
                   .groupBy("department")
                   .agg(F.avg("salary").alias("avg_salary")))
    dept_salary.show(truncate=False)

    dept_salary.write.mode("overwrite").parquet(output_path)
    print(f"Parquet written to {output_path}")

    spark.stop()
