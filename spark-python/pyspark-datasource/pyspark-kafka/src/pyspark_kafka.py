import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]").appName("spark_kafka")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1").getOrCreate())

    bootstrap_server = "localhost:19091,localhost:29091,localhost:39091"
    topic = "events"

    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", bootstrap_server)
          .option("subscribe", topic)
          .load())

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show(truncate=False)
