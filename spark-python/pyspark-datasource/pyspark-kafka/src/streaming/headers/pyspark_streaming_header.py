from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def main():
    app_id = "movies_streaming"
    scala_version = '2.12'
    spark_version = '3.5.1'
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
    ]
    brokers = ["localhost:29091", "localhost:19091", "localhost:39091"]
    topic = "movies"
    starting_offset = "earliest"

    # read from the database
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("kafka_producer")
             .config("spark.jars.packages", ",".join(packages))
             .getOrCreate())

    # Read data from kafka stream

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ",".join(brokers))
          .option("startingOffsets", starting_offset)
          .option("subscribe", topic)
          .load())

    writer = df.writeStream.foreachBatch(batch_processing)

    writer.start().awaitTermination()


def batch_processing(batch_df: DataFrame, batch_id: int):
    print(f"batch id: {batch_id}")
    batch_df.withColumn("json_value", col("value").cast("string")).drop("value").show(truncate=False)


if __name__ == '__main__':
    main()
