import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, get_json_object

from streaming.listener.my_listener import MyListener


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

    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

    # Read data from kafka stream

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ",".join(brokers))
          .option("startingOffsets", starting_offset)
          .option("subscribe", topic)
          .load())

    writer = df.writeStream.foreachBatch(batch_processing)
    # adding listener
    my_listener = MyListener()
    spark.streams.addListener(my_listener)

    streaming_query = writer.trigger(processingTime='10 seconds').start()
    streaming_query.awaitTermination()


def batch_processing(batch_df: DataFrame, batch_id: int):
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    print(f"batch id: {batch_id}")
    file_batch_dir = os.environ['DATA_HOME'].replace("\\", "/") + "/Processing/Streaming/kafka/files/file_stream_batch"

    transformed_df = (batch_df.withColumn("json_value", col("value").cast("string"))
                      .drop("value"))

    value_transformed_df = (transformed_df.select(
        col("topic"),
        col("key").cast("string").alias("key"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        get_json_object(col("json_value"), "$.actor_name").alias("actor_name"),
        get_json_object(col("json_value"), "$.movie_title").alias("movie_title"),
        get_json_object(col("json_value"), "$.produced_year").alias(
            "produced_year")
    ))

    value_transformed_df.show(truncate=False)

    # Writing to file
    (value_transformed_df.select("actor_name", "movie_title", "produced_year")
     .write.mode("append").format("csv").save(file_batch_dir))
    # writing to JDBC

    batch_df.unpersist()


if __name__ == '__main__':
    main()
