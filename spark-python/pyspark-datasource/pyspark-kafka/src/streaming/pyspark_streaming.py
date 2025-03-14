import os

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, rank, concat_ws, struct, to_json, collect_list, lit

from util.config_reader import ConfigReader

JDBC_CONFIG_FILE = os.environ['DATA_HOME'].replace("\\", "/") + "/Database/Config/MySQL/db.conf"


def main():
    app_id = "movies_streaming"
    scala_version = '2.12'
    spark_version = '3.5.1'
    mysql_version = '8.0.32'
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        f'com.mysql:mysql-connector-j:{mysql_version}'
    ]
    brokers = ["localhost:29091", "localhost:19091", "localhost:39091"]
    topic = "movies"
    starting_offset = ""

    # read from the database
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("kafka_producer")
             .config("spark.jars.packages", ",".join(packages))
             # .config("startingOffsets", starting_offset)
             .getOrCreate())

    # Read data from kafka stream

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", ",".join(brokers))
          .option("subscribe", topic)
          .load())

    writer = df.writeStream.foreachBatch(func)

    writer.start().awaitTermination()


def get_s3_data_folder():
    data_home = os.environ.get('DATA_HOME', "").replace("\\", "/") + "/Processing/Streaming/kafka/stocks"
    return data_home


def get_latest_offset_from_batch(batch_df: DataFrame):
    window_spec = Window.partitionBy("topic", "partition").orderBy(col("offset").desc())
    latest_offset_df = batch_df.withColumn("rnk", rank().over(window_spec)).where("rnk=1")
    latest_offset_df = latest_offset_df.groupby(col("topic")).agg(concat_ws(",", collect_list(
        to_json(struct(col("partition"),
                       col("offset"))))).alias("offset"))
    return latest_offset_df


def write_offset_into_database(offset_df: DataFrame, table_name: str):
    config_reader = ConfigReader(JDBC_CONFIG_FILE)
    properties = {
        "url": config_reader.get_url(),
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver()
    }

    (offset_df.write.mode("append")
     .jdbc(url=config_reader.get_url(), table=table_name, mode="append", properties=properties)
     )


def reading_offset_from_database(spark: SparkSession, table_name: str):
    config_reader = ConfigReader(JDBC_CONFIG_FILE)
    properties = {
        "url": config_reader.get_url(),
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver()
    }

    (spark.read.jdbc(url=config_reader.get_url(), table=table_name, properties=properties).collect()
     )


# schema reading

def func(batch_df: DataFrame, batch_id):
    if not batch_df.isEmpty():
        batch_df.persist()
        # 1. Writing to file
        (batch_df.selectExpr("cast(value as string) as value").write.mode("append")
         .format("json").save(get_s3_data_folder()))
        latest_offset = get_latest_offset_from_batch(batch_df).withColumn("batch_id", lit(batch_id))
        latest_offset.show(truncate=False)
        # 2. saving the offset to jdbc
        # topic,partition, offset
        # {"topic1":{"0":23,"1":-2}}
        write_offset_into_database(latest_offset, "kafka_offset")
        # 3. Saving error to error topic
        batch_df.unpersist()


def read_offset_from_db(topic):
    print()


if __name__ == '__main__':
    main()
