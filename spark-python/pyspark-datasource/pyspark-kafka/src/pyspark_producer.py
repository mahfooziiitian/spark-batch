import os

from pyspark.sql import SparkSession

scala_version = '2.12'
spark_version = '3.5.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
]
brokers = ["localhost:29091", "localhost:19091", "localhost:39091"]
topic = "movies"

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("kafka_producer") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    # Read all lines into a single value dataframe  with column 'value'
    data_file = os.environ["DATA_HOME"].replace("\\", "/") + "/FileData/Json/Movies/movies.json"
    df = spark.read.text(data_file).sample(0.05, 1)

    # Write
    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(brokers)) \
        .option("topic", topic) \
        .save()
