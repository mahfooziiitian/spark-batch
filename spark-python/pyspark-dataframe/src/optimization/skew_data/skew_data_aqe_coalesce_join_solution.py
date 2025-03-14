import os
import sys
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable


def prepare_trips_data(spark: SparkSession, data_file: str) -> DataFrame:
    pu_loc_to_change = [
        236, 132, 161, 186, 142, 141, 48, 239, 170, 162, 230, 163, 79, 234, 263, 140, 238, 107, 68, 138, 229, 249,
        237, 164, 90, 43, 100, 246, 231, 262, 113, 233, 143, 137, 114, 264, 148, 151
    ]

    res_df = spark.read \
        .parquet(data_file) \
        .withColumn("PULocationID",
                    f.when(f.col("PULocationID").isin(pu_loc_to_change), f.lit(237)).otherwise(
                        f.col("PULocationID")))
    return res_df


def join_on_skewed_data(spark: SparkSession, data_file: str, trip_data_file: str):
    trips_data = prepare_trips_data(spark=spark, data_file=trip_data_file)
    location_details_data = (spark.read
                             .option("header", True)
                             .option("inferSchema", True)
                             .csv(data_file)
                             .withColumn("LocationID", f.expr("cast(LocationID as long)")))

    trips_with_pickup_location_details = trips_data \
        .join(location_details_data, f.col("PULocationID") == f.col("LocationID"), "inner")

    trips_with_pickup_location_details.show(truncate=False)

    # trips_with_pickup_location_details \
    #     .groupBy("Zone") \
    #     .agg(f.avg("trip_distance").alias("avg_trip_distance")) \
    #     .sort(f.col("avg_trip_distance").desc()) \
    #     .show(truncate=False, n=1000)
    #
    # trips_with_pickup_location_details \
    #     .groupBy("Borough") \
    #     .agg(f.avg("trip_distance").alias("avg_trip_distance")) \
    #     .sort(f.col("avg_trip_distance").desc()) \
    #     .show(truncate=False, n=1000)


def create_spark_session_with_aqe_disabled() -> SparkSession:
    conf = SparkConf() \
        .set("spark.driver.memory", "4G") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "201") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256K")

    spark_session = SparkSession \
        .builder \
        .master("local[*]") \
        .config(conf=conf) \
        .appName("Skew Data Optimization") \
        .getOrCreate()

    return spark_session


def main():
    data_file = f"{os.environ['DATA_HOME']}/FileData/Csv/trips/taxi_zone_lookup.csv"
    trip_data_file = f"{os.environ['DATA_HOME']}/FileData/Parquet/trips/*.parquet"
    start_time = time.time()
    spark = create_spark_session_with_aqe_disabled()

    print(f"Hadoop version = {spark.sparkContext.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

    join_on_skewed_data(spark=spark, data_file=data_file, trip_data_file=trip_data_file)

    print(f"Elapsed_time: {(time.time() - start_time)} seconds")
    time.sleep(10000)


if __name__ == '__main__':
    main()
