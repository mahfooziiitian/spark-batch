"""
One way to better distribute the data is to add a column on both sides of the join.

I’ll do this in the following way:

    In the zone lookup data, which is smaller, repeat each row n times with a different value of a new column, which can have values 0 to n-1. This new column is called location_id_alt.
    In the rides data, which is bigger, and is the one with the data skew, add values 0 to n-1 based on another column. In this example, I choose the pickup timestamp (tpep_pickup_datetime) column, convert it to day of the year using the dayofyear Spark SQL function, and take a mod n. Now, both sides have an additional column, with values from 1 to n.
    Adding this new column in the join will distribute all partitions, including the one with more data, into more partitions. Additionally, the computation of the new column doesn’t involve anything that would require a shuffle to compute, there is no rank or row number operation on a window.

"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f


def join_on_skewed_data_with_sub_split(spark: SparkSession, data_file: str, lookup_data_file: str):
    sub_split_partitions = 20
    trips_data = (prepare_trips_data(spark=spark, data_file=data_file)
                  .withColumn("dom_mod", f.dayofyear(f.col("tpep_pickup_datetime")))
                  .withColumn("dom_mod", f.expr(f"dom_mod %{sub_split_partitions}")))

    location_details_data = (spark.read
                             .option("header", True)
                             .option("inferSchema", True)
                             .csv(lookup_data_file)
                             .withColumn("location_id_alt",
                                         f.array([f.lit(num) for num in range(0, sub_split_partitions)]))
                             .withColumn("location_id_alt", f.explode(f.col("location_id_alt"))))

    trips_with_pickup_location_details = (trips_data.join(
        location_details_data,
        (f.col("PULocationID") == f.col("LocationID")) & (f.col("location_id_alt") == f.col("dom_mod")),
        "inner"
    ))

    trips_with_pickup_location_details.show(truncate=False)

    # (trips_with_pickup_location_details
    #  .groupBy("Zone")
    #  .agg(f.avg("trip_distance").alias("avg_trip_distance"))
    #  .sort(f.col("avg_trip_distance").desc())
    #  .show(truncate=False, n=1000))
    #
    # (trips_with_pickup_location_details
    #  .groupBy("Borough")
    #  .agg(f.avg("trip_distance").alias("avg_trip_distance"))
    #  .sort(f.col("avg_trip_distance").desc())
    #  .show(truncate=False, n=1000))


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


def create_spark_session_with_aqe_disabled() -> SparkSession:
    conf = SparkConf() \
        .set("spark.driver.memory", "4G") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "201") \
        .set("spark.sql.adaptive.enabled", "false")

    # .set("spark.sql.execution.arrow.pyspark.enabled","true")

    spark_session = SparkSession \
        .builder \
        .master("local[*]") \
        .config(conf=conf) \
        .appName("Skew Data Optimization") \
        .getOrCreate()

    return spark_session


def main():
    lookup_data_file = f"{os.environ['DATA_HOME']}/FileData/Csv/trips/taxi_zone_lookup.csv"
    trip_data_file = f"{os.environ['DATA_HOME']}/FileData/Parquet/trips/*.parquet"
    start_time = time.time()
    spark = create_spark_session_with_aqe_disabled()

    print(f"Hadoop version = {spark.sparkContext.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

    join_on_skewed_data_with_sub_split(spark=spark, data_file=trip_data_file, lookup_data_file=lookup_data_file)

    print(f"Elapsed_time: {(time.time() - start_time)} seconds")
    time.sleep(10000)


if __name__ == '__main__':
    main()
