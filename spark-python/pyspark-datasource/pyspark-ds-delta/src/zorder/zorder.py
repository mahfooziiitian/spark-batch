"""

Z-Ordering is a technique used to co-locate related data together in the same set of Parquet files.
It can significantly improve query performance when the data is frequently accessed based on certain columns.
To use Z-Ordering, you can specify the columns you want to optimize the table on when writing data.

"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    builder = (
        SparkSession.builder.appName("z_order")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    data_file = os.environ["DATA_HOME"] + "\\FileData\\Csv\\departuredelays.csv"
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .csv(data_file)

    spark.sql("create database if not exists z_order")

    print(spark.sparkContext.getConf().getAll())

    print(df.rdd.getNumPartitions())

    df.write.format("delta") \
        .mode("Overwrite")\
        .option("zOrderBy", "origin") \
        .saveAsTable("z_order.z_order")
