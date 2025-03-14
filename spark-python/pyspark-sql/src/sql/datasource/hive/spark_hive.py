import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession
             .builder
             .appName("pyspark-sql-hive")
             .enableHiveSupport()
             .config("spark.sql.warehouse.dir", warehouse_location)
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .getOrCreate())

    spark.sql("create database if not exists spark_hive")

    spark.sql('show databases').show()
