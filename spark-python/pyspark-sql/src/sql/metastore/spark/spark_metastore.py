import os

from pyspark.sql import SparkSession

if __name__ == '__main__':

    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("spark_hive_metastore")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())