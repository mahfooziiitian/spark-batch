import os

from pyspark.sql import SparkSession

if __name__ == '__main__':

    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("hive_catalog")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.sql.warehouse.dir", warehouse_location)
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())
    # create database
    spark.sql("create database if not exists spark_hive")

    # Create a Hive table
    spark.sql("CREATE TABLE IF NOT EXISTS spark_hive.my_table (id INT, name STRING) STORED AS PARQUET")

    # Example: Insert data into the Hive table
    spark.sql("INSERT INTO spark_hive.my_table VALUES (1, 'John'), (2, 'Jane')")

    # Example: Query the Hive table
    result = spark.sql("SELECT * FROM spark_hive.my_table")
    result.show()

