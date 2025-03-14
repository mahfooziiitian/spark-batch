"""
Spark SQL uses a Hive metastore to manage the metastore of persistent relational entities (e.g.,
databases, tables, columns, partitions) in a relational database (for fast access).
A Hive metastore (aka metastore_db) is a relational database to manage the metastore of the
persistent relational entities, e.g., databases, tables, columns, partitions.
By default, Spark SQL uses the embedded deployment mode of a Hive metastore with an Apache Derby database.
"""
import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("spark_hive_metastore")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())

    spark.sql("show tables in global_temp").show()