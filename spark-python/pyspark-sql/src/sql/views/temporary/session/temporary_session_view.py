"""
    createOrReplaceTempView() is used when you wanted to store the table for a specific spark session.
    Once created, you can use it to run SQL queries.
    These temporary views are session-scoped i.e., valid only that running spark session.
    It can't be shared between the sessions.
    These views will be dropped when the session ends unless you created it as Hive table.
    Use saveAsTable() to materialize the contents of the DataFrame and create a pointer to the data in the metastore.
    Use saveAsTable() to materialize the contents of the DataFrame and create a pointer to the data in the metastore.
    """
import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("temporary_session_view")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())

    # Create a Temporary View
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    columns = ["firstname", "lastname", "country", "state"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

    df.createOrReplaceTempView("person")

    # Access View using Spark SQL Query
    spark.sql("select firstname, lastname from person").show()
