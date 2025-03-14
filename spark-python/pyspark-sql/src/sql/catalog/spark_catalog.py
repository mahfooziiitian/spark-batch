import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    derby_home = os.environ["derby.system.home"]
    warehouse_directory = os.environ["SPARK_WAREHOUSE"]
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("json-sql")
             .config("spark.sql.warehouse.dir", warehouse_directory)
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate()
             )

    # List all databases
    for database in spark.catalog.listDatabases():
        print(database.name)

    print(spark.catalog.currentDatabase())

    # listing all tables in a current database
    for table in spark.catalog.listTables():
        print(table.name)

    # set spark_hive database
    print(spark.catalog.setCurrentDatabase("spark_hive"))

    # listing all tables in a current database
    for table in spark.catalog.listTables():
        print(table.name)

