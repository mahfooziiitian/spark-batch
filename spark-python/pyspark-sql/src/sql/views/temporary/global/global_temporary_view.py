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
    data_file = os.environ["DATA_HOME"]+"\\FileData\\Json\\people.json"
    df = spark.read.json(data_file)
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.sql("show tables in global_temp").show()