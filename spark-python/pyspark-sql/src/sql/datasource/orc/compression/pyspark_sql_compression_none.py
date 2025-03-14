import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("reading_json_file")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.sql.warehouse.dir", warehouse_location)
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())

    data_file = os.environ["DATA_HOME"].replace("\\", "/") + "/FileData/Orc/Compression/Zlib/employees.orc"

    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW employees_none
        using ORC
        OPTIONS(
            compression 'NONE',
            path '{data_file}' 
        )""")
    print(spark.sql("SELECT * FROM employees_none").rdd.getNumPartitions())
    spark.sql("SELECT * FROM employees_none").show(truncate=False)
