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

    data_file = os.environ["DATA_HOME"].replace("\\", "/") + "/FileData/Orc/Compression/Defaults/employees.orc"

    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW employees_snappy
        using ORC
        OPTIONS(
            compression 'SNAPPY',
            path '{data_file}' 
        )""")
    spark.sql("SELECT * FROM employees_snappy").show(truncate=False)
