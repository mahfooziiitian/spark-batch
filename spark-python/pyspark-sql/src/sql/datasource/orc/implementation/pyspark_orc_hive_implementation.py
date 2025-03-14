import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    spark = (SparkSession.builder
             .appName("pyspark_orc_hive_implementation")
             .config("spark.sql.catalogImplementation", "hive")
             .config("spark.sql.orc.impl", "hive")
             .config("spark.driver.extraJavaOptions",
                     f"-Dderby.system.home='{derby_home}'")
             .enableHiveSupport()
             .getOrCreate())

    data_file = os.environ["DATA_HOME"].replace("\\", "/") + "/FileData/Orc/people"

    print(f"spark.sql.orc.impl = {spark.conf.get('spark.sql.orc.impl')}")

    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW people
        USING orc
        OPTIONS (path '{data_file}')
    """)

    spark.sql("SELECT * FROM people").show()