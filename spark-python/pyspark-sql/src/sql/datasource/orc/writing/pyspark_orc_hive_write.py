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

    data_file = os.environ["DATA_HOME"].replace("\\", "/") + "/FileData/Orc/people"

    print(f"spark.sql.orc.impl = {spark.conf.get('spark.sql.orc.impl')}")

    spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW people
            USING orc
            OPTIONS (path '{data_file}')
        """)

    spark.table("people").printSchema()
    spark.sql("drop database if exists orc cascade")
    spark.sql("create database if not exists orc")

    spark.sql("""
            CREATE TABLE orc.people_hive
            USING Hive
            OPTIONS (
                    orc.enableVectorizedReader 'true'
            )
            AS
            SELECT * FROM people
            """
    )

    spark.sql("SELECT * FROM orc.people_hive").show()
