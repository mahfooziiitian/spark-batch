import os
from pathlib import PurePath

import findspark

if __name__ == "__main__":
    findspark.init()

    spark_home = os.environ.get("SPARK_HOME", "not set")
    print(f"SPARK_HOME          = {spark_home}")

    derby_home = os.environ.get("DERBY_HOME")
    if derby_home:
        print(f"Hive metastore URI  = {PurePath(derby_home).as_posix()}")

    warehouse = os.environ.get("SPARK_WAREHOUSE")
    if warehouse:
        print(f"Spark warehouse     = {PurePath(warehouse).as_posix()}")

    # Verify PySpark is importable after findspark.init()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName("findspark-verify")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version       = {spark.version}")
    print("findspark OK")

    spark.stop()
