import os

from jproperties import Properties
from pyspark.sql import SparkSession

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_DIR = os.path.join(SCRIPT_DIR, "..", "..", "..", "..", "cfg")

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("config-file-jproperties")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    configs = Properties()

    props_path = os.path.join(CFG_DIR, "config.properties")
    with open(props_path, "rb") as config_file:
        configs.load(config_file)

    print(f"Loaded {len(configs)} properties from {props_path}")
    print("-" * 50)
    for key, value in configs.items():
        print(f"  {key} = {value.data}")

    # Apply a property to the Spark session
    partitions = configs.get("spark.sql.shuffle.partitions")
    if partitions:
        spark.conf.set("spark.sql.shuffle.partitions", partitions.data)
        print()
        print("Applied spark.sql.shuffle.partitions =",
              spark.conf.get("spark.sql.shuffle.partitions"))

    spark.stop()
