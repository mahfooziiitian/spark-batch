import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = (SparkConf()
            .setAppName("config-validation")
            .setMaster(os.environ.get("SPARK_MASTER", "local[*]"))
            .set("spark.executor.memory", "2g")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .set("spark.ui.enabled", "false"))

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Retrieve and verify settings
    active_conf = spark.sparkContext.getConf()
    print("spark.app.name       =", active_conf.get("spark.app.name"))
    print("spark.master         =", active_conf.get("spark.master"))
    print("spark.executor.memory=", active_conf.get("spark.executor.memory"))
    print("shuffle.partitions   =", active_conf.get("spark.sql.shuffle.partitions"))
    print("adaptive.enabled     =", active_conf.get("spark.sql.adaptive.enabled"))

    # Validate a required key
    assert active_conf.get("spark.app.name") == "config-validation"

    # Check if a key exists with a fallback
    speculation = active_conf.get("spark.speculation", "false")
    print("spark.speculation    =", speculation)

    spark.stop()
