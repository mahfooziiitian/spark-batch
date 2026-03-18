from typing import Dict

from pyspark.sql import SparkSession


def load_csv(spark: SparkSession, file: str):
    return (
        spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(file)
    )
