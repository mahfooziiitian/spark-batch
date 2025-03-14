from typing import Dict

from pyspark.sql import SparkSession


def get_spark_session(configs: Dict):
    builder = SparkSession.builder
    for key, value in configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


