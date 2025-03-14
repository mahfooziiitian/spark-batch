import os

import pydeequ
from pydeequ.profiles import *
from pyspark.sql import Row, SparkSession


if __name__ == '__main__':
    print(os.environ["SPARK_VERSION"])
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("functions")
             .config("spark.jars.packages", pydeequ.deequ_maven_coord)
             .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
             .getOrCreate())

    df = spark.sparkContext.parallelize([
        Row(a="foo", b=1, c=5),
        Row(a="bar", b=2, c=6),
        Row(a="baz", b=3, c=None)]).toDF()

    result = ColumnProfilerRunner(spark) \
        .onData(df) \
        .run()

    for col, profile in result.profiles.items():
        print(profile)
