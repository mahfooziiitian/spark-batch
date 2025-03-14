import os

import pydeequ
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT
from pyspark.sql import SparkSession, Row

if __name__ == '__main__':
    os.environ["SPARK_VERSION"] = "3.0.2"
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

    suggestionResult = ConstraintSuggestionRunner(spark) \
        .onData(df) \
        .addConstraintRule(DEFAULT()) \
        .run()

    # Constraint Suggestions in JSON format
    print(suggestionResult)
