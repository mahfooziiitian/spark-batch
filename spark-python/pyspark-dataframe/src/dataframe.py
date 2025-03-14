#!/usr/bin/env python3

from pyspark.sql import SparkSession, Row

spark = SparkSession \
    .builder \
    .appName("Data Frame Example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Student = Row("firstName", "lastName", "age", "telephone")
s1 = Student('David', 'Julian', 22, 100000)
s2 = Student('Mark', 'Webb', 23, 658545)
StudentData = [s1, s2]
df = spark.createDataFrame(StudentData)
df.show()
