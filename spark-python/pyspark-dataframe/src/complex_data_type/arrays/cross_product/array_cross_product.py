import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    data = [
        ('John', ['Size', 'Color'], ['M', 'Black']),
        ('Tom', ['Size', 'Color'], ['L', 'White']),
        ('Matteo', ['Size', 'Color'], ['M', 'Red'])
    ]
    df = spark.createDataFrame(data, ['str1', 'array_of_str1', 'array_of_str2'])

    df.withColumn("concat_result", F.array(*[F.struct(
        F.col("str1"),
        F.col("array_of_str1").getItem(i),
        F.col("array_of_str2").getItem(i))
        for i in range(2)])).show(truncate=False)

    expr = ("TRANSFORM(arrays_zip(array_of_str1, array_of_str2), x -> struct(str1, concat(x.array_of_str1), "
            "concat(x.array_of_str2)))")
    df = df.withColumn('concat_result', F.expr(expr))

    df.show(truncate=False)
