from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, col, row_number

if __name__ == '__main__':

    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    buckets = spark.range(9).withColumn("bucket", expr("id % 3"))

    dataset = buckets.union(buckets)

    windowSpec = Window.partitionBy(col('bucket')).orderBy(col('id'))

    dataset.withColumn("row_number", row_number().over(windowSpec)).show(truncate=False)
