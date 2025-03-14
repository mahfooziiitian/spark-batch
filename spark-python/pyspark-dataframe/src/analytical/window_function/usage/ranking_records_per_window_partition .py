from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, col, rank, percent_rank

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    buckets = spark.range(9).withColumn("bucket", expr("id % 3"))

    dataset = buckets.union(buckets)

    windowSpec = Window.partitionBy(col('bucket')).orderBy(col('id'))

    dataset.withColumn("rank", rank().over(windowSpec)).show(truncate=False)

    dataset.withColumn("rank", rank().over(windowSpec)).explain(extended=True)

    dataset.withColumn("percent_rank", percent_rank().over(windowSpec)).show(truncate=False)

    dataset.withColumn("percent_rank", percent_rank().over(windowSpec)).explain(extended=True)
