"""
An SQL CROSS JOIN is used when you need to find out all the possibilities of combining two tables, where the result set
includes every row from each contributing table.
The CROSS JOIN clause returns the Cartesian product of rows from the joined tables.
"""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("cross_join")
             .master("local[*]")
             .getOrCreate())

    spark.conf.set("spark.default.parallelism", "1")
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    df = spark.sparkContext.parallelize([['1', '1'], ['2', '2'],
                                         ['3', '3'], ['4', '4'],
                                         ['5', '5']], 1)\
        .toDF(['a', 'b'])

    df.select('a').distinct().crossJoin(df.select('b').distinct()).count()
