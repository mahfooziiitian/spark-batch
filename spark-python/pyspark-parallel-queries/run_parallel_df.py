from pyspark.sql import SparkSession

data = ["a", "b", "c"]

from multiprocessing.pool import ThreadPool

pool = ThreadPool(10)
spark = SparkSession.builder.getOrCreate()


def fun(x):
    try:
        df = spark.createDataFrame([(1, 2, x), (2, 5, "b"), (5, 6, "c"), (8, 19, "d")], ("st", "end", "ani"))
        df.show()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    pool.map(fun, data)
