from pyspark import SparkContext
from pyspark.sql import SparkSession


def get_spark_context(app_name="RDD Example"):
    sc = SparkContext("local", app_name)
    spark = SparkSession(sc)
    return sc, spark


def create_rdd_from_list(sc, data):
    return sc.parallelize(data)


def main():
    sc, _ = get_spark_context()

    # RDD from DataFrame
    data = [1, 2, 3, 4, 5]
    rdd = create_rdd_from_list(sc, data)

    print("RDD from list:", rdd.collect())

    sc.stop()


if __name__ == "__main__":
    main()
