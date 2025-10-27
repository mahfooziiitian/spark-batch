from pyspark import SparkContext
from pyspark.sql import SparkSession
from typing import List, Tuple, Optional


def get_spark_context(app_name: str = "RDD Example") -> Tuple[SparkContext, SparkSession]:
    """
    Initializes and returns a SparkContext and SparkSession.
    """
    sc = SparkContext("local", app_name)
    spark = SparkSession(sc)
    return sc, spark


def create_rdd_from_list(
    sc: SparkContext,
    data: List[Tuple[str, int, float, str]],
    num_partition: Optional[int] = None
):
    """
    Creates an RDD from a list with optional partitioning.
    """
    return sc.parallelize(data, num_partition) if num_partition else sc.parallelize(data)


def main():
    sc, _ = get_spark_context()

    data = [
        ("Chris", 1523, 0.72, "CA"),
        ("Jake", 1555, 0.83, "NY"),
        ("Cody", 1439, 0.92, "CA"),
        ("Lisa", 1442, 0.81, "FL"),
        ("Daniel", 1600, 0.88, "TX"),
        ("Kelvin", 1382, 0.99, "FL"),
        ("Nancy", 1442, 0.74, "TX"),
        ("Pavel", 1599, 0.82, "NY"),
        ("Josh", 1482, 0.78, "CA"),
        ("Cynthia", 1582, 0.94, "CA"),
    ]
    rdd = create_rdd_from_list(sc, data, num_partition=5)

    print("RDD from list:", rdd.collect())
    print("Number of partitions:", rdd.getNumPartitions())

    sc.stop()


if __name__ == "__main__":
    main()
