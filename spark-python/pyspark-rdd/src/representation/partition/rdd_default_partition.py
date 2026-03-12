import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")


def create_rdd(sc, data, num_partitions=None):
    """
    Creates an RDD with optional partition count.
    """
    if num_partitions:
        return sc.parallelize(data, numSlices=num_partitions)
    return sc.parallelize(data)


def print_partition_info(rdd, title="Partition Info"):
    """
    Prints the number of partitions and the elements in each partition.
    """
    print(f"\n{title}:")
    num_partitions = rdd.getNumPartitions()
    print(f"  Number of Partitions: {num_partitions}")
    partition_data = rdd.glom().collect()
    for i, partition in enumerate(partition_data):
        print(f"  Partition {i}: {partition}")


def repartition_rdd(rdd, num_partitions):
    """
    Repartitions the RDD to the specified number of partitions.
    """
    return rdd.repartition(num_partitions)


def main():
    spark = (
        SparkSession.builder.master("local[*]").appName("RDDPartition").getOrCreate()
    )
    sc = spark.sparkContext

    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    default_rdd = create_rdd(sc, data)
    custom_rdd = create_rdd(sc, data, num_partitions=4)

    print_partition_info(default_rdd, "Default Partitioning")
    print_partition_info(custom_rdd, "Custom Partitioning (4 partitions)")

    repartitioned_rdd = repartition_rdd(default_rdd, 2)
    print_partition_info(repartitioned_rdd, "After Repartitioning to 2 partitions")

    sc.stop()


if __name__ == "__main__":
    main()
