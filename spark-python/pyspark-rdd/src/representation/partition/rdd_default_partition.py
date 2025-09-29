import os
import sys

from pyspark.sql import SparkSession

# Set environment variables for PySpark and Java
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")


def print_partition_info(rdd):
    """
    Prints the number of partitions and the elements in each partition.
    """
    num_partitions = rdd.getNumPartitions()
    print(f"Number of Partitions: {num_partitions}")

    partition_data = rdd.glom().collect()
    for i, partition in enumerate(partition_data):
        print(f"Partition {i}: {partition}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]").appName("RDDPartition").getOrCreate()
    )

    sc = spark.sparkContext

    # Sample data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Create RDDs with different partition counts
    default_rdd = sc.parallelize(data)
    custom_rdd = sc.parallelize(data, numSlices=4)

    print("Default Partitioning:")
    print_partition_info(default_rdd)

    print("\nCustom Partitioning (4 partitions):")
    print_partition_info(custom_rdd)

    # Show repartitioning
    repartitioned_rdd = default_rdd.repartition(2)
    print("\nAfter Repartitioning to 2 partitions:")
    print_partition_info(repartitioned_rdd)

    sc.stop()
