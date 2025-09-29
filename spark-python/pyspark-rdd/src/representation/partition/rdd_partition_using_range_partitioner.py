from pyspark.rdd import RDD
from pyspark.sql import SparkSession


def print_partitions(rdd: RDD, title: str):
    print(f"\n--- {title} ---")
    print(f"Number of Partitions: {rdd.getNumPartitions()}")
    for i, partition in enumerate(rdd.glom().collect()):
        print(f"Partition {i}: {partition}")


# Show partition distribution for each key
def show_key_distribution(rdd: RDD, title: str):
    key_partitions = rdd.mapPartitionsWithIndex(
        lambda idx, it: [(idx, [k for k, v in it])], preservesPartitioning=True
    ).collect()
    print(f"\nKey distribution in {title}:")
    for idx, keys in key_partitions:
        print(f"Partition {idx}: {keys}")


if __name__ == "__main__":
    # Initialize Spark session
    spark = (
        SparkSession.builder.master("local[*]").appName("RDDPartition").getOrCreate()
    )
    sc = spark.sparkContext

    # Sample data as key-value pairs
    data = [("cat", 1), ("dog", 2), ("mouse", 3), ("dog", 4), ("cat", 5), ("mouse", 6)]

    # Create a Pair RDD
    pair_rdd = sc.parallelize(data)
    print_partitions(pair_rdd, "Original RDD")
    show_key_distribution(pair_rdd, "Original RDD")

    # Partition using HashPartitioner
    # hash_partitioner = HashPartitioner(2)
    # hash_partitioned_rdd = pair_rdd.()
    # print_partitions(hash_partitioned_rdd, "HashPartitioner")

    # Partition using RangePartitioner
    # range_partitioner = RangePartitioner(2, pair_rdd)
    # range_partitioned_rdd = pair_rdd.partitionBy(2, range_partitioner)
    # print_partitions(range_partitioned_rdd, "RangePartitioner")

    # show_key_distribution(hash_partitioned_rdd, "HashPartitioner")
    # show_key_distribution(range_partitioned_rdd, "RangePartitioner")

    # Stop the Spark context
    sc.stop()
