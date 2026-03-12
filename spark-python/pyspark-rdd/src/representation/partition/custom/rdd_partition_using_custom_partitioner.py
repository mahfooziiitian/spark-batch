from pyspark import SparkContext
from pyspark.rdd import Partitioner


class CustomPartitioner(Partitioner):
    """
    Custom partitioner that allows for a user-defined partitioning function.
    Falls back to hash-based partitioning if no function is provided.
    """

    def __init__(self, num_partitions, partition_func=None):
        if not isinstance(num_partitions, int) or num_partitions <= 0:
            raise ValueError("numPartitions must be a positive integer")
        self._numPartitions = num_partitions
        self.partitionFunc = partition_func

    def numPartitions(self):
        return self._numPartitions

    def getPartition(self, key):
        if self.partitionFunc:
            return self.partitionFunc(key) % self._numPartitions
        return hash(key) % self._numPartitions


def print_partitioning(rdd, title="Partitioning"):
    """
    Utility function to print the elements in each partition of an RDD,
    and show partition size statistics.
    """
    result = rdd.glom().collect()
    print(f"\n{title}:")
    for i, partition in enumerate(result):
        print(f"  Partition {i}: {partition} (size={len(partition)})")
    sizes = [len(p) for p in result]
    print(f"  Partition sizes: {sizes}")


def animal_partition_func(key):
    """
    Custom partition function for animal keys.
    """
    if key in ("cat", "dog"):
        return 0
    else:
        return 1


def print_key_partition_mapping(rdd, partitionFunc, numPartitions):
    """
    Prints the partition assignment for each key using the custom partition function.
    """
    keys = rdd.keys().distinct().collect()
    print("\nCustom partition mapping for keys:")
    for key in keys:
        partition = partitionFunc(key) % numPartitions
        print(f"  Key '{key}' -> Partition {partition}")


if __name__ == "__main__":
    sc = SparkContext("local[*]", "CustomPartitionerExample")

    data = [
        ("cat", 1),
        ("dog", 2),
        ("cat", 3),
        ("dog", 4),
        ("cat", 5),
        ("bird", 6),
        ("fish", 7),
    ]

    pair_rdd = sc.parallelize(data)

    # Default partitioning
    default_partitioned_rdd = pair_rdd.partitionBy(2)
    print_partitioning(default_partitioned_rdd, "Default Partitioning")

    # Custom partitioning
    custom_partitioner = CustomPartitioner(2, partition_func=animal_partition_func)
    partitioned_rdd = pair_rdd.partitionBy(
        custom_partitioner.numPartitions(), custom_partitioner.partitionFunc
    )
    print_partitioning(partitioned_rdd, "Custom Partitioning")

    # Show custom partition mapping
    print_key_partition_mapping(pair_rdd, animal_partition_func, 2)

    sc.stop()
