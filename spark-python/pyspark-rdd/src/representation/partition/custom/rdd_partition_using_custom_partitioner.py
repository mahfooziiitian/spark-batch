from pyspark import SparkContext
from pyspark.rdd import Partitioner


class CustomPartitioner(Partitioner):
    """
    Custom partitioner that allows for a user-defined partitioning function.
    Falls back to hash-based partitioning if no function is provided.
    """

    def __init__(self, numPartitions, partitionFunc=None):
        if not isinstance(numPartitions, int) or numPartitions <= 0:
            raise ValueError("numPartitions must be a positive integer")
        self._numPartitions = numPartitions
        self.partitionFunc = partitionFunc

    def numPartitions(self):
        return self._numPartitions

    def getPartition(self, key):
        if self.partitionFunc:
            return self.partitionFunc(key) % self._numPartitions
        return hash(key) % self._numPartitions


def print_partitioning(rdd):
    """
    Utility function to print the elements in each partition of an RDD.
    """
    result = rdd.glom().collect()
    for i, partition in enumerate(result):
        print(f"Partition {i}: {partition}")


def animal_partition_func(key):
    if key in ("cat", "dog"):
        return 0
    else:
        return 1


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

    # Use custom partition function
    custom_partitioner = CustomPartitioner(2, partitionFunc=animal_partition_func)

    partitioned_rdd = pair_rdd.partitionBy(
        custom_partitioner.numPartitions(), custom_partitioner.partitionFunc
    )

    print(f"Number of Partitions: {partitioned_rdd.getNumPartitions()}")
    print_partitioning(partitioned_rdd)

    sc.stop()
