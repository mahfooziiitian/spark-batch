from pyspark import SparkContext
from pyspark.rdd import Partitioner


class CustomPartitioner(Partitioner):

    def __init__(self, numPartitions, partitionFunc=None):
        self.numPartitions = numPartitions
        self.partitionFunc = partitionFunc

    def numPartitions(self):
        return 2

    def getPartition(self, key):
        # Your custom partitioning logic
        return hash(key) % 2


if __name__ == '__main__':

    # Create a Spark context
    sc = SparkContext("local[*]", "CustomPartitionerExample")

    # Sample data as key-value pairs
    data = [("cat", 1), ("dog", 2), ("cat", 3), ("dog", 4), ("cat", 5)]

    # Create a Pair RDD
    pair_rdd = sc.parallelize(data)

    # Create an instance of your custom partitioner
    custom_partitioner = CustomPartitioner(2)

    # Apply the custom partitioner to the Pair RDD
    partitioned_rdd = pair_rdd.partitionBy(custom_partitioner)

    # Get the number of partitions
    num_partitions = partitioned_rdd.getNumPartitions()

    # Print the number of partitions
    print(f"Number of Partitions: {num_partitions}")

    # Print the elements in each partition
    result = partitioned_rdd.glom().collect()
    for i, partition in enumerate(result):
        print(f"Partition {i}: {partition}")

    # Stop the Spark context
    sc.stop()
