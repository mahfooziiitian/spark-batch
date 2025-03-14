from pyspark.sql import SparkSession
from pyspark.rdd import RangePartitioner


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("RDDPartition").getOrCreate()

    sc = spark.sparkContext

    # Sample data as key-value pairs
    data = [("cat", 1), ("dog", 2), ("cat", 3), ("dog", 4), ("cat", 5)]

    # Create a Pair RDD
    pair_rdd = sc.parallelize(data)

    # Define a custom partitioner (HashPartitioner in this case)
    partitioner = HashPartitioner(2)

    # Apply the custom partitioner to the Pair RDD
    partitioned_rdd = pair_rdd.partitionBy(partitioner)

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