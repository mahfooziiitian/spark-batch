import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("RDDPartition").getOrCreate()

    # Create a Spark context
    sc = spark.sparkContext

    # Sample data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Create an RDD
    rdd = sc.parallelize(data)  # numSlices specifies the number of partitions

    # Get the number of partitions
    num_partitions = rdd.getNumPartitions()

    # Print the number of partitions
    print(f"Number of Partitions: {num_partitions}")

    # Collect and print the elements in each partition
    result = rdd.glom().collect()
    for i, partition in enumerate(result):
        print(f"Partition {i}: {partition}")

    # Stop the Spark context
    sc.stop()
