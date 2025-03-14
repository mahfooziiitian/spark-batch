import os
import sys

from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark context
    sc = SparkContext("local", "RDDIteratorsExample")

    # Sample data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Create an RDD
    rdd = sc.parallelize(data, numSlices=3)  # numSlices specifies the number of partitions

    # Use mapPartitions to demonstrate the iterator
    def process_partition(iterator):
        yield sum(iterator)


    # Apply the function to each partition
    result_rdd = rdd.mapPartitions(process_partition)

    # Collect and print the results
    result = result_rdd.collect()
    print("Results:", result)
    

    # Stop the Spark context
    sc.stop()
