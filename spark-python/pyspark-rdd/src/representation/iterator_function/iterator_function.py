import os
import sys

from pyspark import SparkContext

# Set environment variables for PySpark and Java
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")


def process_partition(iterator):
    """
    Processes each partition by calculating statistics.
    Returns a dictionary with sum, count, min, and max.
    """
    items = list(iterator)
    if items:
        yield {
            "sum": sum(items),
            "count": len(items),
            "min": min(items),
            "max": max(items),
        }
    else:
        yield {"sum": 0, "count": 0, "min": None, "max": None}


if __name__ == "__main__":
    sc = SparkContext("local", "RDDIteratorsEnhancedExample")

    # Sample data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Create an RDD with 3 partitions
    rdd = sc.parallelize(data, numSlices=3)

    # Apply the enhanced function to each partition
    result_rdd = rdd.mapPartitions(process_partition)

    # Collect and print the results
    results = result_rdd.collect()
    print("Partition statistics:")
    for idx, stats in enumerate(results):
        print(f"Partition {idx}: {stats}")

    # Stop the Spark context
    sc.stop()
