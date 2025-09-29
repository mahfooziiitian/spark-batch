import os
import sys

from pyspark import SparkContext

# Set environment variables for PySpark and Java
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")


def print_rdd_dependencies(rdd, rdd_name):
    print(f"\nDependencies of {rdd_name}:")
    for dep in rdd.dependencies:
        print(f"  - {dep}")


if __name__ == "__main__":
    # Initialize Spark context
    sc = SparkContext("local[*]", "RDDDependencies")

    # Sample data
    data = [1, 2, 3, 4, 5]
    print("Original data:", data)

    # Create an RDD
    rdd = sc.parallelize(data)
    print_rdd_dependencies(rdd, "rdd")

    # Transformation: square each element
    squared_rdd = rdd.map(lambda x: x**2)
    print("Squared data:", squared_rdd.collect())
    print_rdd_dependencies(squared_rdd, "squared_rdd")

    # Transformation: filter elements greater than 5
    filtered_rdd = squared_rdd.filter(lambda x: x > 5)
    print("Filtered data (squared > 5):", filtered_rdd.collect())
    print_rdd_dependencies(filtered_rdd, "filtered_rdd")

    # Stop the Spark context
    sc.stop()
