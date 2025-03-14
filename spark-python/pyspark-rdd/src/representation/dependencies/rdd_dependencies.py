import os
import sys

from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark context
    sc = SparkContext("local[*]", "RDDDependencies")

    # Sample data
    data = [1, 2, 3, 4, 5]

    # Create an RDD
    rdd = sc.parallelize(data)

    # Transformations
    squared_rdd = rdd.map(lambda x: x ** 2)
    filtered_rdd = squared_rdd.filter(lambda x: x > 5)

    # Dependencies
    dependencies_squared = squared_rdd.dependencies()
    dependencies_filtered = filtered_rdd.dependencies()

    # Print the dependencies
    print("Dependencies of squared_rdd:", dependencies_squared)
    print("Dependencies of filtered_rdd:", dependencies_filtered)

    # Stop the Spark context
    sc.stop()
