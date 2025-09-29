from pyspark import SparkContext


def main():
    # Initialize SparkContext
    sc = SparkContext(appName="CreateRDDFromSparkContext")

    # Create an RDD from a Python list
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)

    # Print the elements of the RDD
    print(rdd.collect())

    # Stop the SparkContext
    sc.stop()


if __name__ == "__main__":
    main()
