"""

"""
import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"


def print_partitions(df):
    num_partitions = df.rdd.getNumPartitions()
    print("Total partitions: {}".format(num_partitions))
    print("Partitioner: {}".format(df.rdd.partitioner))
    df.explain()
    parts = df.rdd.glom().collect()
    i = 0
    j = 0
    for p in parts:
        print("Partition {}:".format(i))
        for r in p:
            print("Row {}:{}".format(j, r))
            j = j + 1
        i = i + 1


def main():
    app_name = "PySpark Partition Example"
    master = "local[8]"

    # Create Spark session with Hive supported.
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
    print(spark.version)
    spark.sparkContext.setLogLevel("ERROR")

    # Populate sample data
    countries = ("CN", "AU", "US")
    data = []
    for i in range(1, 13):
        data.append({"ID": i, "Country": countries[i % 3], "Amount": 10 + i})

    df = spark.createDataFrame(data)
    df.show()
    print_partitions(df)


if __name__ == '__main__':
    main()
