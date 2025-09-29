"""Enhanced PySpark RDD Partition Representation Example"""

import os
import sys

from pyspark.sql import SparkSession

# Set environment variables for PySpark and Java
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")


def print_partitions(df):
    num_partitions = df.rdd.getNumPartitions()
    print(f"\nTotal partitions: {num_partitions}")
    print(f"Partitioner: {df.rdd.partitioner}")
    print("\nDataFrame Physical Plan:")
    df.explain()
    parts = df.rdd.glom().collect()
    for i, p in enumerate(parts):
        print(f"\nPartition {i}:")
        for j, r in enumerate(p):
            print(f"  Row {j}: {r}")


def main():
    app_name = "PySpark Repartition"
    master = "local[8]"

    # Create Spark session
    spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
    print(f"Spark Version: {spark.version}")
    spark.sparkContext.setLogLevel("ERROR")

    # Sample data
    countries = ("CN", "AU", "US")
    data = [
        {"ID": i, "Country": countries[i % 3], "Amount": 10 + i} for i in range(1, 13)
    ]

    df = spark.createDataFrame(data)
    print("\nOriginal DataFrame:")
    df.show()

    print_partitions(df)

    # Repartition DataFrame
    repartitioned_df = df.repartition(4, "Country")
    print("\nRepartitioned DataFrame (by Country, 4 partitions):")

    repartitioned_df.show()
    print_partitions(repartitioned_df)

    # Coalesce DataFrame
    coalesced_df = df.coalesce(2)
    print("\nCoalesced DataFrame (2 partitions):")
    coalesced_df.show()
    print_partitions(coalesced_df)

    spark.stop()


if __name__ == "__main__":
    main()
