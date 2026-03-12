from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "WideDependenciesExample")

    # Example data
    data1 = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)]
    data2 = [("a", 100), ("b", 200), ("d", 300)]

    rdd1 = sc.parallelize(data1, 2)
    rdd2 = sc.parallelize(data2, 2)

    # Wide dependency: reduceByKey (causes shuffle)
    reduced_rdd = rdd1.reduceByKey(lambda x, y: x + y)
    print("Reduced RDD (reduceByKey):", reduced_rdd.collect())

    # Wide dependency: join (causes shuffle)
    joined_rdd = reduced_rdd.join(rdd2)
    print("Joined RDD:", joined_rdd.collect())

    # Show partition contents
    print("Reduced RDD partitions:", reduced_rdd.glom().collect())
    print("Joined RDD partitions:", joined_rdd.glom().collect())

    # Show lineage (toDebugString)
    print("\nRDD Lineage (toDebugString):")
    print(
        joined_rdd.toDebugString().decode("utf-8")
        if hasattr(joined_rdd.toDebugString(), "decode")
        else joined_rdd.toDebugString()
    )

    sc.stop()


if __name__ == "__main__":
    main()
