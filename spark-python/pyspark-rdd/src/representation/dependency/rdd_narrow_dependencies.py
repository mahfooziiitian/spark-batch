from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "NarrowDependenciesExample")

    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data, 2)  # 2 partitions

    mapped_rdd = rdd.map(lambda x: x * 2)  # Narrow dependency
    filtered_rdd = mapped_rdd.filter(lambda x: x > 5)  # Narrow dependency

    print("Original RDD partitions:", rdd.glom().collect())
    print("Mapped RDD partitions:", mapped_rdd.glom().collect())
    print("Filtered RDD partitions:", filtered_rdd.glom().collect())

    print("\nRDD Lineage (toDebugString):")
    print(
        filtered_rdd.toDebugString().decode("utf-8")
        if hasattr(filtered_rdd.toDebugString(), "decode")
        else filtered_rdd.toDebugString()
    )

    sc.stop()


if __name__ == "__main__":
    main()
