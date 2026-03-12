import time

from pyspark.sql import SparkSession


def create_rdd(sc):
    """Create a simple RDD with key-value pairs"""
    data = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)]
    return sc.parallelize(data, 3)


def create_complex_rdd(sc):
    """Create a more complex RDD for demonstration"""
    data = list(range(1, 101))  # Numbers 1-100
    return sc.parallelize(data, 4).map(lambda x: (x % 10, x))


def print_rdd_properties(rdd, name="RDD"):
    """Print comprehensive RDD properties and metadata"""
    print(f"\n=== {name} Properties ===")

    # Basic properties
    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Storage level: {rdd.getStorageLevel()}")

    # Partition analysis
    print("\nPartition Analysis:")
    partition_data = rdd.glom().collect()
    for idx, partition in enumerate(partition_data):
        print(
            f"  Partition {idx}: {len(partition)} elements - {partition[:5]}{'...' if len(partition) > 5 else ''}"
        )

    # Element count
    print(f"Total elements: {rdd.count()}")

    # Sample data
    sample_data = rdd.take(5)
    print(f"Sample data (first 5): {sample_data}")


def demonstrate_transformations(rdd):
    """Demonstrate various RDD transformations"""
    print("\n=== RDD Transformations Demo ===")

    # Map transformation
    mapped_rdd = rdd.map(lambda x: (x[0], x[1] * 2))
    print(f"After map (double values): {mapped_rdd.take(3)}")

    # Filter transformation
    filtered_rdd = rdd.filter(lambda x: x[1] > 2)
    print(f"After filter (value > 2): {filtered_rdd.collect()}")

    # Reduce by key (if applicable)
    if all(len(item) == 2 for item in rdd.take(1)):
        reduced_rdd = rdd.reduceByKey(lambda a, b: a + b)
        print(f"After reduceByKey: {reduced_rdd.collect()}")


def demonstrate_actions(rdd):
    """Demonstrate various RDD actions"""
    print("\n=== RDD Actions Demo ===")

    print(f"Count: {rdd.count()}")
    print(f"First element: {rdd.first()}")
    print(f"Take 3: {rdd.take(3)}")

    # Collect (use carefully with large datasets)
    all_data = rdd.collect()
    print(f"All data: {all_data}")


def demonstrate_caching(sc):
    """Demonstrate RDD caching and persistence"""
    print("\n=== Caching Demo ===")

    # Create RDD with expensive operation
    data = sc.parallelize(range(1000000), 4)
    expensive_rdd = data.map(lambda x: x * x).filter(lambda x: x % 2 == 0)

    # Time without caching
    start_time = time.time()
    count1 = expensive_rdd.count()
    time1 = time.time() - start_time

    # Cache the RDD
    expensive_rdd.cache()

    # Time with caching (first call still slow)
    start_time = time.time()
    count2 = expensive_rdd.count()
    print(f"Count after caching (should be same as before): {count2}")
    time2 = time.time() - start_time

    # Time with caching (second call should be faster)
    start_time = time.time()
    count3 = expensive_rdd.count()
    print(f"Count after caching (should be same as before): {count3}")
    time3 = time.time() - start_time

    print(f"First execution: {time1:.4f}s")
    print(f"After caching (1st): {time2:.4f}s")
    print(f"After caching (2nd): {time3:.4f}s")
    print(f"Count: {count1}")

    # Unpersist to free memory
    expensive_rdd.unpersist()


def demonstrate_partitioning(sc):
    """Demonstrate different partitioning strategies"""
    print("\n=== Partitioning Demo ===")

    # Create data with keys
    data = [(f"key_{i % 5}", i) for i in range(20)]
    rdd = sc.parallelize(data, 4)

    print("Original partitioning:")
    print_partition_distribution(rdd)

    # Hash partitioning
    hash_partitioned = rdd.partitionBy(3)
    print("\nAfter hash partitioning (3 partitions):")
    print_partition_distribution(hash_partitioned)


def print_partition_distribution(rdd):
    """Print how data is distributed across partitions"""
    partition_data = rdd.glom().collect()
    for idx, partition in enumerate(partition_data):
        keys = [item[0] for item in partition] if partition else []
        print(f"  Partition {idx}: {len(partition)} items, keys: {set(keys)}")


def main():
    # Initialize Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("EnhancedRDDDemo")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # Reduce log verbosity

    try:
        # Basic RDD demonstration
        simple_rdd = create_rdd(sc)
        print_rdd_properties(simple_rdd, "Simple RDD")

        # Complex RDD demonstration
        complex_rdd = create_complex_rdd(sc)
        print_rdd_properties(complex_rdd, "Complex RDD")

        # Transformations
        demonstrate_transformations(simple_rdd)

        # Actions
        demonstrate_actions(simple_rdd)

        # Caching
        demonstrate_caching(sc)

        # Partitioning
        demonstrate_partitioning(sc)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
