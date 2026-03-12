from pyspark.sql import SparkSession


def main():
    # Initialize Spark session with app name
    spark = SparkSession.builder.appName(
        "AssociativeCommutativeAggregation"
    ).getOrCreate()

    data = [1, 2, 3, 4, 5]

    # Associative and commutative operations
    associative_commutative_ops = {
        "Addition": lambda a, b: a + b,
        "Multiplication": lambda a, b: a * b,
        "Max": max,
        "Min": min,
    }

    # Non-associative operations (order matters)
    non_associative_ops = {
        "Division": lambda a, b: a / b,
        "Subtraction": lambda a, b: a - b,
    }

    print("=" * 60)
    print("ASSOCIATIVE & COMMUTATIVE OPERATIONS")
    print("=" * 60)

    for op_name, op_func in associative_commutative_ops.items():
        print(f"\n{op_name.upper()}:")
        print("-" * 40)

        for i in range(1, 5):
            rdd = spark.sparkContext.parallelize(data, i)
            partitions = rdd.glom().collect()
            result = rdd.reduce(op_func)

            print(f"Partitions ({i}): {partitions}")
            print(f"Result: {result}")
            print()

    print("=" * 60)
    print("NON-ASSOCIATIVE OPERATIONS (ORDER DEPENDENT)")
    print("=" * 60)

    for op_name, op_func in non_associative_ops.items():
        print(f"\n{op_name.upper()}:")
        print("-" * 40)

        for i in range(1, 5):
            rdd = spark.sparkContext.parallelize(data, i)
            partitions = rdd.glom().collect()
            result = rdd.reduce(op_func)

            print(f"Partitions ({i}): {partitions}")
            print(f"Result: {result}")
            print()

    # Demonstrate aggregate function with custom aggregation
    print("=" * 60)
    print("CUSTOM AGGREGATION WITH AGGREGATE FUNCTION")
    print("=" * 60)

    rdd = spark.sparkContext.parallelize(data, 2)

    # Sum and count for average calculation
    sum_count = rdd.aggregate(
        (0, 0),  # Initial value: (sum, count)
        lambda acc, value: (acc[0] + value, acc[1] + 1),  # Sequence operation
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]),  # Combine operation
    )

    average = sum_count[0] / sum_count[1] if sum_count[1] > 0 else 0
    print(f"Sum: {sum_count[0]}, Count: {sum_count[1]}, Average: {average}")

    spark.stop()


if __name__ == "__main__":
    main()
