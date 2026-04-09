import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def demo_explain_plans(spark: SparkSession) -> None:
    """Show different levels of query plan output from Catalyst."""
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("region", StringType()),
            StructField("revenue", DoubleType()),
        ]
    )
    data = [
        (1, "Alice", "North", 100.0),
        (2, "Bob", "South", 200.0),
        (3, "Charlie", "North", 150.0),
        (4, "Diana", "South", 300.0),
        (5, "Eve", "North", 250.0),
    ]
    df = spark.createDataFrame(data, schema)

    result = df.filter(F.col("region") == "North").groupBy("region").agg(F.sum("revenue").alias("total_revenue"))

    print("--- Parsed Logical Plan ---")
    result.explain(mode="simple")

    print("\n--- Full Plan (parsed → analysed → optimised → physical) ---")
    result.explain(mode="extended")

    print("\n--- Formatted Plan ---")
    result.explain(mode="formatted")


def demo_predicate_pushdown(spark: SparkSession) -> None:
    """Catalyst pushes filters as close to the data source as possible."""
    df = spark.range(0, 1000).withColumn("category", F.when(F.col("id") % 2 == 0, "even").otherwise("odd"))

    # Filter applied after withColumn — Catalyst pushes the id filter below the projection
    result = df.withColumn("doubled", F.col("id") * 2).filter(F.col("id") < 10)

    print("--- Predicate Pushdown ---")
    result.explain(mode="extended")
    result.show()


def demo_column_pruning(spark: SparkSession) -> None:
    """Catalyst removes columns not needed by the final result."""
    data = [(i, f"name_{i}", f"addr_{i}", float(i * 10)) for i in range(20)]
    df = spark.createDataFrame(data, ["id", "name", "address", "salary"])

    # Only 'name' and 'salary' are used — Catalyst prunes 'id' and 'address' early
    result = df.select("name", "salary").filter(F.col("salary") > 100.0)

    print("--- Column Pruning ---")
    result.explain(mode="extended")
    result.show()


def demo_constant_folding(spark: SparkSession) -> None:
    """Catalyst evaluates constant expressions at planning time."""
    df = spark.range(0, 10)

    # The literal expression (60 * 60 * 24) is folded into 86400 at plan time
    result = df.withColumn("seconds_per_day", F.lit(60 * 60 * 24))

    print("--- Constant Folding ---")
    result.explain(mode="extended")
    result.show(5)


def demo_aqe(spark: SparkSession) -> None:
    """Adaptive Query Execution re-optimises the plan at runtime."""
    left = spark.range(0, 1000).withColumn("key", (F.col("id") % 10).cast("int"))
    right = spark.createDataFrame([(i, f"val_{i}") for i in range(10)], ["key", "value"])

    joined = left.join(right, on="key")

    print("--- AQE Join (check for BroadcastHashJoin) ---")
    joined.explain(mode="extended")
    print(f"Result count: {joined.count()}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("catalyst-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Explain Plans ===")
    demo_explain_plans(spark)
    print("\n=== Predicate Pushdown ===")
    demo_predicate_pushdown(spark)
    print("\n=== Column Pruning ===")
    demo_column_pruning(spark)
    print("\n=== Constant Folding ===")
    demo_constant_folding(spark)
    print("\n=== Adaptive Query Execution ===")
    demo_aqe(spark)
    spark.stop()
