import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

os.environ["JAVA_HOME"] = os.environ.get(
    "JAVA_HOME_17", "/usr/lib/jvm/java-8-openjdk-amd64"
)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session():
    """Create and configure Spark session with optimized settings"""
    return (
        SparkSession.builder.appName("EnhancedSparkTest")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def test_basic_operations(spark):
    """Test basic DataFrame operations"""
    print("=== Testing Basic Operations ===")
    df = spark.range(1, 101).toDF("id")

    # Add computed columns
    enhanced_df = df.withColumn("squared", col("id") * col("id")).withColumn(
        "category", when(col("id") % 2 == 0, "even").otherwise("odd")
    )

    print(f"Total records: {enhanced_df.count()}")
    print("Sample data:")
    enhanced_df.show(10)

    return enhanced_df


def test_aggregations(df):
    """Test aggregation operations"""
    print("\n=== Testing Aggregations ===")

    # Group by category and calculate statistics
    stats = df.groupBy("category").agg(
        spark_sum("id").alias("sum_id"),
        avg("squared").alias("avg_squared"),
        spark_max("id").alias("max_id"),
    )

    print("Statistics by category:")
    stats.show()

    return stats


def test_sql_operations(spark, df):
    """Test SQL functionality"""
    print("\n=== Testing SQL Operations ===")

    df.createOrReplaceTempView("numbers")

    # Complex SQL query
    sql_result = spark.sql("""
        SELECT
            category,
            COUNT(*) as count,
            AVG(id) as avg_id,
            MIN(squared) as min_squared,
            MAX(squared) as max_squared
        FROM numbers
        WHERE id BETWEEN 10 AND 90
        GROUP BY category
        ORDER BY category
    """)

    print("SQL query results:")
    sql_result.show()

    return sql_result


def test_custom_data(spark):
    """Test with custom structured data"""
    print("\n=== Testing Custom Data ===")

    # Define schema
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", IntegerType(), True),
        ]
    )

    # Sample data
    data = [
        ("Alice", 25, 95),
        ("Bob", 30, 87),
        ("Charlie", 22, 92),
        ("Diana", 28, 98),
        ("Eve", 24, 89),
    ]

    custom_df = spark.createDataFrame(data, schema)

    # Filter and transform
    high_performers = custom_df.filter(col("score") > 90).withColumn(
        "grade", when(col("score") >= 95, "A").otherwise("B")
    )

    print("High performers:")
    high_performers.show()

    return custom_df


def performance_test(spark):
    """Simple performance test"""
    print("\n=== Performance Test ===")

    start_time = time.time()
    large_df = spark.range(1, 1000000)
    result = large_df.filter(col("id") % 1000 == 0).count()
    end_time = time.time()

    print(f"Processed 1M records, found {result} matches")
    print(f"Processing time: {end_time - start_time:.2f} seconds")


def main():
    """Main execution function"""
    try:
        spark = create_spark_session()

        print("=== Spark Session Info ===")
        print(f"Spark Version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")

        # Run tests
        df = test_basic_operations(spark)
        test_aggregations(df)
        test_sql_operations(spark, df)
        test_custom_data(spark)
        performance_test(spark)

        spark.stop()
        print("\n✓ All Spark tests completed successfully!")

    except Exception as e:
        print(f"✗ Error: {e}")
        if "spark" in locals():
            spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
