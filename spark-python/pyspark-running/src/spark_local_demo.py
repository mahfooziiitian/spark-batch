import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["JAVA_HOME"] = os.environ.get(
    "JAVA_HOME_17", "/usr/lib/jvm/java-8-openjdk-amd64"
)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_sample_data(spark):
    """Create sample data for demonstration"""

    # Sample data - employee records
    data = [
        ("Alice", "Engineering", "San Francisco", 75000, 2020),
        ("Bob", "Marketing", "New York", 65000, 2019),
        ("Charlie", "Engineering", "San Francisco", 80000, 2018),
        ("Diana", "Sales", "Chicago", 55000, 2021),
        ("Eve", "Engineering", "New York", 90000, 2017),
        ("Frank", "Marketing", "Chicago", 60000, 2020),
        ("Grace", "Sales", "San Francisco", 70000, 2019),
        ("Henry", "Engineering", "Chicago", 85000, 2018),
    ]

    schema = ["name", "department", "city", "salary", "join_year"]

    return spark.createDataFrame(data, schema)


def demonstrate_operations(df):
    """Demonstrate various Spark operations"""

    print("=== Original Data ===")
    df.show()

    print("=== Schema ===")
    df.printSchema()

    print("=== Basic Aggregations ===")
    # Department-wise statistics
    dept_stats = df.groupBy("department").agg(
        F.count("*").alias("employee_count"),
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
    dept_stats.show()

    print("=== Filtering and Sorting ===")
    high_earners = df.filter(df.salary > 70000).orderBy(F.desc("salary"))
    high_earners.show()

    print("=== City-wise Engineering Salaries ===")
    city_eng = (
        df.filter(df.department == "Engineering")
        .groupBy("city")
        .agg(F.avg("salary").alias("avg_engineering_salary"))
    )
    city_eng.show()

    return dept_stats


def spark_config_demo(spark):
    """Show Spark configuration"""
    print("=== Spark Configuration ===")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.conf.get('spark.master')}")
    print(f"App Name: {spark.conf.get('spark.app.name')}")

    # Show available cores
    sc = spark.sparkContext
    print(f"Available Cores: {sc.defaultParallelism}")


def main():
    """Main function"""
    start_time = time.time()

    # Initialize Spark Session with local configuration
    spark = (
        SparkSession.builder.appName("LocalSparkDemo")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.port", "4041")
        .getOrCreate()
    )

    try:
        # Set log level to WARN to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")

        # Show Spark configuration
        spark_config_demo(spark)

        # Create and process data
        df = create_sample_data(spark)

        # Perform operations
        result_df = demonstrate_operations(df)

        result_df.show()

        # Show execution time
        execution_time = time.time() - start_time
        print("=== Execution Summary ===")
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Data processed: {df.count()} records")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise

    finally:
        # Always stop Spark session
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":  # Entry point for the script
    main()
