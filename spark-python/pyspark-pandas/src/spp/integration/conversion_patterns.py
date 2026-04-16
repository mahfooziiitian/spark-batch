"""Integration patterns — four ways to bridge pandas and PySpark.

Pattern 1: Small data → pandas         (df.toPandas())
Pattern 2: pandas → Spark              (spark.createDataFrame(pdf))
Pattern 3: Pandas UDF inside Spark     (@pandas_udf for vectorized logic)
Pattern 4: Pandas API on Spark         (pyspark.pandas for familiar syntax at scale)
"""

import pandas as pd
import spp._env  # noqa: F401
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from spp.session import create_spark_session


def pattern_spark_to_pandas(spark: SparkSession) -> None:
    """Pattern 1: Spark → pandas (small data extraction)."""
    print("=" * 60)
    print("Pattern 1: Spark → pandas  (df.toPandas())")
    print("=" * 60)

    df = spark.createDataFrame(
        [
            ("North", "Alice", 1200.0),
            ("North", "Bob", 800.0),
            ("South", "Carol", 1500.0),
            ("South", "Dave", 950.0),
            ("East", "Eve", 1100.0),
        ],
        ["region", "name", "revenue"],
    )

    # Aggregate in Spark, then pull small result to pandas for plotting
    summary = df.groupBy("region").agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("name").alias("headcount"),
    )

    pdf = summary.toPandas()
    print(pdf)
    print(f"\nType: {type(pdf).__module__}.{type(pdf).__name__}")
    print(f"Shape: {pdf.shape}\n")


def pattern_pandas_to_spark(spark: SparkSession) -> None:
    """Pattern 2: pandas → Spark (scale up local data)."""
    print("=" * 60)
    print("Pattern 2: pandas → Spark  (spark.createDataFrame(pdf))")
    print("=" * 60)

    pdf = pd.DataFrame(
        {
            "user_id": range(1, 6),
            "signup_date": pd.date_range("2024-01-01", periods=5),
            "plan": ["free", "pro", "free", "enterprise", "pro"],
        }
    )
    print("pandas input:")
    print(pdf)

    # Arrow-optimized conversion to Spark
    df = spark.createDataFrame(pdf)
    print("\nSpark schema:")
    df.printSchema()
    df.show()


def pattern_pandas_udf(spark: SparkSession) -> None:
    """Pattern 3: Pandas UDF inside Spark (vectorized custom logic)."""
    print("=" * 60)
    print("Pattern 3: Pandas UDF  (@pandas_udf)")
    print("=" * 60)

    df = spark.createDataFrame(
        [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)],
        ["id", "value"],
    )

    @pandas_udf(DoubleType())
    def normalize(s: pd.Series) -> pd.Series:
        return (s - s.mean()) / s.std()

    result = df.withColumn("normalized", normalize("value"))
    result.show()


def pattern_pandas_api_on_spark() -> None:
    """Pattern 4: Pandas API on Spark (familiar syntax at scale)."""
    print("=" * 60)
    print("Pattern 4: Pandas API on Spark  (pyspark.pandas)")
    print("=" * 60)

    psdf = ps.DataFrame(
        {
            "category": ["A", "B", "A", "C", "B", "A"],
            "amount": [100, 200, 150, 300, 250, 175],
        }
    )

    print("GroupBy mean:")
    print(psdf.groupby("category").mean())

    print("\nFiltered:")
    print(psdf[psdf["amount"] > 150])

    # Convert to Spark DataFrame when you need Spark operations
    sdf = psdf.to_spark()
    print(f"\nSpark row count: {sdf.count()}")


def main(spark: SparkSession) -> None:
    pattern_spark_to_pandas(spark)
    pattern_pandas_to_spark(spark)
    pattern_pandas_udf(spark)
    pattern_pandas_api_on_spark()


if __name__ == "__main__":
    spark = create_spark_session("conversion-patterns")
    main(spark)
    spark.stop()
