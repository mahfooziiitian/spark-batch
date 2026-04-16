"""User-Defined Table Function (UDTF) examples.

Demonstrates:
  1. Basic UDTF — yield rows from eval()
  2. UDTF with __init__ / terminate lifecycle methods
  3. Arrow-optimized UDTF (PySpark 3.5+)
"""

import os

import psa.spark_env  # noqa: F401 — must be imported before pyspark

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, Row, StringType, StructField, StructType


# --- 1. Basic UDTF ---
class SquareNumbers:
    """Yields (num, num²) for every integer in [start, end]."""

    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield (num, num * num)


# --- 2. UDTF with lifecycle methods ---
class FibonacciNumbers:
    """Yields the first *n* Fibonacci numbers.

    Uses __init__ for setup and terminate for a summary row.
    """

    def __init__(self):
        self._count = 0

    def eval(self, n: int):
        a, b = 0, 1
        for _ in range(n):
            yield (self._count, a)
            a, b = b, a + b
            self._count += 1

    def terminate(self):
        yield (self._count, -1)  # sentinel row with total count


# --- 3. Arrow-optimized UDTF (PySpark 3.5+) ---
class ArrowRangeUDTF:
    """Arrow-optimized UDTF that expands a range into rows."""

    @staticmethod
    def eval(start: int, end: int) -> pd.DataFrame:
        nums = list(range(start, end + 1))
        return pd.DataFrame({"num": nums, "cubed": [n ** 3 for n in nums]})


if __name__ == "__main__":
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
    spark = (
        SparkSession.builder
        .appName("pyspark-udtf-examples")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Basic UDTF
    square_num = F.udtf(SquareNumbers, returnType="num: int, squared: int")
    print("=== Basic UDTF: squares ===")
    square_num(F.lit(1), F.lit(5)).show()

    # 2. Lifecycle UDTF
    fib = F.udtf(FibonacciNumbers, returnType="idx: int, value: int")
    print("=== Lifecycle UDTF: Fibonacci ===")
    fib(F.lit(8)).show()

    # 3. Arrow-optimized UDTF
    arrow_range = F.udtf(ArrowRangeUDTF, returnType="num: int, cubed: int")
    print("=== Arrow-optimized UDTF: cubes ===")
    arrow_range(F.lit(1), F.lit(5)).show()

    spark.stop()
