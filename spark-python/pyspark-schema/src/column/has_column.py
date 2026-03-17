import os

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.utils import AnalysisException


def has_column(df: DataFrame, col: str) -> bool:
    """Return True if *col* (including dot-notation nested paths) exists in *df*."""
    try:
        df[col]
        return True
    except AnalysisException:
        return False


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("has-column")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.sparkContext.parallelize(
        [Row(foo=[Row(bar=Row(foobar=3))])]
    ).toDF()
    df.printSchema()

    checks = [
        ("foobar",         False),
        ("foo",            True),
        ("foo.bar",        True),
        ("foo.bar.foobar", True),
        ("foo.bar.foobaz", False),
    ]

    print(f"\n{'Column path':<25} {'Expected':<10} {'Result':<10} {'Pass'}")
    print("-" * 55)
    for path, expected in checks:
        result = has_column(df, path)
        ok = "✓" if result == expected else "✗"
        print(f"{path:<25} {str(expected):<10} {str(result):<10} {ok}")

    spark.stop()

