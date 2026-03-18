"""
Data profiling: a single-pass report combining schema, nulls, distinct counts,
and basic stats for every column — useful for first-look data quality assessment.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark

_NUMERIC = (IntegerType, LongType, DoubleType)


class DataFrameProfiler:
    """Profile DataFrames with statistics"""

    @staticmethod
    def basic_stats(df):
        """Calculate basic statistics for numeric columns"""
        numeric_cols = [
            col
            for col, dtype in df.dtypes
            if dtype.startswith(("int", "double", "float"))
        ]

        if numeric_cols:
            return df.select(*numeric_cols).summary()
        return None

    @staticmethod
    def column_profiling(df):
        """Detailed profiling for each column"""
        profiling_results = {}

        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]

            # Basic info
            profiling_results[col_name] = {
                "type": col_type,
                "count": df.count(),
                "null_count": df.filter(F.col(col_name).isNull()).count(),
            }

            # Type-specific statistics
            if col_type.startswith(("int", "double", "float")):
                stats = df.agg(
                    F.mean(col_name).alias("mean"),
                    F.stddev(col_name).alias("stddev"),
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                ).collect()[0]

                profiling_results[col_name].update(
                    {
                        "mean": stats["mean"],
                        "stddev": stats["stddev"],
                        "min": stats["min"],
                        "max": stats["max"],
                    }
                )

            elif col_type == "string":
                profiling_results[col_name].update(
                    {
                        "distinct_count": df.select(col_name).distinct().count(),
                        "empty_string_count": df.filter(F.col(col_name) == "").count(),
                    }
                )

        return profiling_results

    @staticmethod
    def correlation_analysis(df, col1, col2):
        """Calculate correlation between two numeric columns"""
        return df.stat.corr(col1, col2)


def profile(df: DataFrame, sample_size: int = 5) -> None:
    total = df.count()
    print(f"Rows: {total}  |  Columns: {len(df.columns)}")
    print("-" * 90)
    header = f"{'Column':22s} {'Type':14s} {'Nulls':>7s} {'Null%':>6s} {'Distinct':>9s} {'Min/Top':>12s} {'Max':>12s}"
    print(header)
    print("-" * 90)

    for field in df.schema.fields:
        col = field.name
        dtype = field.dataType

        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = round(null_count / total * 100, 1) if total else 0
        n_distinct = df.select(col).distinct().count()

        if isinstance(dtype, tuple(_NUMERIC)):
            row = df.agg(F.min(col), F.max(col)).first()
            col_min = str(row[0])
            col_max = str(row[1])
        elif isinstance(dtype, StringType):
            top = df.groupBy(col).count().orderBy(F.desc("count")).first()
            col_min = top[col] if top else "—"
            col_max = "—"
        else:
            col_min = col_max = "—"

        type_name = type(dtype).__name__.replace("Type", "")
        print(
            f"  {col:20s} {type_name:14s} {null_count:7d} {null_pct:6.1f}% "
            f"{n_distinct:9d} {str(col_min):>12s} {str(col_max):>12s}"
        )

    print("-" * 90)
    print(f"\nSample ({sample_size} rows):")
    df.show(sample_size, truncate=True)


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())
    profile(df)


if __name__ == "__main__":
    spark = get_spark("explore-profile")
    main(spark)
    spark.stop()
