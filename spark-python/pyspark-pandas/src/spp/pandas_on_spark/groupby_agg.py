"""Pandas API on Spark — groupby and aggregation patterns.

Demonstrates groupby, agg, transform, and apply on pandas-on-Spark
DataFrames — patterns that mirror pandas but execute on Spark.

Usage::

    from spp.pandas_on_spark.groupby_agg import create_sales_data
"""

import spp._env  # noqa: F401
import pyspark.pandas as ps


def create_sales_data() -> ps.DataFrame:
    """Create a sample sales DataFrame."""
    return ps.DataFrame(
        {
            "region": ["North", "North", "South", "South", "East", "East", "East"],
            "product": [
                "Widget",
                "Gadget",
                "Widget",
                "Gizmo",
                "Widget",
                "Gadget",
                "Gizmo",
            ],
            "revenue": [100.0, 200.0, 150.0, 300.0, 120.0, 180.0, 250.0],
            "quantity": [10, 5, 8, 3, 12, 6, 4],
        }
    )


def demo_basic_groupby(psdf: ps.DataFrame) -> None:
    """Basic groupby with built-in aggregations."""
    print("=== groupby('region').sum() ===")
    print(psdf.groupby("region").sum())

    print("\n=== groupby('region').mean() ===")
    print(psdf.groupby("region").mean())

    print("\n=== groupby('region') revenue stats ===")
    stats = psdf.groupby("region").agg({"revenue": ["count", "mean", "min", "max"]})
    print(stats)


def demo_multi_agg(psdf: ps.DataFrame) -> None:
    """Multiple aggregation functions per column."""
    print("=== groupby.agg — multiple functions ===")
    result = psdf.groupby("region").agg(
        {
            "revenue": ["sum", "mean", "max"],
            "quantity": ["sum", "count"],
        }
    )
    print(result)


def demo_transform(psdf: ps.DataFrame) -> None:
    """Transform — returns same-shaped output (e.g. group-level z-score)."""
    print("=== transform — percentage of group total ===")
    ps.set_option("compute.ops_on_diff_frames", True)
    psdf = psdf.copy()
    psdf["pct_of_region"] = psdf.groupby("region")["revenue"].transform(
        lambda x: x / x.sum() * 100
    )
    print(psdf)
    ps.reset_option("compute.ops_on_diff_frames")


def demo_multi_key_groupby(psdf: ps.DataFrame) -> None:
    """GroupBy on multiple keys."""
    print("=== groupby(['region', 'product']) ===")
    print(
        psdf.groupby(["region", "product"]).agg({"revenue": "sum", "quantity": "sum"})
    )


def main() -> None:
    psdf = create_sales_data()
    print("=== Sales Data ===")
    print(psdf)
    print()
    demo_basic_groupby(psdf)
    print()
    demo_multi_agg(psdf)
    print()
    demo_transform(psdf)
    print()
    demo_multi_key_groupby(psdf)


if __name__ == "__main__":
    from spp.session import create_spark_session

    spark = create_spark_session("ps-groupby-agg")
    main()
    spark.stop()
