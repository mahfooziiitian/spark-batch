"""Pandas API on Spark — missing data handling.

Demonstrates dropna, fillna, interpolation strategies, and isna/notna
filtering on pandas-on-Spark DataFrames.

Usage::

    from spp.pandas_on_spark.missing_data import (
        demo_dropna,
        demo_fillna,
        demo_isna_filter,
    )
"""

import numpy as np
import spp._env  # noqa: F401
import pyspark.pandas as ps


def create_sample_with_nulls() -> ps.DataFrame:
    """Create a sample DataFrame with missing values."""
    return ps.DataFrame(
        {
            "name": ["Alice", "Bob", None, "Dave", "Eve"],
            "age": [30.0, np.nan, 35.0, np.nan, 32.0],
            "score": [85.5, 92.0, np.nan, 88.5, np.nan],
            "city": ["NYC", "LA", "NYC", None, "LA"],
        }
    )


def demo_dropna(psdf: ps.DataFrame) -> None:
    """Show various dropna strategies."""
    print("=== dropna(how='any') — drop rows with any null ===")
    print(psdf.dropna(how="any"))

    print("\n=== dropna(how='all') — drop rows where all are null ===")
    print(psdf.dropna(how="all"))

    print("\n=== dropna(subset=['score']) — only check 'score' ===")
    print(psdf.dropna(subset=["score"]))

    print("\n=== dropna(thresh=3) — keep rows with ≥ 3 non-null ===")
    print(psdf.dropna(thresh=3))


def demo_fillna(psdf: ps.DataFrame) -> None:
    """Show various fillna strategies."""
    print("=== fillna(0) — fill all with zero ===")
    print(psdf.fillna(0))

    print("\n=== fillna per column ===")
    print(psdf.fillna({"age": psdf["age"].mean(), "score": 0.0, "city": "Unknown"}))

    print("\n=== ffill (forward fill) ===")
    print(psdf.ffill())

    print("\n=== bfill (backward fill) ===")
    print(psdf.bfill())


def demo_isna_filter(psdf: ps.DataFrame) -> None:
    """Show isna / notna filtering."""
    print("=== Rows where 'score' is null ===")
    print(psdf[psdf["score"].isna()])

    print("\n=== Rows where 'score' is NOT null ===")
    print(psdf[psdf["score"].notna()])

    print("\n=== Null count per column ===")
    print(psdf.isna().sum())


def main() -> None:
    psdf = create_sample_with_nulls()
    print("=== Original DataFrame ===")
    print(psdf)
    print()
    demo_dropna(psdf)
    print()
    demo_fillna(psdf)
    print()
    demo_isna_filter(psdf)


if __name__ == "__main__":
    from spp.session import create_spark_session

    spark = create_spark_session("ps-missing-data")
    main()
    spark.stop()
