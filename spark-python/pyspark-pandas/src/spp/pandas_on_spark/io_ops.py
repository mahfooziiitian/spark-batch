"""Pandas API on Spark — I/O operations.

Demonstrates read/write for CSV, Parquet, and JSON using the
``pyspark.pandas`` API, plus round-trip via Spark DataFrames.

Usage::

    from spp.pandas_on_spark.io_ops import write_and_read_parquet
"""

import os
import tempfile

import spp._env  # noqa: F401
import pyspark.pandas as ps
from pyspark.sql import SparkSession


def create_sample() -> ps.DataFrame:
    """Create a sample DataFrame for I/O demos."""
    return ps.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "score": [85.5, 92.0, 78.0, 88.5, 95.0],
            "city": ["NYC", "LA", "NYC", "LA", "NYC"],
        }
    )


def write_and_read_csv(psdf: ps.DataFrame, base_dir: str) -> ps.DataFrame:
    """Write and read CSV."""
    path = os.path.join(base_dir, "output.csv")
    psdf.to_csv(path, index=False)
    return ps.read_csv(path)


def write_and_read_parquet(psdf: ps.DataFrame, base_dir: str) -> ps.DataFrame:
    """Write and read Parquet (preferred format)."""
    path = os.path.join(base_dir, "output.parquet")
    psdf.to_parquet(path)
    return ps.read_parquet(path, index_col=None)


def write_and_read_json(psdf: ps.DataFrame, base_dir: str) -> ps.DataFrame:
    """Write and read JSON."""
    path = os.path.join(base_dir, "output.json")
    psdf.to_json(path)
    return ps.read_json(path)


def write_partitioned_parquet(
    psdf: ps.DataFrame, base_dir: str, spark: SparkSession
) -> ps.DataFrame:
    """Write partitioned Parquet via Spark, read back via ps."""
    path = os.path.join(base_dir, "partitioned")
    sdf = psdf.to_spark()
    sdf.write.mode("overwrite").partitionBy("city").parquet(path)
    return spark.read.parquet(path).pandas_api()


def main(spark: SparkSession) -> None:
    psdf = create_sample()
    output_dir = os.environ.get("OUTPUT_PATH", tempfile.mkdtemp(prefix="spp_io_"))

    print("=== CSV round-trip ===")
    print(write_and_read_csv(psdf, output_dir))

    print("\n=== Parquet round-trip ===")
    print(write_and_read_parquet(psdf, output_dir))

    print("\n=== JSON round-trip ===")
    print(write_and_read_json(psdf, output_dir))

    print("\n=== Partitioned Parquet (via Spark) ===")
    print(write_partitioned_parquet(psdf, output_dir, spark).sort_values("id"))

    # Cleanup
    import shutil

    shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    from spp.session import create_spark_session

    spark = create_spark_session("ps-io-ops")
    main(spark)
    spark.stop()
