"""Pandas API on Spark — option management.

Demonstrates get_option, set_option, reset_option, and option_context
for controlling pandas-on-Spark display and compute behaviour.
"""

import spp._env  # noqa: F401
import pyspark.pandas as ps

from spp.session import create_spark_session


def main() -> None:
    print("=== Default display.max_rows ===")
    print(ps.get_option("display.max_rows"))

    print("\n=== After set_option ===")
    ps.set_option("display.max_rows", 10)
    print(ps.get_option("display.max_rows"))

    print("\n=== After reset_option ===")
    ps.reset_option("display.max_rows")
    print(ps.get_option("display.max_rows"))

    print("\n=== option_context (temporary override) ===")
    with ps.option_context("display.max_rows", 5, "compute.max_rows", 500):
        print(f"  display.max_rows = {ps.get_option('display.max_rows')}")
        print(f"  compute.max_rows = {ps.get_option('compute.max_rows')}")

    print("\n=== After context exits (back to defaults) ===")
    print(f"  display.max_rows = {ps.get_option('display.max_rows')}")
    print(f"  compute.max_rows = {ps.get_option('compute.max_rows')}")


if __name__ == "__main__":
    spark = create_spark_session("pandas-on-spark-options")
    main()
    spark.stop()
