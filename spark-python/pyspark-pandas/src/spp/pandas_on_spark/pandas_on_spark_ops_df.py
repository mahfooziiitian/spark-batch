"""Pandas API on Spark — operations on different frames.

Demonstrates ``compute.ops_on_diff_frames`` which allows arithmetic and
column assignment between DataFrames that originate from different sources.
"""

import spp._env  # noqa: F401
import pyspark.pandas as ps

from spp.session import create_spark_session


def main() -> None:
    ps.set_option("compute.ops_on_diff_frames", True)

    orders = ps.DataFrame(
        {"product": ["Widget", "Gadget", "Gizmo"], "quantity": [10, 5, 8]}
    )
    prices = ps.DataFrame(
        {"product": ["Widget", "Gadget", "Gizmo"], "unit_price": [2.50, 15.0, 7.25]}
    )

    orders["unit_price"] = prices["unit_price"]
    orders["total"] = orders["quantity"] * orders["unit_price"]

    print("=== Orders with computed total ===")
    print(orders)

    series_a = ps.Series([1, 2, 3, 4], name="a")
    series_b = ps.Series([10, 20, 30, 40], name="b")
    print("\n=== Series addition (different frames) ===")
    print(series_a + series_b)

    ps.reset_option("compute.ops_on_diff_frames")


if __name__ == "__main__":
    spark = create_spark_session("pandas-on-spark-ops-diff-frames")
    main()
    spark.stop()
