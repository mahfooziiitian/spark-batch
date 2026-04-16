"""Pandas API on Spark — DataFrame creation and basic operations.

Shows how to create a pandas-on-Spark DataFrame and perform common
operations like head, describe, sort, and column selection.
"""

import pyspark.pandas as ps

from spp.session import create_spark_session


def main() -> None:
    ps.set_option("compute.ops_on_diff_frames", True)

    psdf = ps.DataFrame(
        {
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 32],
            "score": [85.5, 92.0, 78.0, 88.5, 95.0],
        }
    )

    print("=== DataFrame ===")
    print(psdf)

    print("\n=== describe ===")
    print(psdf.describe())

    print("\n=== sort by score descending ===")
    print(psdf.sort_values("score", ascending=False))

    print("\n=== column selection ===")
    print(psdf[["name", "score"]])

    df_a = ps.DataFrame({"x": [1, 2, 3]})
    df_b = ps.DataFrame({"x": [10, 20, 30]})
    print("\n=== cross-frame arithmetic ===")
    print((df_a + df_b).sort_index())

    ps.reset_option("compute.ops_on_diff_frames")


if __name__ == "__main__":
    spark = create_spark_session("pandas-on-spark-dataframe")
    main()
    spark.stop()
