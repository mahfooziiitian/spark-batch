import os
import sys
from typing import Any

from pyspark import SparkContext

# Set environment variables for PySpark and Java only if not already set
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("JAVA_HOME", os.environ.get("JAVA_HOME_11", ""))


def print_rdd_dependencies(rdd: Any, rdd_name: str) -> None:
    """
    Prints the debug string (lineage and dependencies) of the given RDD.
    """
    print(f"\nDebug string for {rdd_name}:")
    print(
        rdd.toDebugString().decode("utf-8")
        if hasattr(rdd.toDebugString(), "decode")
        else rdd.toDebugString()
    )


def main() -> None:
    sc = SparkContext("local[*]", "RDDDependencies")

    data = [1, 2, 3, 4, 5]
    print("Original data:", data)

    rdd = sc.parallelize(data)
    print_rdd_dependencies(rdd, "rdd")

    squared_rdd = rdd.map(lambda x: x**2)
    print("Squared data:", squared_rdd.collect())
    print_rdd_dependencies(squared_rdd, "squared_rdd")

    filtered_rdd = squared_rdd.filter(lambda x: x > 5)
    print("Filtered data (squared > 5):", filtered_rdd.collect())
    print_rdd_dependencies(filtered_rdd, "filtered_rdd")

    sc.stop()


if __name__ == "__main__":
    main()
