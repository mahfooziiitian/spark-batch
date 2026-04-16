"""Arrow optimization for pandas ↔ Spark DataFrame conversion.

Shows the performance benefit of enabling Arrow-based columnar transfer
when calling ``df.toPandas()`` and ``spark.createDataFrame(pdf)``.
"""

import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def main(spark: SparkSession) -> None:
    pdf = pd.DataFrame(
        {
            "id": np.arange(100_000),
            "value": np.random.default_rng(42).standard_normal(100_000),
            "label": np.random.default_rng(42).choice(["A", "B", "C"], 100_000),
        }
    )

    # --- Arrow enabled (default for this session) ---
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    start = time.perf_counter()
    df = spark.createDataFrame(pdf)
    _ = df.toPandas()
    arrow_time = time.perf_counter() - start
    print(f"Arrow enabled  : {arrow_time:.3f}s  rows={df.count()}")

    # --- Arrow disabled ---
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    start = time.perf_counter()
    df2 = spark.createDataFrame(pdf)
    _ = df2.toPandas()
    no_arrow_time = time.perf_counter() - start
    print(f"Arrow disabled : {no_arrow_time:.3f}s  rows={df2.count()}")

    speedup = no_arrow_time / arrow_time if arrow_time > 0 else float("inf")
    print(f"Speedup        : {speedup:.1f}x")


if __name__ == "__main__":
    spark = create_spark_session("arrow-optimization")
    main(spark)
    spark.stop()
