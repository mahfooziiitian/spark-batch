"""Pandas ↔ Spark DataFrame interop.

Demonstrates creating a Spark DataFrame from a pandas DataFrame (and back)
using Arrow-based columnar transfer, plus basic schema inspection.
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def main(spark: SparkSession) -> None:
    pdf = pd.DataFrame(
        {
            "id": range(1, 101),
            "score": np.random.default_rng(42).standard_normal(100),
            "label": np.random.default_rng(42).choice(["A", "B", "C"], 100),
        }
    )
    print("=== pandas DataFrame ===")
    print(pdf.head())

    # pandas → Spark (Arrow-optimized)
    df = spark.createDataFrame(pdf)
    print("\n=== Spark DataFrame schema ===")
    df.printSchema()
    df.show(5)

    # Spark → pandas (Arrow-optimized)
    result_pdf = df.select("*").toPandas()
    print("=== Round-trip pandas DataFrame ===")
    print(result_pdf.describe())


if __name__ == "__main__":
    spark = create_spark_session("pandas-dataframe-interop")
    main(spark)
    spark.stop()
