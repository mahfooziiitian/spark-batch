"""Pandas API on Spark — DataFrame conversion.

Demonstrates converting between pandas, Spark, and pandas-on-Spark
DataFrames using the three conversion paths.
"""

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession

from spp.session import create_spark_session


def main(spark: SparkSession) -> None:
    # 1. pandas → pandas-on-Spark
    pdf = pd.DataFrame({"name": ["Alice", "Bob", "Carol"], "score": [85, 92, 78]})
    psdf = ps.from_pandas(pdf)
    print("=== pandas → pandas-on-Spark ===")
    print(psdf)

    # 2. pandas-on-Spark → pandas
    back_to_pdf = psdf.to_pandas()
    print("\n=== pandas-on-Spark → pandas ===")
    print(back_to_pdf)

    # 3. Spark → pandas-on-Spark
    sdf = spark.createDataFrame(pdf)
    psdf_from_spark = sdf.pandas_api()
    print("\n=== Spark → pandas-on-Spark ===")
    print(psdf_from_spark)

    # 4. pandas-on-Spark → Spark
    sdf_back = psdf_from_spark.to_spark()
    print("\n=== pandas-on-Spark → Spark ===")
    sdf_back.show()

    # 5. Spark → pandas (Arrow-optimized)
    pdf_from_spark = sdf.toPandas()
    print("=== Spark → pandas (Arrow) ===")
    print(pdf_from_spark)


if __name__ == "__main__":
    spark = create_spark_session("dataframe-conversion")
    main(spark)
    spark.stop()
