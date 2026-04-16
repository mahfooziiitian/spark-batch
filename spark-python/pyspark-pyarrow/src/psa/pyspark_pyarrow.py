"""Arrow-optimized conversions between PySpark and Pandas DataFrames.

Demonstrates:
  1. Arrow-enabled createDataFrame (Pandas → Spark)
  2. Arrow-enabled toPandas (Spark → Pandas)
  3. mapInPandas — row-wise batch processing
  4. applyInPandas — grouped map with Pandas UDFs
"""

import os


import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

if __name__ == "__main__":
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
    spark = (
        SparkSession.builder
        .appName("pyspark-pyarrow-conversions")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Arrow-enabled createDataFrame (Pandas → Spark) ---
    pdf = pd.DataFrame(np.random.rand(100, 3), columns=["a", "b", "c"])
    df = spark.createDataFrame(pdf)
    print("=== Schema from Pandas → Spark (Arrow-enabled) ===")
    df.printSchema()
    df.show(5)

    # --- 2. Arrow-enabled toPandas (Spark → Pandas) ---
    result_pdf = df.select("*").toPandas()
    print("=== Pandas DataFrame statistics ===")
    print(result_pdf.describe())

    # --- 3. mapInPandas — batch row processing ---
    # Normalise each numeric column to zero-mean within each batch
    def normalise_batch(iterator):
        for batch in iterator:
            for col_name in batch.columns:
                batch[col_name] = batch[col_name] - batch[col_name].mean()
            yield batch

    normalised = df.mapInPandas(normalise_batch, schema=df.schema)
    print("\n=== mapInPandas — normalised (first 5 rows) ===")
    normalised.show(5)

    # --- 4. applyInPandas — grouped map ---
    sales_data = [
        ("North", "2024-01", 1200.0),
        ("North", "2024-02", 1500.0),
        ("South", "2024-01", 900.0),
        ("South", "2024-02", 1100.0),
        ("South", "2024-03", 1300.0),
    ]
    sales_schema = StructType([
        StructField("region", StringType()),
        StructField("month", StringType()),
        StructField("revenue", DoubleType()),
    ])
    sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

    result_schema = StructType([
        StructField("region", StringType()),
        StructField("month", StringType()),
        StructField("revenue", DoubleType()),
        StructField("pct_of_region", DoubleType()),
    ])

    def revenue_pct(pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute each month's share of the region's total revenue."""
        total = pdf["revenue"].sum()
        pdf["pct_of_region"] = (pdf["revenue"] / total * 100).round(2)
        return pdf

    result = sales_df.groupBy("region").applyInPandas(revenue_pct, schema=result_schema)
    print("=== applyInPandas — revenue % per region ===")
    result.orderBy("region", "month").show()

    spark.stop()
