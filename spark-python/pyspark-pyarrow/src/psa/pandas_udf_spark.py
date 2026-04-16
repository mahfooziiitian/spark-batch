"""Pandas UDF examples covering every supported UDF type.

Demonstrates:
  1. Series → Series  (scalar / element-wise UDF)
  2. Iterator[Series] → Iterator[Series]  (iterator UDF, amortises init cost)
  3. Series… → scalar  (grouped aggregate UDF)
  4. Grouped Map via applyInPandas
"""

import os
from typing import Iterator


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

if __name__ == "__main__":
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
    spark = (
        SparkSession.builder
        .appName("pandas-udf-examples")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Sample data
    data = [
        ("Alice", "Engineering", 95000.0),
        ("Bob", "Engineering", 110000.0),
        ("Carol", "Marketing", 78000.0),
        ("Dave", "Marketing", 82000.0),
        ("Eve", "Engineering", 105000.0),
    ]
    schema = StructType([
        StructField("name", StringType()),
        StructField("dept", StringType()),
        StructField("salary", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema=schema)

    # --- 1. Series → Series: element-wise salary tax calculation ---
    @F.pandas_udf(DoubleType())
    def calc_tax(salary: pd.Series) -> pd.Series:
        return salary * 0.30

    print("=== Series → Series (scalar UDF): 30 % tax ===")
    df.withColumn("tax", calc_tax(F.col("salary"))).show()

    # --- 2. Iterator[Series] → Iterator[Series]: uppercase names ---
    @F.pandas_udf(StringType())
    def upper_name(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for batch in batch_iter:
            yield batch.str.upper()

    print("=== Iterator UDF: uppercase names ===")
    df.withColumn("name_upper", upper_name(F.col("name"))).show()

    # --- 3. Grouped Aggregate: mean salary per department ---
    @F.pandas_udf(DoubleType())
    def mean_salary(salary: pd.Series) -> float:
        return salary.mean()

    print("=== Grouped Aggregate UDF: mean salary per dept ===")
    df.groupBy("dept").agg(mean_salary(F.col("salary")).alias("avg_salary")).show()

    # --- 4. Grouped Map via applyInPandas: z-score within department ---
    result_schema = StructType([
        StructField("name", StringType()),
        StructField("dept", StringType()),
        StructField("salary", DoubleType()),
        StructField("salary_zscore", DoubleType()),
    ])

    def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
        mean = pdf["salary"].mean()
        std = pdf["salary"].std(ddof=0)
        pdf["salary_zscore"] = ((pdf["salary"] - mean) / std).round(4) if std else 0.0
        return pdf

    print("=== Grouped Map (applyInPandas): salary z-score per dept ===")
    (df.groupBy("dept")
       .applyInPandas(zscore, schema=result_schema)
       .orderBy("dept", "name")
       .show())

    spark.stop()
