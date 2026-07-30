"""Pandas ↔ PySpark JSON bridge.

Demonstrates converting between Pandas DataFrames and PySpark DataFrames
when working with JSON data. Useful for leveraging Pandas ecosystem
libraries alongside Spark's distributed processing.

Key concepts:
    - pd.read_json() → Spark DataFrame
    - Spark DataFrame → pd.DataFrame (toPandas)
    - Arrow-based conversion (faster, zero-copy)
    - Handling type mismatches between Pandas and Spark
    - When to use Pandas vs Spark for JSON

Caveats:
    - toPandas() collects ALL data to driver — only for small results
    - Arrow optimization requires pyarrow installed
    - Pandas DatetimeIndex may need explicit type mapping

Reference:
    https://spark.apache.org/docs/latest/api/python/user_guide/arrow_pandas.html
"""

import json

import pandas as pd

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.pandas_bridge")


if __name__ == "__main__":
    spark = get_spark(
        "pandas-json-bridge",
        configs={
            "spark.sql.execution.arrow.pyspark.enabled": "true",
        },
    )

    # =========================================================================
    # 1. Pandas read_json → Spark DataFrame
    # =========================================================================
    print_header("1. Pandas → Spark")

    json_file = DATA_HOME + "/df_demo/pandas_demo.json"
    write_json_lines(
        json_file,
        [
            '{"name": "Alice", "age": 30, "salary": 95000.50}',
            '{"name": "Bob", "age": 25, "salary": 85000.00}',
            '{"name": "Charlie", "age": 35, "salary": 75000.75}',
        ],
    )

    # Read with Pandas
    pdf = pd.read_json(json_file, lines=True)
    logger.info("Pandas DataFrame shape: %s", pdf.shape)
    logger.info("Pandas dtypes:\n%s", pdf.dtypes)

    # Convert to Spark (Arrow-optimized)
    df_spark = spark.createDataFrame(pdf)
    print_dataframe(df_spark, title="Pandas → Spark DataFrame")
    print_success("Arrow-based conversion is enabled for speed")

    # =========================================================================
    # 2. Spark DataFrame → Pandas
    # =========================================================================
    print_header("2. Spark → Pandas")

    pdf_back = df_spark.toPandas()
    logger.info("Converted back to Pandas: shape=%s", pdf_back.shape)
    logger.info("Pandas result:\n%s", pdf_back.to_string())
    print_success("toPandas() collects to driver — use only for small results")

    # =========================================================================
    # 3. Pandas JSON string processing → Spark
    # =========================================================================
    print_header("3. Pandas JSON String Processing → Spark")

    # Parse nested JSON with Pandas first, then load to Spark
    nested_records = [
        {"id": 1, "data": json.dumps({"x": 10, "y": 20})},
        {"id": 2, "data": json.dumps({"x": 30, "y": 40})},
    ]
    pdf_nested = pd.DataFrame(nested_records)

    # Parse JSON column in Pandas
    pdf_nested["parsed"] = pdf_nested["data"].apply(json.loads)
    pdf_nested["x"] = pdf_nested["parsed"].apply(lambda d: d["x"])
    pdf_nested["y"] = pdf_nested["parsed"].apply(lambda d: d["y"])

    df_from_pandas = spark.createDataFrame(pdf_nested[["id", "x", "y"]])
    print_dataframe(df_from_pandas, title="Pandas-Preprocessed JSON → Spark")

    # =========================================================================
    # 4. Spark aggregation → Pandas for visualization
    # =========================================================================
    print_header("4. Spark Aggregation → Pandas")

    from pyspark.sql import functions as F

    df_agg = df_spark.groupBy(
        F.when(F.col("age") < 30, "junior").otherwise("senior").alias("level"),
    ).agg(
        F.count("*").alias("count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    print_dataframe(df_agg, title="Spark Aggregation")

    pdf_agg = df_agg.toPandas()
    logger.info("Ready for matplotlib/seaborn:\n%s", pdf_agg.to_string())
    print_success("Aggregate in Spark, visualize in Pandas")

    # =========================================================================
    # 5. Pandas UDF (vectorized)
    # =========================================================================
    print_header("5. Pandas UDF (Vectorized)")

    @F.pandas_udf("double")
    def normalize_salary(salary: pd.Series) -> pd.Series:
        """Normalize salary to 0-1 range using min-max scaling."""
        return (salary - salary.min()) / (salary.max() - salary.min())

    df_normalized = df_spark.withColumn("salary_normalized", normalize_salary(F.col("salary")))
    print_dataframe(df_normalized, title="Pandas UDF: Normalized Salary")
    print_success("Pandas UDFs run vectorized — much faster than row-at-a-time UDFs")

    spark.stop()
