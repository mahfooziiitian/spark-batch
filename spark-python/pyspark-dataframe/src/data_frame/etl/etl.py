"""
End-to-end ETL pipeline example — extract, transform, load as pure functions.

Each phase is a typed function so they can be unit-tested independently and
composed with DataFrame.transform().
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark


def extract(spark: SparkSession, path: str | None = None) -> DataFrame:
    if path:
        return spark.read.parquet(path)
    # Fall back to in-memory sample data when no path is provided
    return spark.createDataFrame(*customer_orders())


def transform(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("status") == "active")
        .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .dropna(subset=["customer_id"])
        .groupBy("customer_id")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_spend"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_spend"))
    )


def load(df: DataFrame, path: str) -> None:
    (df.write.mode("overwrite").partitionBy("customer_id").parquet(path))


def main(spark: SparkSession) -> None:
    input_path = os.environ.get("INPUT_PATH")
    output_path = os.environ.get("OUTPUT_PATH", "/tmp/etl_output")

    raw = extract(spark, input_path)
    processed = transform(raw)

    print(f"Processed {processed.count()} customer rows")
    processed.show(truncate=False)

    load(processed, output_path)
    print(f"Written to {output_path}")


if __name__ == "__main__":
    spark = get_spark("etl-pipeline")
    main(spark)
    spark.stop()
