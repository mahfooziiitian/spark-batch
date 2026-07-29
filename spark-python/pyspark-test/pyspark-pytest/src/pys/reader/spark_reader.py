"""CSV reader utility for loading data files into Spark DataFrames."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def load_csv(spark: SparkSession, file: str) -> DataFrame:
    """Load a CSV file into a DataFrame with inferred schema.

    Args:
        spark: Active SparkSession.
        file: Path to the CSV file.

    Returns:
        DataFrame loaded from the CSV file with header and inferred types.
    """
    return (
        spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(file)
    )


def load_csv_with_schema(spark: SparkSession, file: str, schema: StructType) -> DataFrame:
    """Load a CSV file using an explicit schema (avoids schema inference overhead).

    Args:
        spark: Active SparkSession.
        file: Path to the CSV file.
        schema: StructType schema to apply.

    Returns:
        DataFrame loaded with the provided schema.
    """
    return (
        spark.read.format("csv")
        .option("sep", ",")
        .option("header", "true")
        .schema(schema)
        .load(file)
    )


def load_json(spark: SparkSession, file: str) -> DataFrame:
    """Load a JSON file into a DataFrame with inferred schema.

    Args:
        spark: Active SparkSession.
        file: Path to the JSON file (line-delimited or multi-line).

    Returns:
        DataFrame loaded from the JSON file.
    """
    return spark.read.format("json").option("multiLine", "true").load(file)


def load_parquet(spark: SparkSession, file: str) -> DataFrame:
    """Load a Parquet file into a DataFrame.

    Args:
        spark: Active SparkSession.
        file: Path to the Parquet file or directory.

    Returns:
        DataFrame loaded from Parquet.
    """
    return spark.read.parquet(file)
