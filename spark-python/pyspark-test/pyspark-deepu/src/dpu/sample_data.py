"""Shared sample datasets for PyDeequ examples."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

SAMPLE_SCHEMA = StructType(
    [
        StructField("a", StringType(), nullable=False),
        StructField("b", IntegerType(), nullable=False),
        StructField("c", IntegerType(), nullable=True),
    ]
)

SAMPLE_ROWS = [
    ("foo", 1, 5),
    ("bar", 2, 6),
    ("baz", 3, None),
]


def create_sample_df(spark: SparkSession) -> DataFrame:
    """Create the standard 3-row sample DataFrame used across all demos.

    Returns:
        DataFrame with columns ``a`` (string), ``b`` (int), ``c`` (nullable int).
    """
    return spark.createDataFrame(SAMPLE_ROWS, schema=SAMPLE_SCHEMA)


RETAIL_SCHEMA = StructType(
    [
        StructField("product", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("region", StringType(), nullable=False),
    ]
)

RETAIL_ROWS = [
    ("Widget A", "electronics", 29.99, 100, "North"),
    ("Widget B", "electronics", 49.99, 200, "South"),
    ("Gadget C", "accessories", 9.99, 50, "North"),
    ("Gadget D", "accessories", 14.99, None, "East"),
    ("Tool E", "hardware", 5.99, 300, "West"),
    ("Tool F", "hardware", None, 150, "North"),
    ("Widget G", "electronics", 99.99, 75, "South"),
    ("Gadget H", "accessories", 19.99, 120, "East"),
]


def create_retail_df(spark: SparkSession) -> DataFrame:
    """Create a richer retail DataFrame for more realistic demos.

    Returns:
        DataFrame with columns ``product``, ``category``, ``price``,
        ``quantity``, ``region``.  Contains intentional nulls for
        data-quality testing.
    """
    return spark.createDataFrame(RETAIL_ROWS, schema=RETAIL_SCHEMA)
