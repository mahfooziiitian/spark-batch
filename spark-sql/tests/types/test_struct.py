"""Tests for Spark SQL STRUCT type operations."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.mark.unit
def test_struct_field_access(spark: SparkSession) -> None:
    result = spark.sql("SELECT STRUCT(42 AS id, 'Alice' AS name).name AS person_name")
    assert result.first()["person_name"] == "Alice"


@pytest.mark.unit
def test_named_struct_creation(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT NAMED_STRUCT('city', 'NYC', 'zip', '10001') AS addr"
    )
    row = result.first()["addr"]
    assert row["city"] == "NYC"
    assert row["zip"] == "10001"


@pytest.mark.unit
def test_nested_struct_access(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT STRUCT(1 AS id, STRUCT('NYC' AS city, '10001' AS zip) AS address) AS person"
    )
    person = result.first()["person"]
    assert person["address"]["city"] == "NYC"


@pytest.mark.unit
def test_struct_in_filter(spark: SparkSession) -> None:
    inner_schema = StructType(
        [
            StructField("status", StringType(), True),
            StructField("level", IntegerType(), True),
        ]
    )
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("info", inner_schema, True),
        ]
    )
    users = spark.createDataFrame(
        [(1, ("active", 3)), (2, ("inactive", 1))],
        schema,
    )
    users.createOrReplaceTempView("struct_users")
    result = spark.sql("SELECT id FROM struct_users WHERE info.status = 'active'")

    assert result.count() == 1
    assert result.first()["id"] == 1
