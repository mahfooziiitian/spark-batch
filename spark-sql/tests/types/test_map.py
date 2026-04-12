"""Tests for Spark SQL MAP type operations."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_map_creation(spark: SparkSession) -> None:
    result = spark.sql("SELECT MAP('a', 1, 'b', 2)['a'] AS val")
    assert result.first()["val"] == 1


@pytest.mark.unit
def test_map_element_at(spark: SparkSession) -> None:
    result = spark.sql("SELECT ELEMENT_AT(MAP('x', 10, 'y', 20), 'x') AS val")
    assert result.first()["val"] == 10


@pytest.mark.unit
def test_map_keys_values(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT MAP_KEYS(MAP('a', 1, 'b', 2)) AS keys,"
        "       MAP_VALUES(MAP('a', 1, 'b', 2)) AS vals"
    )
    row = result.first()
    assert sorted(row["keys"]) == ["a", "b"]
    assert sorted(row["vals"]) == [1, 2]


@pytest.mark.unit
def test_map_contains_key(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT MAP_CONTAINS_KEY(MAP('a', 1, 'b', 2), 'a') AS has_a,"
        "       MAP_CONTAINS_KEY(MAP('a', 1, 'b', 2), 'z') AS has_z"
    )
    row = result.first()
    assert row["has_a"] is True
    assert row["has_z"] is False


@pytest.mark.unit
def test_map_concat(spark: SparkSession) -> None:
    result = spark.sql("SELECT MAP_CONCAT(MAP('a', 1), MAP('b', 2)) AS merged")
    merged = result.first()["merged"]
    assert len(merged) == 2
    assert merged["a"] == 1
    assert merged["b"] == 2
