"""Tests for Spark SQL ARRAY type operations."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_array_size(spark: SparkSession) -> None:
    result = spark.sql("SELECT SIZE(ARRAY(1, 2, 3)) AS sz")
    assert result.first()["sz"] == 3


@pytest.mark.unit
def test_array_contains(spark: SparkSession) -> None:
    result = spark.sql(
        "SELECT ARRAY_CONTAINS(ARRAY(1, 2, 3), 2) AS has_two,       ARRAY_CONTAINS(ARRAY(1, 2, 3), 9) AS has_nine"
    )
    row = result.first()
    assert row["has_two"] is True
    assert row["has_nine"] is False


@pytest.mark.unit
def test_array_index_zero_based(spark: SparkSession) -> None:
    result = spark.sql("SELECT ARRAY(10, 20, 30)[0] AS first_elem")
    assert result.first()["first_elem"] == 10


@pytest.mark.unit
def test_element_at_one_based(spark: SparkSession) -> None:
    result = spark.sql("SELECT ELEMENT_AT(ARRAY(10, 20, 30), 1) AS first_elem")
    assert result.first()["first_elem"] == 10


@pytest.mark.unit
def test_sort_array(spark: SparkSession) -> None:
    result = spark.sql("SELECT SORT_ARRAY(ARRAY(3, 1, 2)) AS sorted")
    assert result.first()["sorted"] == [1, 2, 3]


@pytest.mark.unit
def test_array_distinct(spark: SparkSession) -> None:
    result = spark.sql("SELECT ARRAY_DISTINCT(ARRAY(1, 2, 2, 3, 3)) AS deduped")
    assert sorted(result.first()["deduped"]) == [1, 2, 3]


@pytest.mark.unit
def test_array_union(spark: SparkSession) -> None:
    result = spark.sql("SELECT ARRAY_UNION(ARRAY(1, 2, 3), ARRAY(2, 3, 4)) AS unioned")
    assert sorted(result.first()["unioned"]) == [1, 2, 3, 4]


@pytest.mark.unit
def test_array_intersect(spark: SparkSession) -> None:
    result = spark.sql("SELECT ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(2, 3, 4)) AS common")
    assert sorted(result.first()["common"]) == [2, 3]


@pytest.mark.unit
def test_array_except(spark: SparkSession) -> None:
    result = spark.sql("SELECT ARRAY_EXCEPT(ARRAY(1, 2, 3), ARRAY(2, 3)) AS diff")
    assert result.first()["diff"] == [1]


@pytest.mark.unit
def test_flatten(spark: SparkSession) -> None:
    result = spark.sql("SELECT FLATTEN(ARRAY(ARRAY(1, 2), ARRAY(3, 4))) AS flat")
    assert result.first()["flat"] == [1, 2, 3, 4]


@pytest.mark.unit
def test_collect_list_aggregation(spark: SparkSession) -> None:
    tags = spark.createDataFrame(
        [("A", "x"), ("A", "y"), ("B", "z")],
        ["grp", "tag"],
    )
    tags.createOrReplaceTempView("arr_tags")
    result = spark.sql("SELECT grp, COLLECT_LIST(tag) AS tags FROM arr_tags GROUP BY grp ORDER BY grp")
    rows = result.collect()
    assert len(rows) == 2
    assert sorted(rows[0]["tags"]) == ["x", "y"]  # group A
    assert rows[1]["tags"] == ["z"]  # group B
