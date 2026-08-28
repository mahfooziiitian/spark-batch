from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality
from tests._helpers import create_view, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestAggregation:
    @staticmethod
    def _register_sales(spark: SparkSession) -> None:
        create_view(
            spark,
            "sales",
            [
                (1, "East", "Widget", 120.0, date(2024, 1, 15)),
                (2, "West", "Gadget", 340.0, date(2024, 1, 15)),
                (3, "East", "Widget", 80.0, date(2024, 2, 10)),
                (4, "North", "Gadget", 210.0, date(2024, 2, 10)),
                (5, "West", "Widget", 150.0, date(2024, 3, 5)),
                (6, "East", "Gadget", 450.0, date(2024, 3, 5)),
                (7, "North", "Widget", 90.0, date(2024, 3, 20)),
                (8, "West", "Gadget", 270.0, date(2024, 3, 20)),
            ],
            "order_id bigint, region string, product string, amount double, order_date date",
        )
        create_view(
            spark,
            "students",
            [
                (1, "Alice", "F", 55),
                (2, "Bob", "M", 75),
                (3, "Charlie", "M", 85),
                (4, "Diana", "F", 65),
                (5, "Eva", "F", 70),
                (6, "Frank", "M", 90),
            ],
            "id int, name string, gender string, weight int",
        )
        create_view(
            spark,
            "flight_summary",
            [("JFK", 10), ("JFK", 20), ("JFK", 30), ("SFO", 5), ("SFO", 15)],
            "origin_airport string, count int",
        )

    def test_cube_query_comes_from_source(self: TestAggregation, spark: SparkSession) -> None:
        self._register_sales(spark)
        cube_statement = statement_containing(
            "src/aggregation/cube/spark-cube.sql",
            "GROUP BY CUBE (region, product)",
        )
        actual = spark.sql(cube_statement)
        expected = spark.createDataFrame(
            [
                ("East", "Gadget", 450.0),
                ("East", "Widget", 200.0),
                ("East", None, 650.0),
                ("North", "Gadget", 210.0),
                ("North", "Widget", 90.0),
                ("North", None, 300.0),
                ("West", "Gadget", 610.0),
                ("West", "Widget", 150.0),
                ("West", None, 760.0),
                (None, "Gadget", 1270.0),
                (None, "Widget", 440.0),
                (None, None, 1710.0),
            ],
            "region string, product string, total_sales double",
        )
        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
        assert actual.filter("region IS NULL AND product IS NULL AND total_sales = 1710.0").count() == 1

    def test_rollup_and_pivot_queries_come_from_source(self: TestAggregation, spark: SparkSession) -> None:
        self._register_sales(spark)
        rollup_statement = statement_containing(
            "src/aggregation/rollup/rollup.sql",
            "GROUP BY ROLLUP (region, product)",
        )
        pivot_statement = statement_containing(
            "src/aggregation/pivoting/spark/spark_pivot.sql",
            "FROM students PIVOT",
        )

        rollup = spark.sql(rollup_statement)
        pivot = spark.sql(pivot_statement)

        assert rollup.count() == 10
        assert rollup.filter("region = 'East' AND product IS NULL AND total_sales = 650.0").count() == 1
        assert pivot.count() == 6
        assert pivot.filter("name = 'Alice' AND female_min = 55").count() == 1

    def test_simple_aggregation_queries_come_from_source(self: TestAggregation, spark: SparkSession) -> None:
        self._register_sales(spark)
        avg_statement = statement_containing("src/aggregation/simple/avg/avg.sql", "AVG(count) AS avg_fun")
        minmax_statement = statement_containing(
            "src/aggregation/simple/minmax/min_max_agg.sql",
            "GROUP BY origin_airport",
        )

        avg_result = spark.sql(avg_statement)
        minmax_result = spark.sql(minmax_statement)

        avg_row = avg_result.first()
        assert avg_result.count() == 1
        assert avg_row is not None and avg_row.avg_fun == avg_row.avg == 16.0
        assert minmax_result.count() == 2
        assert minmax_result.filter("min_count = 10 AND max_count = 30 AND count = 3").count() == 1
