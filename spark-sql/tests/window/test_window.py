from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestWindow:
    def test_navigation_query_comes_from_source(
        self: TestWindow, spark: SparkSession
    ) -> None:
        execute_sql_file(spark, "sql/window/navigation/navigation.sql")
        statement = statement_containing(
            "sql/window/navigation/navigation.sql",
            "LAG(amount, 1)",
        )
        result = spark.sql(statement)

        assert result.count() == 9
        assert (
            result.filter(
                "region = 'North' AND rep = 'Alice' AND sale_date = DATE '2024-01-05' AND prev_amount = 100"
            ).count()
            == 1
        )

    def test_frame_query_comes_from_source(
        self: TestWindow, spark: SparkSession
    ) -> None:
        execute_sql_file(spark, "sql/window/frame/frame.sql")
        statement = statement_containing(
            "sql/window/frame/frame.sql",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
        )
        result = spark.sql(statement)

        assert result.count() == 9
        assert (
            result.filter(
                "region = 'North' AND rep = 'Alice' AND suffix_sum = 600"
            ).count()
            == 1
        )

    def test_ranking_queries_execute_from_source(
        self: TestWindow, spark: SparkSession
    ) -> None:
        results = execute_sql_file(spark, "sql/window/ranking/ranking.sql")
        rank_statement = statement_containing(
            "sql/window/ranking/ranking.sql",
            "WHERE rn <= 2",
        )

        assert len(results) == 6
        assert results[0].count() > 0
        top_two = spark.sql(rank_statement)
        assert top_two.count() == 4
