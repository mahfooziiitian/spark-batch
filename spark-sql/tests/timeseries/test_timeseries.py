from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestTimeseries:
    def test_tumbling_queries_execute_from_source(
        self: TestTimeseries, spark: SparkSession
    ) -> None:
        results = execute_sql_file(spark, "sql/timeseries/tumbling.sql")

        assert len(results) >= 10
        assert results[0].count() == 9
        assert results[1].count() == 6

    def test_hopping_query_comes_from_source(
        self: TestTimeseries, spark: SparkSession
    ) -> None:
        execute_sql_file(spark, "sql/timeseries/hopping.sql")
        statement = statement_containing(
            "sql/timeseries/hopping.sql",
            "FROM hopping_assignments",
        )
        result = spark.sql(statement)

        assert result.count() > 0
        assert result.filter("region = 'US'").count() > 0

    def test_session_queries_execute_from_source(
        self: TestTimeseries, spark: SparkSession
    ) -> None:
        results = execute_sql_file(spark, "sql/timeseries/session.sql")

        assert len(results) >= 6
        assert results[0].count() == 10
        assert results[2].filter("user_id = 'alice' AND session_id = 1").count() == 1

    def test_lag_lead_query_comes_from_source(
        self: TestTimeseries, spark: SparkSession
    ) -> None:
        execute_sql_file(spark, "sql/timeseries/lag_lead.sql")
        statement = statement_containing(
            "sql/timeseries/lag_lead.sql",
            "LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_day_revenue",
        )
        result = spark.sql(statement)

        assert result.count() == 14
        assert (
            result.filter(
                "region = 'US' AND sale_date = DATE '2024-01-02' AND prev_day_revenue = 120.0"
            ).count()
            == 1
        )
