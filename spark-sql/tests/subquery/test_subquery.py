from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestSubquery:
    def test_subquery_file_executes_from_source(self: TestSubquery, spark: SparkSession) -> None:
        results = execute_sql_file(spark, "src/subquery/subquery.sql")

        assert len(results) == 9
        assert results[0].count() == 6
        assert results[5].count() == 2

    def test_scalar_and_correlated_subqueries_come_from_source(self: TestSubquery, spark: SparkSession) -> None:
        execute_sql_file(spark, "src/subquery/subquery.sql")
        scalar_statement = statement_containing(
            "src/subquery/subquery.sql",
            "WHERE o.amount > (SELECT AVG(inner_o.amount)",
        )
        correlated_statement = statement_containing(
            "src/subquery/subquery.sql",
            "SELECT AVG(o3.amount)",
        )

        scalar = spark.sql(scalar_statement)
        correlated = spark.sql(correlated_statement)
        assert scalar.count() == 2
        assert scalar.filter("order_id = 3 AND customer_id = 'C1'").count() == 1
        assert correlated.count() == 2
        assert correlated.filter("order_id = 5 AND customer_id = 'C2'").count() == 1
