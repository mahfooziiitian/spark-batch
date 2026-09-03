from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from tests._helpers import create_view, execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestJoin:
    @staticmethod
    def _register_allsales(spark: SparkSession) -> None:
        create_view(
            spark,
            "allsales",
            [
                ("Alice", "Toyota", datetime(2024, 1, 5, 0, 0), 20000.0),
                ("Alice", "Honda", datetime(2024, 2, 1, 0, 0), 22000.0),
                ("Bob", "Toyota", datetime(2024, 1, 10, 0, 0), 19000.0),
                ("Bob", "Toyota", datetime(2024, 1, 20, 0, 0), 19500.0),
                ("Carol", "Ford", datetime(2024, 3, 1, 0, 0), 25000.0),
            ],
            "customername string, makename string, saledate timestamp, saleprice double",
        )

    def test_join_sql_files_execute_from_source(self: TestJoin, spark: SparkSession) -> None:
        inner_results = execute_sql_file(spark, "sql/join/types/inner/inner_join.sql")
        outer_results = execute_sql_file(spark, "sql/join/types/outer/outer_join.sql")
        semi_results = execute_sql_file(spark, "sql/join/types/semi/semi_join.sql")
        cross_results = execute_sql_file(
            spark,
            "sql/join/types/cross/cross_join.sql",
            skip_predicate=lambda statement: "tier_price" in statement,
        )
        anti_results = execute_sql_file(spark, "sql/join/types/anti/left_anti_join.sql")

        assert inner_results[0].count() == 6
        assert outer_results[4].count() == 1
        assert semi_results[0].count() == 3
        assert anti_results[0].count() == 0
        assert anti_results[1].count() == 1
        assert anti_results[2].count() == 1
        assert cross_results[0].count() == 18

    def test_non_equi_join_query_comes_from_source(self: TestJoin, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/join/types/non_equi/non_equi_join.sql")
        statement = statement_containing(
            "sql/join/types/non_equi/non_equi_join.sql",
            "ON t.amount BETWEEN pt.min_amount AND pt.max_amount",
        )
        actual = spark.sql(statement)

        assert actual.count() == 6
        assert actual.filter("txn_id = 4 AND tier = 'Gold' AND discounted_amount = 675.0").count() == 1
        assert actual.filter("txn_id = 5 AND tier = 'Platinum'").count() == 1

    def test_self_join_query_comes_from_source(self: TestJoin, spark: SparkSession) -> None:
        self._register_allsales(spark)
        same_month = statement_containing(
            "sql/join/patterns/self_join.sql",
            "DATE_TRUNC('MONTH', a1.saledate) = DATE_TRUNC('MONTH', a2.saledate)",
        )
        customer_makes = statement_containing(
            "sql/join/patterns/self_join.sql",
            "SELECT DISTINCT",
        )
        actual_same_month = spark.sql(same_month)
        actual_makes = spark.sql(customer_makes)

        assert actual_same_month.count() == 3
        assert actual_same_month.filter("makename = 'Toyota'").count() == 3
        assert actual_makes.count() == 1
        customer_row = actual_makes.first()
        assert customer_row is not None and customer_row.customername == "Alice"
