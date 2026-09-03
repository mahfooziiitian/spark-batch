from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestHaving:
    def test_having_queries_come_from_source(self: TestHaving, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/having/having.sql")
        basic_statement = statement_containing(
            "sql/having/having.sql",
            "HAVING SUM(amount) > 1000.0",
        )
        filter_statement = statement_containing(
            "sql/having/having.sql",
            "HAVING SUM(amount) FILTER (WHERE product = 'A') > 400.0",
        )

        basic = spark.sql(basic_statement)
        filtered = spark.sql(filter_statement)

        assert basic.count() == 2
        assert basic.filter("region = 'North' AND total_sales = 1590.0").count() == 1
        assert filtered.count() == 2
        assert filtered.filter("region = 'South' AND product_a_sales = 800.0").count() == 1
