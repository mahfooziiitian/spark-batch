from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestCTE:
    def test_execute_supported_cte_queries_from_source(self: TestCTE, spark: SparkSession) -> None:
        results = execute_sql_file(
            spark,
            "src/cte/cte.sql",
            skip_predicate=lambda statement: "MERGE INTO orders_target" in statement,
        )

        assert len(results) == 5
        assert results[0].count() == 2
        assert results[-1].count() == 4

    def test_recursive_cte_comes_from_source(self: TestCTE, spark: SparkSession) -> None:
        recursive_statement = statement_containing(
            "src/cte/cte.sql",
            "WITH RECURSIVE int_sequence",
        )
        result = spark.sql(recursive_statement)

        assert result.count() == 10
        first_row = result.orderBy("n").first()
        last_row = result.orderBy("n", ascending=False).first()
        assert first_row is not None and first_row.n == 1
        assert last_row is not None and last_row.n == 10
