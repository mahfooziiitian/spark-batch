from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality

from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestOperator:
    def test_arithmetic_query_comes_from_source(
        self: TestOperator, spark: SparkSession
    ) -> None:
        statement = statement_containing(
            "sql/operator/operator.sql", "10 DIV 3 AS int_division"
        )
        row = spark.sql(statement).first()

        assert row is not None
        assert row.addition == 13
        assert row.modulo == 1
        assert row.int_division == 3

    def test_set_operator_query_comes_from_source(
        self: TestOperator, spark: SparkSession
    ) -> None:
        execute_sql_file(spark, "sql/operator/operator.sql")
        statement = statement_containing(
            "sql/operator/operator.sql", "FROM all_products\nEXCEPT"
        )
        actual = spark.sql(statement)
        expected = spark.createDataFrame(
            [
                (1, "Laptop", "Electronics"),
                (2, "Desk", "Furniture"),
                (5, "Headphones", "Electronics"),
            ],
            "product_id int, product_name string, category string",
        )

        assert_df_equality(
            actual, expected, ignore_row_order=True, ignore_nullable=True
        )
