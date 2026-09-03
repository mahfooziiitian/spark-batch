from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestCondition:
    def test_comparison_query_comes_from_source(self: TestCondition, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/condition/comparison.sql")
        between_statement = statement_containing(
            "sql/condition/comparison.sql",
            "WHERE price BETWEEN 100.00 AND 400.00",
        )
        subquery_statement = statement_containing(
            "sql/condition/comparison.sql",
            "WHERE p.category IN",
        )

        results = spark.sql(between_statement)
        featured = spark.sql(subquery_statement)

        assert results.count() == 3
        assert results.filter("product_name = 'Desk'").count() == 1
        assert featured.count() == 4

    def test_logical_query_comes_from_source(self: TestCondition, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/condition/logical.sql")
        distinct_statement = statement_containing(
            "sql/condition/logical.sql",
            "WHERE NOT (department = 'HR' OR department = 'Sales')",
        )
        complex_statement = statement_containing(
            "sql/condition/logical.sql",
            "OR active = FALSE",
        )

        distinct_result = spark.sql(distinct_statement)
        complex_result = spark.sql(complex_statement)

        assert distinct_result.count() == 3
        assert complex_result.count() == 4
        assert complex_result.filter("emp_name = 'Grace'").count() == 1

    def test_pattern_query_comes_from_source(self: TestCondition, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/condition/pattern.sql")
        ilike_statement = statement_containing(
            "sql/condition/pattern.sql",
            "WHERE email ILIKE '%@example.com'",
        )
        escape_statement = statement_containing(
            "sql/condition/pattern.sql",
            "WHERE full_name LIKE '%!_%' ESCAPE '!'",
        )

        ilike_result = spark.sql(ilike_statement)
        escape_result = spark.sql(escape_statement)

        assert ilike_result.count() == 4
        assert escape_result.count() == 1
        escaped_row = escape_result.first()
        assert escaped_row is not None and escaped_row.full_name == "Grace_Lee"
