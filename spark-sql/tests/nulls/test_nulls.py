from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestNulls:
    def test_null_safe_equality_comes_from_source(self: TestNulls, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/nulls/nulls.sql")
        statement = statement_containing("sql/nulls/nulls.sql", "age <=> NULL AS null_safe_eq")
        result = spark.sql(statement)

        assert result.count() == 5
        assert result.filter("name = 'Bob' AND null_safe_eq = TRUE").count() == 1

    def test_grouping_and_case_queries_come_from_source(self: TestNulls, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/nulls/nulls.sql")
        grouping_statement = statement_containing(
            "sql/nulls/nulls.sql",
            "GROUP BY age",
        )
        case_statement = statement_containing(
            "sql/nulls/nulls.sql",
            "CASE",
        )
        grouped = spark.sql(grouping_statement)
        cased = spark.sql(case_statement)
        expected = spark.createDataFrame(
            [(25, 1), (30, 1), (40, 1), (None, 2)],
            "age int, person_count long",
        )

        assert_df_equality(grouped, expected, ignore_row_order=True, ignore_nullable=True)
        assert cased.filter("name = 'Dave' AND age_group = 'unknown'").count() == 1
