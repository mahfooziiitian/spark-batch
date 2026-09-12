from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from spark_sql.runner import SqlRunner

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestSqlRunner:
    def test_run_and_explain(self: TestSqlRunner, spark: SparkSession) -> None:
        runner = SqlRunner.local(spark)
        assert runner.run("SELECT 1 AS n").scalar() == 1
        assert "Physical Plan" in runner.explain("SELECT 1 AS n")

    def test_run_file_executes_all_statements(self: TestSqlRunner, spark: SparkSession) -> None:
        runner = SqlRunner.local(spark)
        results = runner.run_file("sql/nulls/nulls.sql")
        assert len(results) > 1
        assert any(result.row_count == 5 for result in results)

    def test_run_file_queries_only_skips_ddl(self: TestSqlRunner, spark: SparkSession) -> None:
        runner = SqlRunner.local(spark)
        all_results = runner.run_file("sql/nulls/nulls.sql")
        query_results = runner.run_file("sql/nulls/nulls.sql", queries_only=True)
        assert len(query_results) < len(all_results)
        assert query_results

    def test_run_file_skip_predicate(self: TestSqlRunner, spark: SparkSession) -> None:
        runner = SqlRunner.local(spark)
        full = runner.run_file("sql/nulls/nulls.sql")
        filtered = runner.run_file(
            "sql/nulls/nulls.sql",
            skip_predicate=lambda statement: "null_safe_eq" in statement,
        )
        assert len(filtered) < len(full)
